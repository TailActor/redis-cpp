#ifndef REDIS_CLIENT_H_
#define REDIS_CLIENT_H_

#include <cstdint>
#include <cassert>
#include <string>
#include <vector>
#include <queue>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <format>
#include <stdexcept>

#include "3rd/asio/include/asio.hpp"

namespace gamer {

class AsioTcpClient {
public:
	using ConnectCallback = std::function<void(std::error_code)>;
	using WriteCallback = std::function<void(const char*, std::size_t)>;
	using ReceiveCallback = std::function<void(const char*, std::size_t)>;

	AsioTcpClient(): AsioTcpClient("", 0) {
	}

    AsioTcpClient(const std::string& host, std::size_t port)
        : host_(host),
          port_(port) {
    }

    ~AsioTcpClient() {
        if (socket_) {
            delete socket_;
            socket_ = nullptr;
        }
    }

    void Start() {
        try {
            if (socket_) return;

            socket_ = new asio::ip::tcp::socket(io_context_);
            asio::ip::tcp::resolver resolver(io_context_);
            auto endpoints = resolver.resolve(host_, std::to_string(port_));
            this->DoConnect(endpoints);

            io_context_.run();
        } catch (std::exception& e) {
			
        }
    }

    void AsyncWrite(const char* buffer, std::size_t length) {
        asio::async_write(*socket_,
            asio::buffer(buffer, length),
            [this, buffer](std::error_code ec, std::size_t len) {
            if (!ec) {
				if (write_cb_) {
					write_cb_(buffer, len);
				}
            }
        });
    }

    void Close() {
		if (socket_ && socket_->is_open()) {
			asio::post(io_context_, [this]() { socket_->close(); });
		}
    }

    inline bool is_connected() const { return socket_ && socket_->is_open(); }

    std::string host_;
    std::size_t port_;
	ConnectCallback connect_cb_ = nullptr;
	WriteCallback write_cb_ = nullptr;
	ReceiveCallback receive_cb_ = nullptr;

private:
    void DoConnect(const asio::ip::tcp::resolver::results_type& endpoints) {
        asio::async_connect(*socket_, endpoints,
            [this](std::error_code ec, asio::ip::tcp::endpoint) {
				//std::cout << "tcp client thread id : " << std::this_thread::get_id() << std::endl;
                if (!ec) {
                    this->AsyncRead();
                }
				if (connect_cb_) {
					connect_cb_(ec);
				}
        });
    }

    void AsyncRead() {
		socket_->async_read_some(asio::buffer(read_buffer),
					std::bind(&AsioTcpClient::ReadHandler, this, std::placeholders::_1, std::placeholders::_2));
    }

	void ReadHandler(const asio::error_code& e, std::size_t bytes_transferred)
	{
		if (!e)
		{
			//std::cout.write(read_buffer, bytes_transferred);
			if (receive_cb_) {
				receive_cb_(read_buffer, bytes_transferred);
			}
			this->AsyncRead();
		}
	}

private:
    asio::io_context io_context_;
    asio::ip::tcp::socket* socket_ = nullptr;
	static constexpr int MAX_READ_BUFFER = 1024;
	char read_buffer[MAX_READ_BUFFER];
}; // class AsioTcpClient

namespace redis {

/// ======================================================================================================================

const std::string empty_string_;
constexpr const char* RESP_S1 = "*";
constexpr const char* RESP_S2 = "$";
constexpr const char* RESP_E = "\r\n";

struct read_result {
	bool success;
	std::vector<char> buffer;
};

struct write_result {
	bool success;
	std::size_t size;
};

enum class connect_state : uint8_t {
	connecting,
	connect_failed,
	connected,
	disconnected,
	reconnecting,
};

class reply;
using connect_callback_t = std::function<void(const std::string& host, std::size_t port, connect_state state)>;
using receive_callback_t = std::function<void(std::vector<char>&& buffer)>;
using async_read_callback_t = std::function<void(read_result&)>;
using async_write_callback_t = std::function<void(write_result&)>;
using reply_callback_t = std::function<void(reply&)>;

struct read_request {
	std::size_t size;
	async_read_callback_t async_read_callback;
};

struct write_request {
	std::vector<char> buffer;
	async_write_callback_t async_write_callback;
};

/// ======================================================================================================================

class i_tcp_client {
public:
	i_tcp_client() = default;

	virtual ~i_tcp_client() = default;

	virtual void connect(const std::string& host, std::uint32_t port, connect_callback_t&& connect_cb, receive_callback_t&& receive_cb, std::uint32_t timeout_milliseconds = 0) = 0;

	virtual void disconnect() = 0;

	virtual bool is_connected() const = 0;

	virtual void async_write(std::vector<char>&& buffer) = 0;
};

/// ======================================================================================================================

class tcp_client final : public i_tcp_client {
public:
	void connect(const std::string& host, std::uint32_t port, connect_callback_t&& connect_cb, receive_callback_t&& receive_cb, std::uint32_t timeout_milliseconds = 0) override {
		tcp_client_.host_ = host;
		tcp_client_.port_ = port;
		tcp_client_.connect_cb_ = [=, connect_cb = std::move(connect_cb)](std::error_code ec) {
			if (connect_cb) {
				connect_cb(host, port, ec ? connect_state::connect_failed : connect_state::connected);
			}
		};

		tcp_client_.receive_cb_ = [=, receive_cb = std::move(receive_cb)](const char* buffer, std::size_t length) {
			if (receive_cb) {
				std::vector<char> vec(buffer, buffer + length);
				receive_cb(std::move(vec));
			}
		};

		if (connect_cb) {
			connect_cb(host, port, connect_state::connecting);
		}

		tcp_client_.Start();
	}

	void disconnect() override {
		tcp_client_.Close();
	}

	bool is_connected() const override {
		return tcp_client_.is_connected();
	}

	void async_write(std::vector<char>&& buffer) override {
		tcp_client_.AsyncWrite(buffer.data(), buffer.size());
	}

private:
	gamer::AsioTcpClient tcp_client_;
};

/// ======================================================================================================================

class error : public std::runtime_error {
public:
	using std::runtime_error::runtime_error;
	using std::runtime_error::what;
	explicit error(const std::string& err) : std::runtime_error(err.c_str()) {}
	explicit error(const char* err) : std::runtime_error(err) {}
};

/// ======================================================================================================================

class reply {
public:
	enum class type {
		err,
		bulk_string,
		simple_string,
		null,
		integer,
		array
	};

	enum class string_type {
		err,
		bulk_string,
		simple_string
	};

	reply() : t_(type::null) {}

	reply(const std::string& v, string_type t) : t_(static_cast<type>(t)), str_v_(v) {}

	reply(int64_t v) : t_(type::integer), int_v_(v) {}

	reply(const std::vector<reply>& rows) : t_(type::array), rows_(rows) {}

	reply(const reply&) = default;

	reply(reply&& other) noexcept {
		t_ = other.t_;
		rows_ = std::move(other.rows_);
		str_v_ = std::move(other.str_v_);
		int_v_ = other.int_v_;
	}

	reply& operator=(const reply&) = default;

	reply& operator=(reply&& other) noexcept {
		if (this != &other) {
			t_ = other.t_;
			rows_ = std::move(other.rows_);
			str_v_ = std::move(other.str_v_);
			int_v_ = other.int_v_;
		}
		return *this;
	}

	reply& operator<<(const reply& r) {
		t_ = type::array;
		rows_.push_back(r);
		return *this;
	}

	~reply() = default;

	bool is_err() const { return t_ == type::err; }

	bool is_string() const { return is_simple_string() || is_bulk_string() || is_err(); }

	bool is_bulk_string() const { return t_ == type::bulk_string; }

	bool is_simple_string() const { return t_ == type::simple_string; }

	bool is_null() const { return t_ == type::null; }

	bool is_integer() const { return t_ == type::integer; }

	bool is_array() const { return t_ == type::array; }

	bool ok() const { return !is_err(); }

	const std::string& err() const { return is_err() ? as_string() : empty_string_; }

	const std::vector<reply>& as_array() const {
		if (!is_array()) {
			throw redis::error("Reply is not an array");
		}
		return rows_;
	}

	const std::string& as_string() const { return is_string() ? str_v_ : empty_string_; }

	int64_t as_integer() const { return is_integer() ? int_v_ : 0; }

	void set() { t_ = type::null; }

	void set(const std::string& v, string_type reply_type) {
		t_ = static_cast<type>(reply_type);
		str_v_ = v;
	}

	void set(int64_t v) {
		t_ = type::integer;
		int_v_ = v;
	}

	void set(const std::vector<reply>& rows) {
		t_ = type::array;
		rows_ = rows;
	}

	type t_ = type::null;
	std::vector<redis::reply> rows_;
	std::string str_v_;
	int64_t int_v_ = 0;
};

/// ======================================================================================================================

class i_builder {
public:
	virtual ~i_builder() = default;
	virtual i_builder& operator<<(std::string &data) = 0;
	virtual bool ready() const = 0;
	virtual reply get_reply() const = 0;
};

class int_builder : public i_builder {
public:

	int_builder() {}

	int_builder(const int_builder&) = delete;

	~int_builder() override = default;

	int_builder& operator=(const int_builder&) = delete;

	int_builder& operator<<(std::string& buffer) override {
		if (ready_) {
			return *this;
		}

		auto end_pos = buffer.find(RESP_E);
		if (end_pos == std::string::npos) {
			return *this;
		}

		for (std::size_t i = 0; i < end_pos; i++) {
			// check for negative numbers
			if (!i && negative_multiplicator_ == 1 && buffer[i] == '-') {
				negative_multiplicator_ = -1;
				continue;
			} else if (!std::isdigit(buffer[i])) {
				throw error("Invalid character for integer redis reply");
			}
			num_ *= 10;
			num_ += buffer[i] - '0';
		}

		buffer.erase(0, end_pos + 2);
		reply_.set(negative_multiplicator_ * num_);
		ready_ = true;

		return *this;
	}

	bool ready() const override { return ready_; }

	reply get_reply() const override { return reply{ reply_ }; }

	int64_t get_integer() const { return negative_multiplicator_ * num_; }

private:
	int64_t num_ = 0;
	int64_t negative_multiplicator_ = 1; // -1 for negative number, 1 otherwise
	bool ready_ = false;
	reply reply_;
};

class bulk_string_builder : public i_builder {
public:
	bulk_string_builder() {}

	bulk_string_builder(const bulk_string_builder&) = delete;

	~bulk_string_builder() override = default;

	bulk_string_builder& operator=(const bulk_string_builder&) = delete;

	i_builder& operator<<(std::string& buffer) override {
		if (ready_) {
			return *this;
		}
		//! if we don't have the size, try to get it with the current buffer
		if (!this->fetch_size(buffer) || ready_) {
			return *this;
		}

		this->fetch_str(buffer);
		return *this;
	}

	bool ready() const override { return ready_; }

	reply get_reply() const override { return reply{ reply_ }; }

	const std::string& get_bulk_string() const { return str_; }

	bool is_null() const { return is_null_; }

private:
	void build_reply() {
		if (is_null_) {
			reply_.set();
		} else {
			reply_.set(str_, reply::string_type::bulk_string);
		}
		ready_ = true;
	}

	bool fetch_size(std::string& buffer) {
		if (int_builder_.ready()) {
			return true;
		}

		int_builder_ << buffer;
		if (!int_builder_.ready()) {
			return false;
		}

		auto tmp = int_builder_.get_integer();
		if (tmp == -1) {
			is_null_ = true;
			this->build_reply();
		}
		str_size_ = tmp;

		return true;
	}
	void fetch_str(std::string& buffer) {
		if (buffer.size() < str_size_ + 2) { // also wait for end sequence
			return;
		}
		if (buffer[str_size_] != '\r' || buffer[str_size_ + 1] != '\n') {
			return;
		}

		str_ = buffer.substr(0, str_size_);
		buffer.erase(0, str_size_ + 2);
		is_null_ = false;
		this->build_reply();
	}

	int_builder int_builder_;
	std::size_t str_size_ = 0;
	std::string str_;
	bool is_null_ = true;
	bool ready_ = false;
	reply reply_;
};

class simple_string_builder : public i_builder {
public:
  simple_string_builder(void) {}

  ~simple_string_builder(void) = default;

  simple_string_builder(const simple_string_builder&) = delete;

  simple_string_builder& operator=(const simple_string_builder&) = delete;

  i_builder& operator<<(std::string& buffer) {
		if (ready_) {
			return *this;
		}
			
		auto end_pos = buffer.find(RESP_E);
		if (end_pos == std::string::npos) {
			return *this;
		}

		str_ = buffer.substr(0, end_pos);
		reply_.set(str_, reply::string_type::simple_string);
		buffer.erase(0, end_pos + 2);
		ready_ = true;

		return *this;
	}

	bool ready() const override { return ready_; }

	reply get_reply() const override { return reply{ reply_ }; }

	const std::string& get_simple_string() const { return str_; }

private:
  std::string str_;
  bool ready_ = false;
  reply reply_;
};

class error_builder : public i_builder {
public:
  error_builder(void) = default;

  ~error_builder(void) = default;

  error_builder(const error_builder&) = delete;

  error_builder& operator=(const error_builder&) = delete;

  i_builder& operator<<(std::string& buffer) {
		string_builder_ << buffer;
		if (string_builder_.ready()) {
			reply_.set(string_builder_.get_simple_string(), reply::string_type::err);
		}

		return *this;
	}

  bool ready(void) const override { return string_builder_.ready(); }

	reply get_reply() const override { return reply{ reply_ }; }

  const std::string& get_error(void) const { return string_builder_.get_simple_string(); }

private:
  simple_string_builder string_builder_;
  reply reply_;
};

std::unique_ptr<i_builder> create_builder(char id);

class array_builder : public i_builder {
public:
  array_builder(void) {}

  ~array_builder(void) = default;

  array_builder(const array_builder&) = delete;

  array_builder& operator=(const array_builder&) = delete;

  i_builder& operator<<(std::string& buffer) {
		if (ready_) {
			return *this;
		}

		if (!this->fetch_array_size(buffer)) {
			return *this;
		}

		while (buffer.size() && !ready_) {
			if (!this->build_row(buffer)) {
				return *this;
			}
		}

		return *this;
	}

  bool ready(void) const override { return ready_; }

	reply get_reply() const override { return reply{ reply_ }; }

private:
  bool fetch_array_size(std::string& buffer) {
		if (int_builder_.ready()) {
			return true;
		}
			
		int_builder_ << buffer;
		if (!int_builder_.ready()) {
			return false;
		}

		int64_t size = int_builder_.get_integer();
		if (size < 0) {
			reply_.set();	
		}

		ready_ = true;
		array_size_ = size;

		return true;
	}

  bool build_row(std::string& buffer) {
		if (!current_builder_) {
			current_builder_ = redis::create_builder(buffer.front());
			buffer.erase(0, 1);
		}

		*current_builder_ << buffer;
		if (!current_builder_->ready()) {
			return false;
		}

		reply_ << current_builder_->get_reply();
		current_builder_ = nullptr;

		if (reply_.as_array().size() == array_size_) {
			ready_ = true;
		}

		return true;
	}

  int_builder int_builder_;
  uint64_t array_size_ = 0;
  std::unique_ptr<i_builder> current_builder_;
  bool ready_ = false;
  reply reply_;
};

std::unique_ptr<i_builder> create_builder(char id) {
	switch (id) {
		case '+': {
			return std::make_unique<simple_string_builder>();
		}
		case '-': {
			return std::make_unique<error_builder>();
			break;
		}
		case ':': {
			return std::make_unique<int_builder>();
		}
		case '$': {
			return std::make_unique<bulk_string_builder>();
		}
		case '*': {
			return std::make_unique<array_builder>();
		}
		default: {
		}
	}
	return nullptr;
}

class reply_builder {
public:
	reply_builder() {}

	~reply_builder() = default;

	reply_builder(const reply_builder&) = delete;

	reply_builder& operator=(const reply_builder&) = delete;

	reply_builder& operator<<(const std::string& data) {
		buffer_ += data;
		while (build_reply());
		return *this;
	}

	void operator>>(reply& reply) { reply = front(); }

	const reply& front() const {
		if (!available()) {
			throw error("No available reply");
		}
		return available_replies_.front();
	}

	void pop() {
		if (!available()) {
			throw error("No available reply");
		}
		available_replies_.pop_front();
	}

	bool available() const { return !available_replies_.empty(); }

	void reset() {
		builder_ = nullptr;
		buffer_.clear();
		//available_replies_.clear(); // TODO check if this is correct
	}

private:
	bool build_reply() {
		if (buffer_.empty()) {
			return false;
		}

		if (!builder_) {
			builder_ = redis::create_builder(buffer_.front());
			buffer_.erase(0, 1);
		}

		*builder_ << buffer_;

		if (builder_->ready()) {
			available_replies_.push_back(builder_->get_reply());
			builder_ = nullptr;

			return true;
		}

		return false;
	}

	std::string buffer_;
	std::unique_ptr<i_builder> builder_ = nullptr;
	std::deque<reply> available_replies_;
};

/// ======================================================================================================================

class client final {
public:
	client(i_tcp_client* tcp_client) : tcp_client_(tcp_client) {
		assert(tcp_client_ != nullptr);
	}

	client() {
		default_tcp_client_ = new tcp_client();
		tcp_client_ = default_tcp_client_;
	}

	~client() {
		if (default_tcp_client_) {
			delete default_tcp_client_;
			default_tcp_client_ = nullptr;
		}
	}

	void connect(const std::string& host, std::uint32_t port, connect_callback_t&& cb, std::uint32_t timeout_milliseconds = 0) {
		if (connect_state_ != connect_state::disconnected) {
			return;
		}
		host_ = host;
		port_ = port;
		connect_callback_ = cb;
		timeout_milliseconds_ = timeout_milliseconds;

		auto connect_cb = std::bind(&client::on_connect, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
		auto receive_cb = std::bind(&client::on_receive, this, std::placeholders::_1);
		tcp_client_->connect(host, port, std::move(connect_cb), std::move(receive_cb), timeout_milliseconds);
	}

	void connect(connect_callback_t&& connect_cb) {
		this->connect("127.0.0.1", 6379, std::move(connect_cb));
	}

	void disconnect() {
		tcp_client_->disconnect();
	}

	bool is_connected() const {
		return tcp_client_->is_connected();
	}

	client& set(const std::string& k, const std::string& v, const reply_callback_t& cb) {
		this->push({ "SET", k, v }, cb);
		return *this;
	}

	client& get(const std::string& k, const reply_callback_t& cb) {
		this->push({ "GET", k }, cb);
		return *this;
	}

	client& asyn_commit() {
		if (!is_reconnecting()) {
			try_commit();
		}
		return *this;
	}

	client& sync_commit() {
		if (!is_reconnecting()) {
			try_commit();
		}
		std::unique_lock<std::mutex> lock(callbacks_mtx_);
		cv_.wait(lock, [this] { return callbacks_running_ == 0 && commands_.empty(); });
		return *this;
	}

	std::string host_;
	std::uint32_t port_ = 0;
	connect_callback_t connect_callback_ = nullptr;
	std::uint32_t timeout_milliseconds_ = 0;
	connect_state connect_state_ = connect_state::disconnected;

protected:
	void on_connect(const std::string& host, std::size_t port, connect_state state) {
		connect_state_ = state;
		if (connect_callback_) {
			connect_callback_(host, port, state);
		}
	}

	void on_receive(std::vector<char>&& buffer) {
		try {
			builder_ << std::string(buffer.begin(), buffer.end());
		} catch (const error&) {
			//call_disconnection_handler(); // TODO check if this is correct
			return;
		}

		while (builder_.available()) {
			auto reply = builder_.front();
			builder_.pop();

			reply_callback_t cb = nullptr;
			{
				std::unique_lock<std::mutex> lock(callbacks_mtx_);
				callbacks_running_ += 1;
				if (!commands_.empty()) {
					cb = commands_.front().cb;
					commands_.pop();
				}

				lock.unlock();
				if (cb) {
					cb(reply);
				}

				lock.lock();
				callbacks_running_ -= 1;
				cv_.notify_all();
			}
		}
	}

	client& push(const std::vector<std::string>& redis_cmd, const reply_callback_t& cb) {
		std::lock_guard<std::mutex> lock(callbacks_mtx_);
		this->unprotected_push(redis_cmd, cb);
		return *this;
	}

	void unprotected_push(const std::vector<std::string>& redis_cmd, const reply_callback_t& cb) {
		buffer_ += this->build_command(redis_cmd);
		commands_.push({ redis_cmd, cb });
	}

	std::string build_command(const std::vector<std::string>& redis_cmd) {
		//std::string cmd = "*" + std::to_string(redis_cmd.size()) + "\r\n";
		//for (const auto &cmd_part : redis_cmd)
		//	cmd += "$" + std::to_string(cmd_part.length()) + "\r\n" + cmd_part + "\r\n";
		auto cmd  = std::format("{}{}{}", RESP_S1, redis_cmd.size(), RESP_E);
		for (const auto& cmd_part : redis_cmd) {
			cmd += std::format("{}{}{}{}{}", RESP_S2, cmd_part.length(), RESP_E, cmd_part, RESP_E);
		}
		return cmd;
	}

	void try_commit() {
		try {
			auto buf = std::move(buffer_);
			tcp_client_->async_write({ buf.begin(), buf.end() });
		} catch (const redis::error&) {
			//clear_callbacks(); // TODO
			throw;
		}
	}

	bool is_reconnecting() const { return connect_state_ == connect_state::reconnecting; }

private:
	struct command_t {
		std::vector<std::string> cmd;
		reply_callback_t cb;
	};

	client(const client&);
	const client& operator=(const client&);

	i_tcp_client* tcp_client_ = nullptr;
	i_tcp_client* default_tcp_client_ = nullptr;
	std::mutex callbacks_mtx_;
	std::queue<command_t> commands_;
	std::string buffer_;
	reply_builder builder_;
	std::atomic<unsigned int> callbacks_running_ = 0;
	std::condition_variable cv_;
};

/// ======================================================================================================================

} // namespace redis

} // namespace gamer

#endif // REDIS_CLIENT_H_
