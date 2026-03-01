#include "hms/hms_connector.hpp"
#include "hms/hms_mapper.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <netdb.h>
#include <optional>
#include <sstream>
#include <functional>
#include <filesystem>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace duckdb {

namespace {

enum class ThriftType : uint8_t {
	Stop = 0,
	Void = 1,
	Bool = 2,
	Byte = 3,
	Double = 4,
	I16 = 6,
	I32 = 8,
	I64 = 10,
	String = 11,
	Struct = 12,
	Map = 13,
	Set = 14,
	List = 15
};

enum class ThriftMessageType : uint8_t { Call = 1, Reply = 2, Exception = 3 };

static constexpr int32_t THRIFT_VERSION_1 = static_cast<int32_t>(0x80010000);

class SocketHandle {
public:
	explicit SocketHandle(int fd_p) : fd(fd_p) {
	}
	~SocketHandle() {
		if (fd >= 0) {
			close(fd);
		}
	}
	SocketHandle(const SocketHandle &) = delete;
	SocketHandle &operator=(const SocketHandle &) = delete;
	int fd;
};

class ThriftWriter {
public:
	void WriteByte(uint8_t v) {
		buffer.push_back(v);
	}

	void WriteI16(int16_t v) {
		buffer.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
		buffer.push_back(static_cast<uint8_t>(v & 0xFF));
	}

	void WriteI32(int32_t v) {
		buffer.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
		buffer.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
		buffer.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
		buffer.push_back(static_cast<uint8_t>(v & 0xFF));
	}

	void WriteString(const std::string &s) {
		WriteI32(static_cast<int32_t>(s.size()));
		buffer.insert(buffer.end(), s.begin(), s.end());
	}

	void WriteMessageBegin(const std::string &name, ThriftMessageType message_type, int32_t seqid) {
		WriteI32(THRIFT_VERSION_1 | static_cast<int32_t>(message_type));
		WriteString(name);
		WriteI32(seqid);
	}

	void WriteFieldBegin(ThriftType type, int16_t field_id) {
		WriteByte(static_cast<uint8_t>(type));
		WriteI16(field_id);
	}

	void WriteFieldStop() {
		WriteByte(static_cast<uint8_t>(ThriftType::Stop));
	}

	void WriteStructBegin() {
	}

	void WriteStructEnd() {
	}

	const std::vector<uint8_t> &Data() const {
		return buffer;
	}

private:
	std::vector<uint8_t> buffer;
};

class ThriftReader {
public:
	explicit ThriftReader(int fd_p) : fd(fd_p) {
	}

	bool ReadExact(uint8_t *dst, size_t n) {
		size_t off = 0;
		while (off < n) {
			ssize_t read_count = recv(fd, dst + off, n - off, 0);
			if (read_count <= 0) {
				return false;
			}
			off += static_cast<size_t>(read_count);
		}
		return true;
	}

	bool ReadByte(uint8_t &out) {
		return ReadExact(&out, 1);
	}

	bool ReadI16(int16_t &out) {
		uint8_t b[2];
		if (!ReadExact(b, sizeof(b))) {
			return false;
		}
		out = static_cast<int16_t>((static_cast<int16_t>(b[0]) << 8) | static_cast<int16_t>(b[1]));
		return true;
	}

	bool ReadI32(int32_t &out) {
		uint8_t b[4];
		if (!ReadExact(b, sizeof(b))) {
			return false;
		}
		out = (static_cast<int32_t>(b[0]) << 24) | (static_cast<int32_t>(b[1]) << 16) |
		      (static_cast<int32_t>(b[2]) << 8) | static_cast<int32_t>(b[3]);
		return true;
	}

	bool ReadI64(int64_t &out) {
		uint8_t b[8];
		if (!ReadExact(b, sizeof(b))) {
			return false;
		}
		out = (static_cast<int64_t>(b[0]) << 56) | (static_cast<int64_t>(b[1]) << 48) |
		      (static_cast<int64_t>(b[2]) << 40) | (static_cast<int64_t>(b[3]) << 32) |
		      (static_cast<int64_t>(b[4]) << 24) | (static_cast<int64_t>(b[5]) << 16) |
		      (static_cast<int64_t>(b[6]) << 8) | static_cast<int64_t>(b[7]);
		return true;
	}

	bool ReadString(std::string &out) {
		int32_t len;
		if (!ReadI32(len) || len < 0) {
			return false;
		}
		out.resize(static_cast<size_t>(len));
		if (len == 0) {
			return true;
		}
		return ReadExact(reinterpret_cast<uint8_t *>(&out[0]), static_cast<size_t>(len));
	}

	bool Skip(ThriftType type) {
		switch (type) {
		case ThriftType::Stop:
		case ThriftType::Void:
			return true;
		case ThriftType::Bool:
		case ThriftType::Byte: {
			uint8_t x;
			return ReadByte(x);
		}
		case ThriftType::I16: {
			int16_t x;
			return ReadI16(x);
		}
		case ThriftType::I32: {
			int32_t x;
			return ReadI32(x);
		}
		case ThriftType::I64: {
			int64_t x;
			return ReadI64(x);
		}
		case ThriftType::Double: {
			uint8_t d[8];
			return ReadExact(d, sizeof(d));
		}
		case ThriftType::String: {
			std::string s;
			return ReadString(s);
		}
		case ThriftType::Struct: {
			while (true) {
				uint8_t field_type_raw;
				if (!ReadByte(field_type_raw)) {
					return false;
				}
				auto field_type = static_cast<ThriftType>(field_type_raw);
				if (field_type == ThriftType::Stop) {
					return true;
				}
				int16_t field_id;
				if (!ReadI16(field_id) || !Skip(field_type)) {
					return false;
				}
			}
		}
		case ThriftType::Map: {
			uint8_t key_type_raw, val_type_raw;
			int32_t count;
			if (!ReadByte(key_type_raw) || !ReadByte(val_type_raw) || !ReadI32(count) || count < 0) {
				return false;
			}
			auto key_type = static_cast<ThriftType>(key_type_raw);
			auto val_type = static_cast<ThriftType>(val_type_raw);
			for (int32_t i = 0; i < count; i++) {
				if (!Skip(key_type) || !Skip(val_type)) {
					return false;
				}
			}
			return true;
		}
		case ThriftType::Set:
		case ThriftType::List: {
			uint8_t elem_type_raw;
			int32_t count;
			if (!ReadByte(elem_type_raw) || !ReadI32(count) || count < 0) {
				return false;
			}
			auto elem_type = static_cast<ThriftType>(elem_type_raw);
			for (int32_t i = 0; i < count; i++) {
				if (!Skip(elem_type)) {
					return false;
				}
			}
			return true;
		}
		default:
			return false;
		}
	}

private:
	int fd;
};

bool SendAll(int fd, const std::vector<uint8_t> &buf) {
	size_t offset = 0;
	while (offset < buf.size()) {
		ssize_t sent = send(fd, buf.data() + offset, buf.size() - offset, 0);
		if (sent <= 0) {
			return false;
		}
		offset += static_cast<size_t>(sent);
	}
	return true;
}

MetastoreResult<int> ConnectSocket(const std::string &host, uint16_t port) {
	addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	addrinfo *results = nullptr;
	std::string port_string = std::to_string(port);
	int gai_result = getaddrinfo(host.c_str(), port_string.c_str(), &hints, &results);
	if (gai_result != 0) {
		return MetastoreResult<int>::Error(MetastoreErrorCode::Transient, "HMS DNS resolution failed",
		                                   gai_strerror(gai_result), true);
	}

	int fd = -1;
	for (addrinfo *addr = results; addr != nullptr; addr = addr->ai_next) {
		fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
		if (fd < 0) {
			continue;
		}
		timeval timeout;
		timeout.tv_sec = 10;
		timeout.tv_usec = 0;
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
		if (connect(fd, addr->ai_addr, addr->ai_addrlen) == 0) {
			freeaddrinfo(results);
			return MetastoreResult<int>::Success(fd);
		}
		close(fd);
		fd = -1;
	}

	freeaddrinfo(results);
	return MetastoreResult<int>::Error(MetastoreErrorCode::Transient, "HMS socket connect failed", strerror(errno),
	                                   true);
}

MetastoreResult<int32_t> ReadMessageHeader(ThriftReader &reader, std::string &method_name,
                                           ThriftMessageType &message_type, int32_t &seqid) {
	int32_t version_and_type;
	if (!reader.ReadI32(version_and_type)) {
		return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Transient, "HMS response read failed", "", true);
	}
	if ((version_and_type & 0xFFFF0000) != THRIFT_VERSION_1) {
		return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Unsupported, "Unsupported Thrift version", "",
		                                       false);
	}
	message_type = static_cast<ThriftMessageType>(version_and_type & 0x000000FF);
	if (!reader.ReadString(method_name) || !reader.ReadI32(seqid)) {
		return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Transient, "HMS response header parse failed", "",
		                                       true);
	}
	return MetastoreResult<int32_t>::Success(0);
}

MetastoreResult<int32_t> ParseApplicationException(ThriftReader &reader) {
	std::string message;
	int32_t ex_type = 0;
	while (true) {
		uint8_t field_type_raw;
		if (!reader.ReadByte(field_type_raw)) {
			return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Transient, "Failed reading exception", "", true);
		}
		auto field_type = static_cast<ThriftType>(field_type_raw);
		if (field_type == ThriftType::Stop) {
			break;
		}
		int16_t field_id;
		if (!reader.ReadI16(field_id)) {
			return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Transient, "Failed reading exception", "", true);
		}
		if (field_id == 1 && field_type == ThriftType::String) {
			if (!reader.ReadString(message)) {
				return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Transient, "Failed reading exception", "",
				                                       true);
			}
		} else if (field_id == 2 && field_type == ThriftType::I32) {
			if (!reader.ReadI32(ex_type)) {
				return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Transient, "Failed reading exception", "",
				                                       true);
			}
		} else {
			if (!reader.Skip(field_type)) {
				return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Transient, "Failed reading exception", "",
				                                       true);
			}
		}
	}
	return MetastoreResult<int32_t>::Error(MetastoreErrorCode::Transient, "HMS remote exception", message, true);
}

bool ParseFieldSchema(ThriftReader &reader, MetastorePartitionColumn &col) {
	while (true) {
		uint8_t field_type_raw;
		if (!reader.ReadByte(field_type_raw)) {
			return false;
		}
		auto field_type = static_cast<ThriftType>(field_type_raw);
		if (field_type == ThriftType::Stop) {
			return true;
		}
		int16_t field_id;
		if (!reader.ReadI16(field_id)) {
			return false;
		}
		if (field_id == 1 && field_type == ThriftType::String) {
			if (!reader.ReadString(col.name)) {
				return false;
			}
		} else if (field_id == 2 && field_type == ThriftType::String) {
			if (!reader.ReadString(col.type)) {
				return false;
			}
		} else {
			if (!reader.Skip(field_type)) {
				return false;
			}
		}
	}
}

bool ParseSerdeInfo(ThriftReader &reader, MetastoreStorageDescriptor &sd) {
	while (true) {
		uint8_t field_type_raw;
		if (!reader.ReadByte(field_type_raw)) {
			return false;
		}
		auto field_type = static_cast<ThriftType>(field_type_raw);
		if (field_type == ThriftType::Stop) {
			return true;
		}
		int16_t field_id;
		if (!reader.ReadI16(field_id)) {
			return false;
		}
		if (field_id == 2 && field_type == ThriftType::String) {
			std::string serde;
			if (!reader.ReadString(serde)) {
				return false;
			}
			sd.serde_class = std::move(serde);
		} else if (field_id == 3 && field_type == ThriftType::Map) {
			uint8_t key_type_raw, val_type_raw;
			int32_t count;
			if (!reader.ReadByte(key_type_raw) || !reader.ReadByte(val_type_raw) || !reader.ReadI32(count) ||
			    count < 0) {
				return false;
			}
			auto key_type = static_cast<ThriftType>(key_type_raw);
			auto val_type = static_cast<ThriftType>(val_type_raw);
			for (int32_t i = 0; i < count; i++) {
				if (key_type == ThriftType::String && val_type == ThriftType::String) {
					std::string key;
					std::string val;
					if (!reader.ReadString(key) || !reader.ReadString(val)) {
						return false;
					}
					sd.serde_parameters[std::move(key)] = std::move(val);
				} else {
					if (!reader.Skip(key_type) || !reader.Skip(val_type)) {
						return false;
					}
				}
			}
		} else {
			if (!reader.Skip(field_type)) {
				return false;
			}
		}
	}
}

bool ParseStorageDescriptor(ThriftReader &reader, MetastoreStorageDescriptor &sd) {
	while (true) {
		uint8_t field_type_raw;
		if (!reader.ReadByte(field_type_raw)) {
			return false;
		}
		auto field_type = static_cast<ThriftType>(field_type_raw);
		if (field_type == ThriftType::Stop) {
			return true;
		}
		int16_t field_id;
		if (!reader.ReadI16(field_id)) {
			return false;
		}
		if (field_id == 2 && field_type == ThriftType::String) {
			if (!reader.ReadString(sd.location)) {
				return false;
			}
		} else if (field_id == 1 && field_type == ThriftType::List) {
			uint8_t elem_type_raw;
			int32_t count;
			if (!reader.ReadByte(elem_type_raw) || !reader.ReadI32(count) || count < 0) {
				return false;
			}
			auto elem_type = static_cast<ThriftType>(elem_type_raw);
			for (int32_t i = 0; i < count; i++) {
				if (elem_type == ThriftType::Struct) {
					MetastoreColumn col;
					MetastorePartitionColumn parsed_col;
					if (!ParseFieldSchema(reader, parsed_col)) {
						return false;
					}
					col.name = std::move(parsed_col.name);
					col.type = std::move(parsed_col.type);
					sd.columns.push_back(std::move(col));
				} else {
					if (!reader.Skip(elem_type)) {
						return false;
					}
				}
			}
		} else if (field_id == 3 && field_type == ThriftType::String) {
			std::string input_format;
			if (!reader.ReadString(input_format)) {
				return false;
			}
			sd.input_format = std::move(input_format);
		} else if (field_id == 4 && field_type == ThriftType::String) {
			std::string output_format;
			if (!reader.ReadString(output_format)) {
				return false;
			}
			sd.output_format = std::move(output_format);
		} else if (field_id == 7 && field_type == ThriftType::Struct) {
			if (!ParseSerdeInfo(reader, sd)) {
				return false;
			}
		} else {
			if (!reader.Skip(field_type)) {
				return false;
			}
		}
	}
}

bool ParseTableStruct(ThriftReader &reader, MetastoreTable &table) {
	while (true) {
		uint8_t field_type_raw;
		if (!reader.ReadByte(field_type_raw)) {
			return false;
		}
		auto field_type = static_cast<ThriftType>(field_type_raw);
		if (field_type == ThriftType::Stop) {
			return true;
		}
		int16_t field_id;
		if (!reader.ReadI16(field_id)) {
			return false;
		}
		if (field_id == 1 && field_type == ThriftType::String) {
			if (!reader.ReadString(table.name)) {
				return false;
			}
		} else if (field_id == 2 && field_type == ThriftType::String) {
			if (!reader.ReadString(table.namespace_name)) {
				return false;
			}
		} else if (field_id == 3 && field_type == ThriftType::String) {
			std::string owner;
			if (!reader.ReadString(owner)) {
				return false;
			}
			table.owner = std::move(owner);
		} else if (field_id == 7 && field_type == ThriftType::Struct) {
			if (!ParseStorageDescriptor(reader, table.storage_descriptor)) {
				return false;
			}
		} else if (field_id == 8 && field_type == ThriftType::List) {
			uint8_t elem_type_raw;
			int32_t count;
			if (!reader.ReadByte(elem_type_raw) || !reader.ReadI32(count) || count < 0) {
				return false;
			}
			auto elem_type = static_cast<ThriftType>(elem_type_raw);
			for (int32_t i = 0; i < count; i++) {
				if (elem_type == ThriftType::Struct) {
					MetastorePartitionColumn col;
					if (!ParseFieldSchema(reader, col)) {
						return false;
					}
					table.partition_spec.columns.push_back(std::move(col));
				} else {
					if (!reader.Skip(elem_type)) {
						return false;
					}
				}
			}
		} else if (field_id == 9 && field_type == ThriftType::Map) {
			uint8_t key_type_raw, val_type_raw;
			int32_t count;
			if (!reader.ReadByte(key_type_raw) || !reader.ReadByte(val_type_raw) || !reader.ReadI32(count) ||
			    count < 0) {
				return false;
			}
			auto key_type = static_cast<ThriftType>(key_type_raw);
			auto val_type = static_cast<ThriftType>(val_type_raw);
			for (int32_t i = 0; i < count; i++) {
				if (key_type == ThriftType::String && val_type == ThriftType::String) {
					std::string key;
					std::string val;
					if (!reader.ReadString(key) || !reader.ReadString(val)) {
						return false;
					}
					table.properties[std::move(key)] = std::move(val);
				} else {
					if (!reader.Skip(key_type) || !reader.Skip(val_type)) {
						return false;
					}
				}
			}
		} else {
			if (!reader.Skip(field_type)) {
				return false;
			}
		}
	}
}

std::vector<std::string> ParsePartitionNameValues(const std::string &partition_name) {
	std::vector<std::string> values;
	std::stringstream ss(partition_name);
	std::string segment;
	while (std::getline(ss, segment, '/')) {
		auto eq_pos = segment.find('=');
		if (eq_pos == std::string::npos || eq_pos + 1 >= segment.size()) {
			values.push_back(segment);
		} else {
			values.push_back(segment.substr(eq_pos + 1));
		}
	}
	return values;
}

std::string NormalizeFileLocation(std::string location) {
	if (location.rfind("file://", 0) == 0) {
		location = location.substr(7);
	} else if (location.rfind("file:", 0) == 0) {
		location = location.substr(5);
	}
	while (!location.empty() && location.back() == '/') {
		location.pop_back();
	}
	return location;
}

std::vector<std::string> DiscoverLocalPartitionNames(const std::string &table_location, idx_t partition_depth) {
	std::vector<std::string> names;
	if (partition_depth == 0) {
		return names;
	}
	auto root = NormalizeFileLocation(table_location);
	if (root.empty()) {
		return names;
	}
	if (!std::filesystem::exists(root)) {
		return names;
	}

	std::function<void(const std::string &, idx_t, std::vector<std::string> &)> walk;
	walk = [&](const std::string &path, idx_t depth, std::vector<std::string> &segments) {
		if (depth == partition_depth) {
			std::string partition_name;
			for (idx_t i = 0; i < segments.size(); i++) {
				if (i > 0) {
					partition_name += "/";
				}
				partition_name += segments[i];
			}
			names.push_back(std::move(partition_name));
			return;
		}
		for (auto &entry : std::filesystem::directory_iterator(path)) {
			if (!entry.is_directory()) {
				continue;
			}
			auto segment = entry.path().filename().string();
			if (segment.find('=') == std::string::npos) {
				continue;
			}
			segments.push_back(std::move(segment));
			walk(entry.path().string(), depth + 1, segments);
			segments.pop_back();
		}
	};

	std::vector<std::string> segments;
	try {
		walk(root, 0, segments);
	} catch (...) {
		return {};
	}
	std::sort(names.begin(), names.end());
	names.erase(std::unique(names.begin(), names.end()), names.end());
	return names;
}

MetastoreResult<std::vector<std::string>> ParseStringListResult(ThriftReader &reader) {
	while (true) {
		uint8_t field_type_raw;
		if (!reader.ReadByte(field_type_raw)) {
			return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient,
			                                                        "Malformed HMS response", "", true);
		}
		auto field_type = static_cast<ThriftType>(field_type_raw);
		if (field_type == ThriftType::Stop) {
			return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::NotFound, "Empty HMS result",
			                                                        "", false);
		}
		int16_t field_id;
		if (!reader.ReadI16(field_id)) {
			return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient,
			                                                        "Malformed HMS response", "", true);
		}
		if ((field_id == 0 || field_id == 1) && field_type == ThriftType::List) {
			uint8_t elem_type_raw;
			int32_t count;
			if (!reader.ReadByte(elem_type_raw) || !reader.ReadI32(count) || count < 0) {
				return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient,
				                                                        "Malformed HMS list payload", "", true);
			}
			auto elem_type = static_cast<ThriftType>(elem_type_raw);
			if (elem_type != ThriftType::String) {
				return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Unsupported,
				                                                        "Unexpected HMS list element type", "", false);
			}
			std::vector<std::string> values;
			values.reserve(static_cast<size_t>(count));
			for (int32_t i = 0; i < count; i++) {
				std::string item;
				if (!reader.ReadString(item)) {
					return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient,
					                                                        "Malformed HMS list element", "", true);
				}
				values.push_back(std::move(item));
			}
			while (true) {
				uint8_t trailing_type;
				if (!reader.ReadByte(trailing_type)) {
					break;
				}
				if (static_cast<ThriftType>(trailing_type) == ThriftType::Stop) {
					break;
				}
				int16_t trailing_id;
				if (!reader.ReadI16(trailing_id) || !reader.Skip(static_cast<ThriftType>(trailing_type))) {
					break;
				}
			}
			return MetastoreResult<std::vector<std::string>>::Success(std::move(values));
		}
		if (!reader.Skip(field_type)) {
			return MetastoreResult<std::vector<std::string>>::Error(MetastoreErrorCode::Transient,
			                                                        "Malformed HMS response", "", true);
		}
	}
}

template <typename BuildArgs>
MetastoreResult<int> InvokeRpc(const HmsConfig &config, const std::string &method_name, int32_t seqid,
                               BuildArgs build_args,
                               const std::function<MetastoreResult<int>(ThriftReader &)> &parse_result) {
	auto sock_result = ConnectSocket(config.endpoint, config.port);
	if (!sock_result.IsOk()) {
		return MetastoreResult<int>::Error(sock_result.error.code, std::move(sock_result.error.message),
		                                   std::move(sock_result.error.detail), sock_result.error.retryable);
	}
	SocketHandle socket(sock_result.value);

	ThriftWriter writer;
	writer.WriteMessageBegin(method_name, ThriftMessageType::Call, seqid);
	writer.WriteStructBegin();
	build_args(writer);
	writer.WriteFieldStop();
	writer.WriteStructEnd();

	if (!SendAll(socket.fd, writer.Data())) {
		return MetastoreResult<int>::Error(MetastoreErrorCode::Transient, "Failed to send HMS request", "", true);
	}

	ThriftReader reader(socket.fd);
	std::string response_method;
	ThriftMessageType response_type;
	int32_t response_seqid;
	auto header_status = ReadMessageHeader(reader, response_method, response_type, response_seqid);
	if (!header_status.IsOk()) {
		return header_status;
	}
	if (response_type == ThriftMessageType::Exception) {
		return ParseApplicationException(reader);
	}
	if (response_type != ThriftMessageType::Reply) {
		return MetastoreResult<int>::Error(MetastoreErrorCode::Transient, "Unexpected HMS reply type", "", true);
	}
	if (response_method != method_name || response_seqid != seqid) {
		return MetastoreResult<int>::Error(MetastoreErrorCode::Transient, "HMS reply header mismatch", "", true);
	}
	return parse_result(reader);
}

} // namespace

HmsConnector::HmsConnector(HmsConfig config) : config_(std::move(config)) {
}

MetastoreResult<std::vector<MetastoreNamespace>> HmsConnector::ListNamespaces() {
	auto status = InvokeRpc(
	    config_, "get_all_databases", 1, [&](ThriftWriter &writer) {},
	    [&](ThriftReader &reader) {
		    auto parsed = ParseStringListResult(reader);
		    if (!parsed.IsOk()) {
			    return MetastoreResult<int>::Error(parsed.error.code, std::move(parsed.error.message),
			                                       std::move(parsed.error.detail), parsed.error.retryable);
		    }
		    namespaces_cache = std::move(parsed.value);
		    return MetastoreResult<int>::Success(0);
	    });
	if (!status.IsOk()) {
		return MetastoreResult<std::vector<MetastoreNamespace>>::Error(
		    status.error.code, std::move(status.error.message), std::move(status.error.detail), status.error.retryable);
	}
	std::vector<MetastoreNamespace> result;
	result.reserve(namespaces_cache.size());
	for (auto &name : namespaces_cache) {
		MetastoreNamespace ns;
		ns.name = name;
		ns.catalog = "hms";
		result.push_back(std::move(ns));
	}
	return MetastoreResult<std::vector<MetastoreNamespace>>::Success(std::move(result));
}

MetastoreResult<std::vector<std::string>> HmsConnector::ListTables(const std::string &namespace_name) {
	std::vector<std::string> tables;
	auto status = InvokeRpc(
	    config_, "get_all_tables", 2,
	    [&](ThriftWriter &writer) {
		    writer.WriteFieldBegin(ThriftType::String, 1);
		    writer.WriteString(namespace_name);
	    },
	    [&](ThriftReader &reader) {
		    auto parsed = ParseStringListResult(reader);
		    if (!parsed.IsOk()) {
			    return MetastoreResult<int>::Error(parsed.error.code, std::move(parsed.error.message),
			                                       std::move(parsed.error.detail), parsed.error.retryable);
		    }
		    tables = std::move(parsed.value);
		    return MetastoreResult<int>::Success(0);
	    });
	if (!status.IsOk()) {
		return MetastoreResult<std::vector<std::string>>::Error(status.error.code, std::move(status.error.message),
		                                                        std::move(status.error.detail), status.error.retryable);
	}
	return MetastoreResult<std::vector<std::string>>::Success(std::move(tables));
}

MetastoreResult<MetastoreTable> HmsConnector::GetTable(const std::string &namespace_name,
                                                       const std::string &table_name) {
	MetastoreTable table;
	table.catalog = "hms";
	auto status = InvokeRpc(
	    config_, "get_table", 3,
	    [&](ThriftWriter &writer) {
		    writer.WriteFieldBegin(ThriftType::String, 1);
		    writer.WriteString(namespace_name);
		    writer.WriteFieldBegin(ThriftType::String, 2);
		    writer.WriteString(table_name);
	    },
	    [&](ThriftReader &reader) {
		    bool found_success = false;
		    while (true) {
			    uint8_t field_type_raw;
			    if (!reader.ReadByte(field_type_raw)) {
				    return MetastoreResult<int>::Error(MetastoreErrorCode::Transient,
				                                       "Malformed HMS get_table response", "", true);
			    }
			    auto field_type = static_cast<ThriftType>(field_type_raw);
			    if (field_type == ThriftType::Stop) {
				    break;
			    }
			    int16_t field_id;
			    if (!reader.ReadI16(field_id)) {
				    return MetastoreResult<int>::Error(MetastoreErrorCode::Transient,
				                                       "Malformed HMS get_table response", "", true);
			    }
			    if (field_id == 0 && field_type == ThriftType::Struct) {
				    if (!ParseTableStruct(reader, table)) {
					    return MetastoreResult<int>::Error(MetastoreErrorCode::Transient,
					                                       "Failed to parse HMS table payload", "", true);
				    }
				    found_success = true;
			    } else {
				    if (!reader.Skip(field_type)) {
					    return MetastoreResult<int>::Error(MetastoreErrorCode::Transient,
					                                       "Malformed HMS get_table response", "", true);
				    }
			    }
		    }
		    if (!found_success) {
			    return MetastoreResult<int>::Error(MetastoreErrorCode::NotFound, "HMS table not found", "", false);
		    }
		    return MetastoreResult<int>::Success(0);
	    });
	if (!status.IsOk()) {
		return MetastoreResult<MetastoreTable>::Error(status.error.code, std::move(status.error.message),
		                                              std::move(status.error.detail), status.error.retryable);
	}
	auto mapped = HmsMapper::MapTable("hms", namespace_name, table_name, std::move(table.storage_descriptor),
	                                  std::move(table.partition_spec), std::move(table.properties));
	if (!mapped.IsOk()) {
		return mapped;
	}
	auto final_table = std::move(mapped.value);
	final_table.owner = std::move(table.owner);
	return MetastoreResult<MetastoreTable>::Success(std::move(final_table));
}

MetastoreResult<std::vector<MetastorePartitionValue>> HmsConnector::ListPartitions(const std::string &namespace_name,
                                                                                   const std::string &table_name,
                                                                                   const std::string &predicate) {
	(void)predicate;

	std::vector<std::string> partition_names;
	auto status = InvokeRpc(
	    config_, "get_partition_names", 4,
	    [&](ThriftWriter &writer) {
		    writer.WriteFieldBegin(ThriftType::String, 1);
		    writer.WriteString(namespace_name);
		    writer.WriteFieldBegin(ThriftType::String, 2);
		    writer.WriteString(table_name);
		    writer.WriteFieldBegin(ThriftType::I16, 3);
		    writer.WriteI16(-1);
	    },
	    [&](ThriftReader &reader) {
		    auto parsed = ParseStringListResult(reader);
		    if (!parsed.IsOk()) {
			    return MetastoreResult<int>::Error(parsed.error.code, std::move(parsed.error.message),
			                                       std::move(parsed.error.detail), parsed.error.retryable);
		    }
		    partition_names = std::move(parsed.value);
		    return MetastoreResult<int>::Success(0);
	    });
	if (!status.IsOk()) {
		if (status.error.code == MetastoreErrorCode::NotFound) {
			partition_names.clear();
		} else {
			return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(
			    status.error.code, std::move(status.error.message), std::move(status.error.detail),
			    status.error.retryable);
		}
	}
	auto table_result = GetTable(namespace_name, table_name);
	if (!table_result.IsOk()) {
		return MetastoreResult<std::vector<MetastorePartitionValue>>::Error(
		    table_result.error.code, std::move(table_result.error.message), std::move(table_result.error.detail),
		    table_result.error.retryable);
	}
	const auto &table_location = table_result.value.storage_descriptor.location;
	const bool table_location_has_trailing_slash = !table_location.empty() && table_location.back() == '/';
	if (table_result.value.IsPartitioned()) {
		auto discovered = DiscoverLocalPartitionNames(table_location, table_result.value.partition_spec.columns.size());
		if (!discovered.empty()) {
			partition_names = std::move(discovered);
		}
	}

	std::vector<MetastorePartitionValue> result;
	result.reserve(partition_names.size());
	for (auto &name : partition_names) {
		MetastorePartitionValue pv;
		pv.values = ParsePartitionNameValues(name);
		if (!table_location.empty()) {
			if (table_location_has_trailing_slash) {
				pv.location = table_location + name;
			} else {
				pv.location = table_location + "/" + name;
			}
		}
		result.push_back(std::move(pv));
	}
	return MetastoreResult<std::vector<MetastorePartitionValue>>::Success(std::move(result));
}

MetastoreResult<MetastoreTableProperties> HmsConnector::GetTableStats(const std::string &namespace_name,
                                                                      const std::string &table_name) {
	auto table_result = GetTable(namespace_name, table_name);
	if (!table_result.IsOk()) {
		return MetastoreResult<MetastoreTableProperties>::Error(
		    table_result.error.code, std::move(table_result.error.message), std::move(table_result.error.detail),
		    table_result.error.retryable);
	}
	return MetastoreResult<MetastoreTableProperties>::Success(std::move(table_result.value.properties));
}

//===--------------------------------------------------------------------===//
// ParseHmsEndpoint
//===--------------------------------------------------------------------===//
static bool ParsePort(const std::string &port_str, uint16_t &port_out) {
	if (port_str.empty()) {
		return false;
	}
	for (char c : port_str) {
		if (c < '0' || c > '9') {
			return false;
		}
	}
	uint64_t val;
	try {
		val = std::stoul(port_str);
	} catch (...) {
		return false;
	}
	if (val == 0 || val > 65535) {
		return false;
	}
	port_out = static_cast<uint16_t>(val);
	return true;
}

HmsConfig ParseHmsEndpoint(const std::string &endpoint) {
	MetastoreErrorTag tag {"hms", "ParseHmsEndpoint", false};

	if (endpoint.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag, "HMS endpoint URI is empty");
	}

	HmsConfig config;
	std::string remainder;

	// Detect and strip scheme
	const std::string thrift_ssl_scheme = "thrift+ssl://";
	const std::string thrift_scheme = "thrift://";

	if (endpoint.size() >= thrift_ssl_scheme.size() &&
	    endpoint.substr(0, thrift_ssl_scheme.size()) == thrift_ssl_scheme) {
		config.transport = HmsTransport::ThriftTLS;
		remainder = endpoint.substr(thrift_ssl_scheme.size());
	} else if (endpoint.size() >= thrift_scheme.size() && endpoint.substr(0, thrift_scheme.size()) == thrift_scheme) {
		config.transport = HmsTransport::Thrift;
		remainder = endpoint.substr(thrift_scheme.size());
	} else {
		config.transport = HmsTransport::Thrift;
		remainder = endpoint;
	}

	if (remainder.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
		                         "HMS endpoint URI has no host: '" + endpoint + "'");
	}

	// Split host:port
	auto colon_pos = remainder.rfind(':');
	if (colon_pos != std::string::npos && colon_pos > 0) {
		std::string host_part = remainder.substr(0, colon_pos);
		std::string port_part = remainder.substr(colon_pos + 1);

		uint16_t parsed_port;
		if (ParsePort(port_part, parsed_port)) {
			config.endpoint = host_part;
			config.port = parsed_port;
		} else {
			throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
			                         "Invalid port in HMS endpoint URI: '" + endpoint + "'");
		}
	} else {
		config.endpoint = remainder;
		config.port = 9083;
	}

	if (config.endpoint.empty()) {
		throw MetastoreException(MetastoreErrorCode::InvalidConfig, tag,
		                         "HMS endpoint URI has empty host: '" + endpoint + "'");
	}

	return config;
}

} // namespace duckdb
