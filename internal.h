#ifndef LIB_RECORDIO_INTERNAL_H_
#define LIB_RECORDIO_INTERNAL_H_

#include <zlib.h>

#include <array>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace grail {
namespace recordio {
namespace internal {

typedef std::string Error;
typedef std::array<uint8_t, 8> Magic;

const Magic MagicInvalid = {{0xe4, 0xe7, 0x9a, 0xc1, 0xb3, 0xf6, 0xb7, 0xa2}};
const Magic MagicUnpacked = {{0xfc, 0xae, 0x95, 0x31, 0xf0, 0xd9, 0xbd, 0x20}};
const Magic MagicPacked = {{0x2e, 0x76, 0x47, 0xeb, 0x34, 0x07, 0x3c, 0x2e}};
const Magic MagicHeader = {{0xd9, 0xe1, 0xd9, 0x5c, 0xc2, 0x16, 0x04, 0xf7}};
const Magic MagicTrailer = {{0x2e, 0x76, 0x47, 0xeb, 0x34, 0x07, 0x3c, 0x2e}};

std::string MagicDebugString(const Magic& m);

inline uint32_t Crc32(const uint8_t* data, int bytes) {
  return crc32(0, data, bytes);
}

// Read "bytes" byte from in_. Returns the actual number of bytes read.
int ReadBytes(std::istream* in, char* data, int bytes);
int ReadBytes(std::istream* in, uint8_t* data, int bytes);

// Read exactly "bytes". Returns an error string on error.
Error ReadFull(std::istream* in, uint8_t* data, int bytes);

// Seek to the given abs offset. Returns an error string on error.
Error AbsSeek(std::istream* in, int64_t off);
// Get the current seek offset. Returns an error string on error.
Error Tell(std::istream* in, int64_t* off);

class Cleanup {
 public:
  virtual ~Cleanup() {}
};

// For accumulating errors. Thread compatible.
class ErrorReporter {
 public:
  ErrorReporter() {}

  // Set an error. If this function is called multiple times, only the first
  // non-OK error will be taken.
  void Set(const Error& err) {
    if (err_.empty() && !err.empty()) {
      err_ = err;
    }
  }

  // See if at least one non-OK status was set in Set().
  bool Ok() const { return err_.empty(); }

  // Return the error message.
  const Error& Err() const { return err_; }

 private:
  Error err_;
  ErrorReporter(const ErrorReporter&) = delete;
};

// BinaryParser is parses contents in a buffer.
class BinaryParser {
 public:
  // Arrange to parse data in range [data,data+bytes). Any parsing error will be
  // reported in err.
  BinaryParser(const uint8_t* data, int bytes, ErrorReporter* err)
      : data_(data), bytes_(bytes), err_(err) {}

  // Return the pointer to the unread part of the data.
  const uint8_t* Data() { return data_; }

  // Consume "bytes" of data. Return the pointer to the data consumed.  Returns
  // nil and sets err_ if less than "bytes" are remaining in the buffer.
  const uint8_t* ReadBytes(int bytes);

  // Read a string of exactly "bytes". On error, returns "" and sets err_.
  std::string ReadString(int bytes);
  // Read a 64bit little-endian 64bit uint. On error, returns 0 and sets err_.
  uint64_t ReadLEUint64();
  // Read a 32bit little-endian 64bit uint. On error, returns 0 and sets err_.
  uint32_t ReadLEUint32();
  // Read a uvarint. On error, returns 0 and sets err_.
  uint64_t ReadUVarint();
  // Read a varint. On error, returns 0 and sets err_.
  int64_t ReadVarint();

  ErrorReporter* err() const { return err_; }

 private:
  const uint8_t* data_;
  int bytes_;
  ErrorReporter* err_;
};

std::string StrError(const std::string& prefix);

// Span<T> is like vector<T>, but doesn't own data.
//
// TODO(saito) Replace with std::span once c++-20 comes out.
template <typename T>
class Span {
 public:
  Span() : data_(nullptr), size_(0) {}
  Span(const Span<T>& other) : data_(other.data_), size_(other.size_) {}
  Span<T>& operator=(const Span<T>& other) {
    data_ = other.data_;
    size_ = other.size_;
    return *this;
  }

  Span(const T* data, size_t size) : data_(data), size_(size) {}
  explicit Span(const std::vector<T>* data)
      : data_(&data->at(0)), size_(data->size()) {}

  const T* data() const { return data_; }
  size_t size() const { return size_; }

  const T* begin() const { return data_; }
  const T* end() const { return data_ + size_; }
  const T& operator[](int index) const { return data_[index]; }

 private:
  const T* data_;
  size_t size_;
};

typedef Span<uint8_t> ByteSpan;
typedef Span<ByteSpan> IoVec;

// Compute the total size of the iov.
inline size_t IoVecSize(IoVec iov) {
  size_t n = 0;
  for (size_t i = 0; i < iov.size(); i++) {
    n += iov[i].size();
  }
  return n;
}

// Convert an iovec to a flat vector.
std::vector<uint8_t> IoVecFlatten(IoVec iov);

}  // namespace internal
}  // namespace recordio
}  // namespace grail

#endif  // LIB_RECORDIO_INTERNAL_H_
