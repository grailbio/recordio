#include "./internal.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>
#include <sstream>

#include "./portable_endian.h"

namespace grail {
namespace recordio {
namespace internal {

std::string MagicDebugString(const Magic& m) {
  std::ostringstream s;
  int n = 0;
  s << "[";
  for (auto b : m) {
    char buf[128];
    snprintf(buf, sizeof(buf), "%x", b);
    if (n++ > 0) s << ",";
    s << buf;
  }
  s << "]";
  return s.str();
}

Error ReadFull(ReadSeeker* in, uint8_t* data, int bytes) {
  ssize_t n;
  Error err = in->Read(data, bytes, &n);
  if (err != "") return err;
  if (n != bytes) {
    std::ostringstream msg;
    msg << "Failed to read " << bytes << " bytes from stream, read " << n
        << " bytes instead: " << std::strerror(errno);
    return msg.str();
  }
  return "";
}

std::string StrError(const std::string& prefix) {
  std::ostringstream msg;
  msg << prefix << ": " << std::strerror(errno);
  return msg.str();
}

Error AbsSeek(ReadSeeker* in, off_t off) {
  off_t new_off;
  Error err = in->Seek(off, SEEK_SET, &new_off);
  if (err != "") return err;
  if (new_off != off) {
    std::ostringstream msg;
    msg << "failed to seek to offset " << off << ", " << SEEK_SET << ": (got "
        << new_off << "): " << std::strerror(errno);
    return msg.str();
  }
  return "";
}

uint64_t BinaryParser::ReadLEUint64() {
  uint64_t v;
  if (bytes_ < static_cast<int>(sizeof v)) {
    err_->Set("Failed to read uint64");
    return 0;
  }
  memcpy(&v, data_, sizeof v);
  v = le64toh(v);
  data_ += sizeof v;
  bytes_ -= sizeof v;
  return v;
}

uint32_t BinaryParser::ReadLEUint32() {
  uint32_t v;
  if (bytes_ < static_cast<int>(sizeof v)) {
    err_->Set("Failed to read uint32");
    return 0;
  }
  memcpy(&v, data_, sizeof v);
  v = le32toh(v);
  data_ += sizeof v;
  bytes_ -= sizeof v;
  return v;
}

uint64_t BinaryParser::ReadUVarint() {
  uint64_t v = 0;
  int shift = 0;
  int i = 0;
  while (bytes_ > 0) {
    auto b = *data_;
    if (b < 0x80) {
      if (i > 9 || (i == 9 && b > 1)) {
        err_->Set("Failed to read uvarint");
        return 0;
      }
      --bytes_;
      ++data_;
      v |= (static_cast<uint64_t>(b) << shift);
      return v;
    }
    v |= static_cast<uint64_t>(b & 0x7f) << shift;
    shift += 7;
    --bytes_;
    ++data_;
    ++i;
  }
  return v;
}

int64_t BinaryParser::ReadVarint() {
  uint64_t u;
  u = ReadUVarint();
  uint64_t x = u >> 1;
  if ((u & 1) != 0) {
    x = ~x;
  }
  int64_t v;
  memcpy(&v, &x, sizeof(x));
  return v;
}

const uint8_t* BinaryParser::ReadBytes(int bytes) {
  if (bytes_ < bytes) {
    std::ostringstream msg;
    msg << "ReadBytes: failed to read " << bytes << " bytes";
    err_->Set(msg.str());
    return nullptr;
  }
  const uint8_t* p = data_;
  data_ += bytes;
  bytes_ -= bytes;
  return p;
}

std::string BinaryParser::ReadString(int bytes) {
  std::string s;
  const uint8_t* v = ReadBytes(bytes);
  if (v == nullptr) return s;
  s.assign(reinterpret_cast<const char*>(v), bytes);
  return s;
}

std::vector<uint8_t> IoVecFlatten(IoVec iov) {
  std::vector<uint8_t> buf;
  buf.resize(IoVecSize(iov));
  size_t n = 0;
  for (size_t i = 0; i < iov.size(); i++) {
    std::copy(iov[i].begin(), iov[i].end(), buf.data() + n);
    n += iov[i].size();
  }
  return buf;
}

bool HasSuffix(const std::string& str, const std::string& suffix) {
  if (str.size() < suffix.size()) {
    return false;
  }
  return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}
}  // namespace internal
}  // namespace recordio
}  // namespace grail
