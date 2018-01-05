#include <endian.h>
#include <zlib.h>

#include <cstdint>
#include <cstring>
#include <sstream>
#include <vector>

#include "lib/recordio/recordio.h"

namespace grail {

RecordIOReader::~RecordIOReader() {}

namespace {
constexpr int NumMagicBytes = 8;
constexpr int SizeOffset = NumMagicBytes;
constexpr int CrcOffset = NumMagicBytes + 8;
constexpr int Crc32Size = 4;
constexpr int DataOffset = NumMagicBytes + 8 + Crc32Size;
// HeaderSize is the size in bytes of the recordio header.
constexpr int HeaderSize = DataOffset;

const unsigned char Magic[NumMagicBytes] = {0xfc, 0xae, 0x95, 0x31,
                                            0xf0, 0xd9, 0xbd, 0x20};

// MaxReadRecordSize defines a max size for a record when reading to avoid
// crashes for unreasonable requests.
constexpr uint64_t MaxReadRecordSize = 1ULL << 29;

class ReaderImpl : public RecordIOReader {
 public:
  explicit ReaderImpl(std::istream* in) : in_(in) {}

  bool Scan() {
    uint64_t size;
    if (!ReadHeader(&size)) {
      return false;
    }
    buf_.resize(size);
    int n = ReadBytes(buf_.data(), size);
    if (static_cast<uint64_t>(n) != size) {
      std::ostringstream msg;
      msg << "failed to read " << size << " byte body (found " << in_->gcount()
          << " bytes";
      err_ = msg.str();
      return false;
    }
    return true;
  }

  std::vector<char>* Mutable() { return &buf_; }

  std::string Error() { return err_; }

 private:
  // Read the header part of the block from in_. On success, set *size to the
  // length of the rest of the block.
  bool ReadHeader(uint64_t* size) {
    char header[HeaderSize];
    int n = ReadBytes(header, sizeof(header));
    if (n <= 0) {
      return false;  // EOF
    }
    if (n != sizeof header) {
      std::ostringstream msg;
      msg << "Corrupt header; read " << n << " bytes, expect " << sizeof header
          << " bytes";
      err_ = msg.str();
      return false;
    }
    if (memcmp(header, Magic, sizeof Magic) != 0) {
      err_ = "wrong header magic";
      return false;
    }
    memcpy(size, header + SizeOffset, sizeof *size);
    *size = le64toh(*size);
    uint32_t expected_crc;
    memcpy(&expected_crc, header + CrcOffset, sizeof expected_crc);
    expected_crc = le32toh(expected_crc);
    auto actual_crc =
        crc32(0, reinterpret_cast<const Bytef*>(header + SizeOffset),
              CrcOffset - SizeOffset);
    if (actual_crc != expected_crc) {
      std::ostringstream msg;
      msg << "corrupt header crc, expect " << expected_crc << " found "
          << actual_crc;
      err_ = msg.str();
      return false;
    }
    if (*size > MaxReadRecordSize) {
      std::ostringstream msg;
      msg << "unreasonably large read record encountered: " << *size << " > "
          << MaxReadRecordSize << " bytes";
      err_ = msg.str();
      return false;
    }
    return true;
  }

  // Read "bytes" byte from in_.
  int ReadBytes(char* data, int bytes) {
    int remaining = bytes;
    while (remaining > 0) {
      in_->read(data, remaining);
      int n = in_->gcount();
      if (n <= 0) {
        break;
      }
      data += n;
      remaining -= n;
    }
    return bytes - remaining;
  }

  std::string err_;
  std::istream* const in_;
  std::vector<char> buf_;
};
}  // namespace
}  // namespace grail

std::unique_ptr<grail::RecordIOReader> grail::NewRecordIOReader(
    std::istream* in) {
  return std::unique_ptr<RecordIOReader>(new ReaderImpl(in));
}
