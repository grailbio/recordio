#include <endian.h>
#include <zlib.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <sstream>
#include <vector>

#include "lib/file/names.h"
#include "lib/recordio/recordio.h"

namespace grail {

RecordIOReader::~RecordIOReader() {}
RecordIOTransformer::~RecordIOTransformer() {}

namespace {
typedef std::array<uint8_t, 8> Magic;

constexpr int SizeOffset = sizeof(Magic);
constexpr int CrcOffset = sizeof(Magic) + 8;
constexpr int Crc32Size = 4;
constexpr int DataOffset = sizeof(Magic) + 8 + Crc32Size;
// HeaderSize is the size in bytes of the recordio header.
constexpr int HeaderSize = DataOffset;

const Magic MagicUnpacked = {0xfc, 0xae, 0x95, 0x31, 0xf0, 0xd9, 0xbd, 0x20};
const Magic MagicPacked = {0x2e, 0x76, 0x47, 0xeb, 0x34, 0x07, 0x3c, 0x2e};

// MaxReadRecordSize defines a max size for a record when reading to avoid
// crashes for unreasonable requests.
constexpr uint64_t MaxReadRecordSize = 1ULL << 29;

uint32_t Crc32(const char* data, int bytes) {
  return crc32(0, reinterpret_cast<const Bytef*>(data), bytes);
}

std::string RunTransformer(RecordIOTransformer* t, std::vector<char>* buf,
                           int buf_off) {
  RecordIOReader::Span in{buf->data() + buf_off, buf->size() - buf_off};
  std::string err;
  RecordIOReader::Span out = t->Transform(in, &err);
  if (!err.empty()) return err;

  buf->resize(out.size);
  std::copy(out.data, out.data + out.size, buf->data());
  return "";
}

class BinaryParser {
 public:
  BinaryParser(const char* data, int bytes) : data_(data), bytes_(bytes) {}
  const char* Data() { return data_; }

  bool ReadLEUint64(uint64_t* v) {
    if (bytes_ < static_cast<int>(sizeof *v)) return false;
    memcpy(v, data_, sizeof *v);
    *v = le64toh(*v);
    data_ += sizeof *v;
    bytes_ -= sizeof *v;
    return true;
  }

  bool ReadLEUint32(uint32_t* v) {
    if (bytes_ < static_cast<int>(sizeof *v)) return false;
    memcpy(v, data_, sizeof *v);
    *v = le32toh(*v);
    data_ += sizeof *v;
    bytes_ -= sizeof *v;
    return true;
  }

  bool ReadUVarint(uint64_t* v) {
    *v = 0;
    int shift = 0;
    int i = 0;
    while (bytes_ > 0) {
      auto b = *reinterpret_cast<const uint8_t*>(data_);
      if (b < 0x80) {
        if (i > 9 || (i == 9 && b > 1)) {
          return false;
        }
        --bytes_;
        ++data_;
        *v |= (static_cast<uint64_t>(b) << shift);
        return true;
      }
      *v |= static_cast<uint64_t>(b & 0x7f) << shift;
      shift += 7;
      --bytes_;
      ++data_;
      ++i;
    }
    return false;
  }

 private:
  const char* data_;
  int bytes_;
};

class Cleanup {
 public:
  virtual ~Cleanup() {}
};

// BaseReader implements a raw reader w/o any transformation.
class BaseReader {
 public:
  explicit BaseReader(std::istream* in, Magic magic,
                      std::unique_ptr<Cleanup> cleanup)
      : in_(in), magic_(magic), cleanup_(std::move(cleanup)) {
  }

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
      SetError(msg.str());
      return false;
    }
    return true;
  }

  std::vector<char>* Mutable() { return &buf_; }
  std::string Error() { return err_; }
  void SetError(const std::string& err) {
    if (err_.empty()) {
      err_ = err;
    }
  }

 private:
  // Read the header part of the block from in_. On success, set *size to the
  // length of the rest of the block.
  bool ReadHeader(uint64_t* size) {
    char header[HeaderSize];
    int n = ReadBytes(header, sizeof(header));
    if (n <= 0) {
      if (!in_->eof()) {
        std::ostringstream msg;
        msg << "Failed to read file: " << strerror(errno);
        SetError(msg.str());
      }
      return false;  // EOF
    }
    if (n != sizeof header) {
      std::ostringstream msg;
      msg << "Corrupt header; read " << n << " bytes, expect " << sizeof header
          << " bytes";
      SetError(msg.str());
      return false;
    }
    if (memcmp(header, magic_.data(), sizeof magic_) != 0) {
      SetError("wrong header magic");
      return false;
    }

    BinaryParser parser(header + SizeOffset, sizeof(header) - SizeOffset);
    if (!parser.ReadLEUint64(size)) {
      SetError("header too small (size)");
      return false;
    }
    uint32_t expected_crc;
    if (!parser.ReadLEUint32(&expected_crc)) {
      SetError("header too small (crc)");
      return false;
    }
    auto actual_crc = Crc32(header + SizeOffset, CrcOffset - SizeOffset);
    if (actual_crc != expected_crc) {
      std::ostringstream msg;
      msg << "corrupt header crc, expect " << expected_crc << " found "
          << actual_crc;
      SetError(msg.str());
      return false;
    }
    if (*size > MaxReadRecordSize) {
      std::ostringstream msg;
      msg << "unreasonably large read record encountered: " << *size << " > "
          << MaxReadRecordSize << " bytes";
      SetError(msg.str());
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

  std::istream* const in_;
  const Magic magic_;
  const std::unique_ptr<Cleanup> cleanup_;
  std::string err_;
  std::vector<char> buf_;
};

// Implementation of an unpacked reader.
class UnpackedReaderImpl : public RecordIOReader {
 public:
  explicit UnpackedReaderImpl(std::istream* in,
                              std::unique_ptr<RecordIOTransformer> transformer,
                              std::unique_ptr<Cleanup> cleanup)
      : r_(new BaseReader(in, MagicUnpacked, std::move(cleanup))),
        transformer_(std::move(transformer)) {}

  bool Scan() {
    if (!r_->Scan()) return false;
    block_ = std::move(*r_->Mutable());
    if (transformer_ != nullptr) {
      const std::string err = RunTransformer(transformer_.get(), &block_, 0);
      if (!err.empty()) {
        r_->SetError(err);
        return false;
      }
    }
    return true;
  }

  std::vector<char>* Mutable() { return &block_; }
  Span Get() { return Span{block_.data(), block_.size()}; }
  std::string Error() { return r_->Error(); }

 private:
  std::unique_ptr<BaseReader> r_;  // Underlying unpacked reader.
  const std::unique_ptr<RecordIOTransformer> transformer_;
  std::vector<char> block_;  // Current rio block being read
};

// Implementation of a packed reader.
class PackedReaderImpl : public RecordIOReader {
 public:
  explicit PackedReaderImpl(std::istream* in,
                            std::unique_ptr<RecordIOTransformer> transformer,
                            std::unique_ptr<Cleanup> cleanup)
      : r_(new BaseReader(in, MagicPacked, std::move(cleanup))),
        transformer_(std::move(transformer)),
        cur_item_(0) {}

  bool Scan() {
    ++cur_item_;
    while (cur_item_ >= items_.size()) {
      if (!ReadBlock()) return false;
    }
    return true;
  }

  std::vector<char>* Mutable() {
    const Span span = Get();
    tmp_.resize(span.size);
    std::copy(span.data, span.data + span.size, tmp_.begin());
    return &tmp_;
  }

  Span Get() {
    const Item item = items_[cur_item_];
    return Span{items_start_ + item.offset, static_cast<size_t>(item.size)};
  }

  std::string Error() { return r_->Error(); }

 private:
  // Read and parse the next block from the underlying (unpacked) reader.
  bool ReadBlock() {
    cur_item_ = 0;
    items_.clear();
    if (!r_->Scan()) return false;

    block_ = std::move(*r_->Mutable());
    BinaryParser parser(block_.data(), block_.size());
    uint32_t expected_crc;
    if (!parser.ReadLEUint32(&expected_crc)) {
      r_->SetError("invalid block header (crc)");
      return false;
    }
    const char* crc_start = parser.Data();
    uint64_t n_items;
    if (!parser.ReadUVarint(&n_items)) {
      r_->SetError("invalid block header (n_items)");
      return false;
    }
    if (n_items <= 0 || n_items >= block_.size()) {
      r_->SetError("invalid block header (n_items)");
      return false;
    }
    for (uint32_t i = 0; i < n_items; i++) {
      uint64_t item_size;
      if (!parser.ReadUVarint(&item_size)) {
        r_->SetError("invalid block header(item_size)");
        return false;
      }
      Item item = {0, static_cast<int>(item_size)};
      if (i > 0) {
        item.offset = items_[i - 1].offset + items_[i - 1].size;
      }
      items_.push_back(item);
    }
    items_start_ = parser.Data();
    const uint32_t actual_crc = Crc32(crc_start, items_start_ - crc_start);
    if (actual_crc != expected_crc) {
      r_->SetError("wrong crc");
      return false;
    }
    const char* items_limit = nullptr;
    if (transformer_ != nullptr) {
      size_t off = items_start_ - block_.data();
      const std::string err = RunTransformer(transformer_.get(), &block_, off);
      if (!err.empty()) {
        r_->SetError(err);
        return false;
      }
      items_start_ = block_.data();
    }
    items_limit = block_.data() + block_.size();
    if (items_.back().offset + items_.back().size !=
        (items_limit - items_start_)) {
      r_->SetError("junk at the end of block");
      return false;
    }
    return true;
  }

  struct Item {
    int offset;  // byte offset from items_start_
    int size;    // byte size of the item
  };
  std::unique_ptr<BaseReader> r_;  // Underlying unpacked reader.
  const std::unique_ptr<RecordIOTransformer> transformer_;
  std::vector<char> block_;  // Current rio block being read
  std::vector<Item> items_;  // Result of parsing the block_ metadata
  const char* items_start_;  // Start of the payload part in block_.
  size_t cur_item_;          // Indexes into items_.
  std::vector<char> tmp_;    // For implementing Mutable().
};

class UncompressTransformer : public RecordIOTransformer {
  RecordIOReader::Span Transform(RecordIOReader::Span in, std::string* err) {
    err->clear();
    z_stream stream;
    memset(&stream, 0, sizeof stream);
    int ret = inflateInit2(&stream, -15 /*RPC1951*/);
    if (ret != Z_OK) {
      std::ostringstream msg;
      msg << "inflateInit failed(" << ret << ")";
      *err = msg.str();
      return RecordIOReader::Span{nullptr, 0};
    }
    if (tmp_.capacity() >= static_cast<size_t>(in.size) * 2) {
      tmp_.resize(tmp_.capacity());
    } else {
      tmp_.resize(in.size * 2);
    }
    stream.avail_in = in.size;
    stream.next_in =
        const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data));
    stream.avail_out = tmp_.size();
    stream.next_out = reinterpret_cast<Bytef*>(tmp_.data());
    for (;;) {
      ret = inflate(&stream, Z_NO_FLUSH);
      if (ret != Z_OK && ret != Z_STREAM_END) {
        std::ostringstream msg;
        msg << "inflate failed(" << ret << ")";
        *err = msg.str();
        inflateEnd(&stream);
        return RecordIOReader::Span{nullptr, 0};
      }
      if (stream.avail_out > 0 || ret == Z_STREAM_END) {
        inflateEnd(&stream);
        return RecordIOReader::Span{tmp_.data(),
                                    tmp_.size() - stream.avail_out};
      }
      size_t cur_size = tmp_.size();
      tmp_.resize(cur_size * 2);
      stream.avail_out = tmp_.size() - cur_size;
      stream.next_out = reinterpret_cast<Bytef*>(tmp_.data() + cur_size);
    }
  }
  std::vector<char> tmp_;
};
}  // namespace
}  // namespace grail

grail::RecordIOReaderOpts grail::DefaultRecordIOReaderOpts(
    const std::string& path) {
  RecordIOReaderOpts r;
  switch (DetermineFileType(path)) {
    case FileType::GrailRIO:
      break;
    case FileType::GrailRIOPacked:
      r.packed = true;
      break;
    case FileType::GrailRIOPackedCompressed:
      r.packed = true;
      r.transformer = UncompressRecordIOTransformer();
      break;
    default:
      // Punt. The reader will cause an error.
      break;
  }
  return r;
}

std::unique_ptr<grail::RecordIOReader> grail::NewRecordIOReader(
    std::istream* in, RecordIOReaderOpts opts) {
  if (opts.packed) {
    return std::unique_ptr<RecordIOReader>(
        new PackedReaderImpl(in, std::move(opts.transformer), nullptr));
  } else {
    return std::unique_ptr<RecordIOReader>(
        new UnpackedReaderImpl(in, std::move(opts.transformer), nullptr));
  }
}

std::unique_ptr<grail::RecordIOReader> grail::NewRecordIOReader(
    const std::string& path) {
  class Closer : public Cleanup {
   public:
    std::ifstream in;
  };
  auto opts = DefaultRecordIOReaderOpts(path);

  std::unique_ptr<Closer> c(new Closer);
  c->in.open(path.c_str());
  if (opts.packed) {
    return std::unique_ptr<RecordIOReader>(new PackedReaderImpl(
        &c->in, std::move(opts.transformer), std::move(c)));
  } else {
    return std::unique_ptr<RecordIOReader>(new UnpackedReaderImpl(
        &c->in, std::move(opts.transformer), std::move(c)));
  }
}

std::unique_ptr<grail::RecordIOTransformer>
grail::UncompressRecordIOTransformer() {
  return std::unique_ptr<grail::RecordIOTransformer>(
      new UncompressTransformer());
}
