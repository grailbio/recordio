#include <zlib.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <sstream>
#include <vector>

#include "./portable_endian.h"
#include "lib/file/names.h"
#include "lib/recordio/internal.h"
#include "lib/recordio/recordio.h"

namespace grail {
namespace recordio {
namespace {

constexpr int SizeOffset = sizeof(internal::Magic);
constexpr int CrcOffset = SizeOffset + 8;
constexpr int DataOffset = CrcOffset + 4;
// HeaderSize is the size in bytes of the recordio header.
constexpr int HeaderSize = DataOffset;

// MaxReadRecordSize defines a max size for a record when reading to avoid
// crashes for unreasonable requests.
constexpr uint64_t MaxReadRecordSize = 1ULL << 29;

internal::Error RunTransformer(Transformer* t, std::vector<uint8_t>* buf,
                               int buf_off) {
  ByteSpan span{buf->data() + buf_off, buf->size() - buf_off};
  IoVec iov(&span, 1);
  internal::Error err;

  IoVec out = t->Transform(iov, &err);
  if (!err.empty()) return err;
  buf->resize(IoVecSize(out));
  size_t n = 0;
  for (size_t i = 0; i < out.size(); i++) {
    std::copy(out[i].begin(), out[i].end(), buf->data() + n);
    n += out[i].size();
  }
  return "";
}

// BaseReader implements a raw reader w/o any transformation.
class BaseReader {
 public:
  explicit BaseReader(std::istream* in, internal::Magic magic,
                      std::unique_ptr<internal::Cleanup> cleanup,
                      internal::ErrorReporter* err)
      : in_(in), magic_(magic), cleanup_(std::move(cleanup)), err_(err) {}

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
      err_->Set(msg.str());
      return false;
    }
    return true;
  }

  std::vector<uint8_t>* Mutable() { return &buf_; }

 private:
  // Read the header part of the block from in_. On success, set *size to the
  // length of the rest of the block.
  bool ReadHeader(uint64_t* size) {
    uint8_t header[HeaderSize];
    int n = ReadBytes(header, sizeof(header));
    if (n <= 0) {
      if (!in_->eof()) {
        std::ostringstream msg;
        msg << "Failed to read file: " << strerror(errno);
        err_->Set(msg.str());
      }
      return false;  // EOF
    }
    if (n != sizeof header) {
      std::ostringstream msg;
      msg << "Corrupt header; read " << n << " bytes, expect " << sizeof header
          << " bytes";
      err_->Set(msg.str());
      return false;
    }
    internal::Magic magic;
    memcpy(magic.data(), header, magic.size());
    if (magic != magic_) {
      std::ostringstream msg;
      msg << "Wrong header magic: " << internal::MagicDebugString(magic)
          << ", expect " << internal::MagicDebugString(magic_);
      err_->Set(msg.str());
      return false;
    }

    internal::BinaryParser parser(header + SizeOffset,
                                  sizeof(header) - SizeOffset, err_);
    *size = parser.ReadLEUint64();
    const uint32_t expected_crc = parser.ReadLEUint32();
    if (!err_->Ok()) return false;
    auto actual_crc =
        internal::Crc32(header + SizeOffset, CrcOffset - SizeOffset);
    if (actual_crc != expected_crc) {
      std::ostringstream msg;
      msg << "corrupt header crc, expect " << expected_crc << " found "
          << actual_crc;
      err_->Set(msg.str());
      return false;
    }
    if (*size > MaxReadRecordSize) {
      std::ostringstream msg;
      msg << "unreasonably large read record encountered: " << *size << " > "
          << MaxReadRecordSize << " bytes";
      err_->Set(msg.str());
      return false;
    }
    return true;
  }

  // Read "bytes" byte from in_.
  int ReadBytes(uint8_t* data, int bytes) {
    int remaining = bytes;
    while (remaining > 0) {
      in_->read(reinterpret_cast<char*>(data), remaining);
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
  const internal::Magic magic_;
  const std::unique_ptr<internal::Cleanup> cleanup_;
  internal::ErrorReporter* const err_;
  std::vector<uint8_t> buf_;
};

// Implementation of an unpacked reader.
class UnpackedReaderImpl : public Reader {
 public:
  explicit UnpackedReaderImpl(std::istream* in,
                              std::unique_ptr<Transformer> transformer,
                              std::unique_ptr<internal::Cleanup> cleanup)
      : r_(in, internal::MagicUnpacked, std::move(cleanup), &err_),
        transformer_(std::move(transformer)) {}

  std::vector<HeaderEntry> Header() override {
    return std::vector<HeaderEntry>();
  }

  bool Scan() override {
    if (!r_.Scan()) return false;
    block_ = std::move(*r_.Mutable());
    if (transformer_ != nullptr) {
      const std::string err = RunTransformer(transformer_.get(), &block_, 0);
      if (!err.empty()) {
        err_.Set(err);
        return false;
      }
    }
    return true;
  }

  std::vector<uint8_t>* Mutable() override { return &block_; }
  ByteSpan Get() override { return ByteSpan{block_.data(), block_.size()}; }
  void Seek(ItemLocation loc) override { err_.Set("Seek not supported"); }
  std::string Error() override { return err_.Err(); }
  ByteSpan Trailer() override { return ByteSpan{nullptr, 0}; }

 private:
  internal::ErrorReporter err_;
  BaseReader r_;  // Underlying unpacked reader.
  const std::unique_ptr<Transformer> transformer_;
  std::vector<uint8_t> block_;  // Current rio block being read
};

// Implementation of a packed reader.
class PackedReaderImpl : public Reader {
 public:
  explicit PackedReaderImpl(std::istream* in,
                            std::unique_ptr<Transformer> transformer,
                            std::unique_ptr<internal::Cleanup> cleanup)
      : r_(in, internal::MagicPacked, std::move(cleanup), &err_),
        transformer_(std::move(transformer)),
        cur_item_(0) {}

  bool Scan() override {
    ++cur_item_;
    while (cur_item_ >= items_.size()) {
      if (!ReadBlock()) return false;
    }
    return true;
  }

  std::vector<uint8_t>* Mutable() override {
    const ByteSpan span = Get();
    tmp_.resize(span.size());
    std::copy(span.begin(), span.end(), tmp_.begin());
    return &tmp_;
  }

  ByteSpan Get() override {
    const Item item = items_[cur_item_];
    return ByteSpan{items_start_ + item.offset, static_cast<size_t>(item.size)};
  }

  void Seek(ItemLocation loc) override { err_.Set("Seek not supported"); }
  std::string Error() override { return err_.Err(); }
  std::vector<HeaderEntry> Header() override {
    return std::vector<HeaderEntry>();
  }
  ByteSpan Trailer() override { return ByteSpan{nullptr, 0}; }

 private:
  // Read and parse the next block from the underlying (unpacked) reader.
  bool ReadBlock() {
    cur_item_ = 0;
    items_.clear();
    if (!r_.Scan()) return false;

    block_ = std::move(*r_.Mutable());
    internal::BinaryParser parser(block_.data(), block_.size(), &err_);
    uint32_t expected_crc = parser.ReadLEUint32();
    if (!err_.Ok()) return false;
    const uint8_t* crc_start = parser.Data();
    uint64_t n_items = parser.ReadUVarint();
    if (n_items <= 0 || n_items >= block_.size()) {
      err_.Set("invalid block header (n_items)");
      return false;
    }
    for (uint32_t i = 0; i < n_items; i++) {
      uint64_t item_size = parser.ReadUVarint();
      Item item = {0, static_cast<int>(item_size)};
      if (i > 0) {
        item.offset = items_[i - 1].offset + items_[i - 1].size;
      }
      items_.push_back(item);
    }
    items_start_ = parser.Data();
    const uint32_t actual_crc =
        internal::Crc32(crc_start, items_start_ - crc_start);
    if (actual_crc != expected_crc) {
      err_.Set("wrong crc");
      return false;
    }
    const uint8_t* items_limit = nullptr;
    if (transformer_ != nullptr) {
      size_t off = items_start_ - block_.data();
      const std::string err = RunTransformer(transformer_.get(), &block_, off);
      if (!err.empty()) {
        err_.Set(err);
        return false;
      }
      items_start_ = block_.data();
    }
    items_limit = block_.data() + block_.size();
    if (items_.back().offset + items_.back().size !=
        (items_limit - items_start_)) {
      err_.Set("junk at the end of block");
      return false;
    }
    return err_.Ok();
  }

  struct Item {
    int offset;  // byte offset from items_start_
    int size;    // byte size of the item
  };
  internal::ErrorReporter err_;
  BaseReader r_;  // Underlying unpacked reader.
  const std::unique_ptr<Transformer> transformer_;
  std::vector<uint8_t> block_;  // Current rio block being read
  std::vector<Item> items_;     // Result of parsing the block_ metadata
  const uint8_t* items_start_;  // Start of the payload part in block_.
  size_t cur_item_;             // Indexes into items_.
  std::vector<uint8_t> tmp_;    // For implementing Mutable().
};

class UncompressTransformerImpl : public Transformer {
  IoVec Transform(IoVec in_iov, std::string* err) {
    err->clear();
    z_stream stream;
    memset(&stream, 0, sizeof stream);
    int ret = inflateInit2(&stream, -15 /*RFC1951*/);
    if (ret != Z_OK) {
      std::ostringstream msg;
      msg << "inflateInit failed(" << ret << ")";
      *err = msg.str();
      return IoVec();
    }
    const size_t in_bytes = IoVecSize(in_iov);
    if (tmp_.capacity() >= in_bytes * 2) {
      tmp_.resize(tmp_.capacity());
    } else {
      tmp_.resize(in_bytes);
    }
    stream.avail_out = tmp_.size();
    stream.next_out = tmp_.data();
    ret = Z_STREAM_END;
    size_t iov_idx = 0;
    for (iov_idx = 0; iov_idx < in_iov.size(); ++iov_idx) {
      stream.avail_in = in_iov[iov_idx].size();
      stream.next_in = const_cast<Bytef*>(in_iov[iov_idx].data());
      for (;;) {
        ret = inflate(&stream, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
          std::ostringstream msg;
          msg << "inflate failed(" << ret << ")";
          *err = msg.str();
          inflateEnd(&stream);
          return IoVec();
        }
        if (ret == Z_STREAM_END || stream.avail_in == 0) {
          break;
        }
        if (stream.avail_out == 0) {
          size_t cur_size = tmp_.size();
          tmp_.resize(cur_size * 2);
          stream.avail_out = tmp_.size() - cur_size;
          stream.next_out = tmp_.data() + cur_size;
          continue;
        }
        abort();  // shouldn't happen.
      }
      if (ret == Z_STREAM_END) {
        iov_idx++;
        break;
      }
    }
    if (stream.avail_in != 0 || iov_idx != in_iov.size()) {
      std::ostringstream msg;
      msg << "found trailing junk during inflate";
      *err = msg.str();
      return IoVec();
    }
    inflateEnd(&stream);

    tmp_span_ = ByteSpan(tmp_.data(), tmp_.size() - stream.avail_out);
    return IoVec(&tmp_span_, 1);
  }

  ByteSpan tmp_span_;
  std::vector<uint8_t> tmp_;
};
}  // namespace

namespace internal {
std::unique_ptr<Reader> NewLegacyPackedReader(
    std::istream* in, std::unique_ptr<Transformer> transformer,
    std::unique_ptr<internal::Cleanup> cleanup) {
  return std::unique_ptr<Reader>(
      new PackedReaderImpl(in, std::move(transformer), std::move(cleanup)));
}

std::unique_ptr<Reader> NewLegacyUnpackedReader(
    std::istream* in, std::unique_ptr<Transformer> transformer,
    std::unique_ptr<internal::Cleanup> cleanup) {
  return std::unique_ptr<Reader>(
      new UnpackedReaderImpl(in, std::move(transformer), std::move(cleanup)));
}
}  // namespace internal

std::unique_ptr<Transformer> UncompressTransformer() {
  return std::unique_ptr<Transformer>(new UncompressTransformerImpl());
}

}  // namespace recordio
}  // namespace grail
