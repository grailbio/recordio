#include <zlib.h>

#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>

#include <iostream>

#include "./portable_endian.h"
#include "lib/file/names.h"
#include "lib/recordio/internal.h"
#include "lib/recordio/recordio.h"

namespace grail {
namespace recordio {

Writer::~Writer() {}
WriterIndexer::~WriterIndexer() {}

namespace {

class FileCloser : public internal::Cleanup {
 public:
  bool Close() {
    out.close();
    return out.good();
  }
  std::ofstream out;
};

// class BaseWriter implements a raw writer w/o any transformation.
class BaseWriter {
 public:
  explicit BaseWriter(std::ostream* out, internal::Magic magic,
                      std::unique_ptr<FileCloser> cleanup,
                      std::unique_ptr<WriterIndexer> indexer)
      : out_(out),
        initial_pos_(out->tellp()),
        magic_(magic),
        cleanup_(std::move(cleanup)),
        indexer_(std::move(indexer)) {}

  // Write accepts two spans and writes them both, contiguously, into a single
  // record. It accepts two just to avoid an extra data copy in the packed
  // writer.
  bool Write(ByteSpan one, ByteSpan two) {
    uint64_t block_start = static_cast<uint64_t>(out_->tellp() - initial_pos_);

    if (!WriteHeader(one.size() + two.size())) return false;

    out_->write(reinterpret_cast<const char*>(one.data()), one.size());
    if (!out_->good()) {
      SetError(std::string("Failed to write data part 1: ") +
               std::strerror(errno));
      return false;
    }

    if (two.size() > 0) {
      out_->write(reinterpret_cast<const char*>(two.data()), two.size());
      if (!out_->good()) {
        SetError(std::string("Failed to write data part 2: ") +
                 std::strerror(errno));
        return false;
      }
    }

    if (indexer_ != nullptr) {
      std::string error = indexer_->IndexBlock(block_start);
      if (!error.empty()) {
        SetError(std::string("Indexer error: ") + error);
        return false;
      }
    }

    return true;
  }

  bool Close() {
    if (cleanup_ != nullptr && !cleanup_->Close()) {
      SetError(std::string("Failed to close output file: ") +
               std::strerror(errno));
      return false;
    }
    return true;
  }

  std::string Error() { return err_; }

  void SetError(const std::string& err) {
    if (err_.empty()) {
      err_ = err;
    }
  }

 private:
  bool WriteLEUint64(uint64_t v) {
    uint64_t le = htole64(v);
    out_->write(reinterpret_cast<const char*>(&le), sizeof le);
    return out_->good();
  }

  bool WriteLEUint32(uint32_t v) {
    uint32_t le = htole64(v);
    out_->write(reinterpret_cast<const char*>(&le), sizeof le);
    return out_->good();
  }

  // Write the header for this block to out_, where size is the number of bytes
  // of data that will be written in the block.
  bool WriteHeader(uint64_t size) {
    out_->write(reinterpret_cast<const char*>(magic_.data()), magic_.size());
    if (!out_->good()) {
      SetError("Failed to write header magic");
      return false;
    }

    WriteLEUint64(size);
    if (!out_->good()) {
      SetError("Failed to write header size");
      return false;
    }

    uint64_t size_le = htole64(size);
    uint32_t size_crc32 =
        internal::Crc32(reinterpret_cast<uint8_t*>(&size_le), sizeof size_le);
    WriteLEUint32(size_crc32);
    if (!out_->good()) {
      SetError("Failed to write header size checksum");
      return false;
    }

    return true;
  }

  std::ostream* const out_;
  const std::streampos initial_pos_;
  const internal::Magic magic_;
  const std::unique_ptr<FileCloser> cleanup_;
  std::string err_;
  const std::unique_ptr<WriterIndexer> indexer_;
};

// Implementation of an unpacked writer.
class UnpackedWriterImpl : public Writer {
 public:
  explicit UnpackedWriterImpl(std::ostream* out,
                              std::unique_ptr<Transformer> transformer,
                              std::unique_ptr<WriterIndexer> indexer,
                              std::unique_ptr<FileCloser> cleanup)
      : r_(out, internal::MagicUnpacked, std::move(cleanup),
           std::move(indexer)),
        transformer_(std::move(transformer)) {}

  bool Write(ByteSpan in) {
    if (transformer_ != nullptr) {
      std::string err;
      IoVec iov(&in, 1);
      IoVec out = transformer_->Transform(iov, &err);
      if (!err.empty()) {
        r_.SetError(err);
        return false;
      }
      if (out.size() != 1) {
        std::cerr << "TODO: Transformer returned multiple iovec\n";
        abort();
      }
      in = out[0];
    }
    return r_.Write(in, ByteSpan{nullptr, 0});
  }

  bool Close() { return r_.Close(); }

  std::string Error() { return r_.Error(); }

 private:
  BaseWriter r_;  // Underlying unpacked writer.
  const std::unique_ptr<Transformer> transformer_;
};

class PackedHeaderBuilder {
 public:
  PackedHeaderBuilder() : items_count_(0) {}

  bool AddItemSize(uint32_t size) {
    if (items_count_ == std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    items_count_++;
    AppendUVarint(&sizes_, size);
    return true;
  }

  int64_t items_count() { return items_count_; }

  void AppendHeader(std::vector<uint8_t>* buf) {
    // We need to compute the CRC32 of items count and all of the item sizes.
    // To avoid allocating a temporary buffer, we copy them into the output
    // buffer first, compute the checksum, then write it into place.
    auto checksum_offset = buf->size();
    buf->insert(buf->end(), sizeof(uint32_t), static_cast<uint8_t>(0));

    auto varints_offset = buf->size();
    AppendUVarint(buf, items_count_);
    buf->insert(buf->end(), sizes_.cbegin(), sizes_.cend());
    uint32_t checksum = internal::Crc32(buf->data() + varints_offset,
                                        buf->size() - varints_offset);
    WriteLEUint32At(buf, checksum_offset, checksum);
  }

  void Clear() {
    items_count_ = 0;
    sizes_.clear();
  }

 private:
  void AppendUVarint(std::vector<uint8_t>* buf, uint64_t v) {
    while (v >= 0x80) {
      buf->push_back(static_cast<uint8_t>(v) | 0x80);
      v >>= 7;
    }
    buf->push_back(static_cast<uint8_t>(v));
  }

  void WriteLEUint32At(std::vector<uint8_t>* buf, size_t offset, uint32_t v) {
    uint32_t le = htole64(v);
    std::copy(reinterpret_cast<const uint8_t*>(&le),
              reinterpret_cast<const uint8_t*>(&le) + sizeof le,
              buf->begin() + offset);
  }

  int64_t items_count_;
  std::vector<uint8_t> sizes_;
};

class PackedWriterImpl : public Writer {
 public:
  explicit PackedWriterImpl(std::ostream* out,
                            std::unique_ptr<Transformer> transformer,
                            std::unique_ptr<WriterIndexer> indexer,
                            std::unique_ptr<FileCloser> cleanup,
                            const uint32_t max_packed_items,
                            const uint32_t max_packed_bytes)
      : r_(out, internal::MagicPacked, std::move(cleanup), std::move(indexer)),
        transformer_(std::move(transformer)),
        max_packed_items_(max_packed_items),
        max_packed_bytes_(max_packed_bytes) {}

  bool Write(ByteSpan item) {
    if (static_cast<int>(item.size()) > max_packed_bytes_) {
      r_.SetError("Item size exceeds block size");
      return false;
    }

    if ((header_builder_.items_count() + 1) > max_packed_items_ ||
        static_cast<int>(buffered_items_.size() + item.size()) >
            max_packed_bytes_) {
      if (!Flush()) {
        return false;
      }
    }

    if (!header_builder_.AddItemSize(item.size())) {
      r_.SetError("Could not add new item");
      return false;
    }
    buffered_items_.insert(buffered_items_.end(), item.begin(), item.end());

    return true;
  }

  bool Close() {
    if (!Flush()) {
      return false;
    }
    return r_.Close();
  }

  std::string Error() { return r_.Error(); }

 private:
  bool Flush() {
    std::vector<uint8_t> header;
    header_builder_.AppendHeader(&header);

    ByteSpan transformed = {buffered_items_.data(), buffered_items_.size()};
    if (transformer_ != nullptr) {
      std::string err;
      IoVec iov(&transformed, 1);
      IoVec out = transformer_->Transform(iov, &err);
      if (!err.empty()) {
        r_.SetError(err);
        return false;
      }
      if (out.size() != 1) {
        std::cerr << "TODO: Transformer returned multiple iovec\n";
        abort();
      }
      transformed = out[0];
    }
    if (!r_.Write(ByteSpan{header.data(), header.size()}, transformed)) {
      return false;
    }
    header_builder_.Clear();
    buffered_items_.clear();
    return true;
  }

  BaseWriter r_;  // Underlying unpacked writer.
  const std::unique_ptr<Transformer> transformer_;

  const int64_t max_packed_items_;
  const int64_t max_packed_bytes_;

  PackedHeaderBuilder header_builder_;
  std::vector<uint8_t> buffered_items_;
};

}  // namespace

class CompressTransformerImpl : public Transformer {
  IoVec Transform(IoVec in_iov, std::string* err) {
    err->clear();
    z_stream stream;
    memset(&stream, 0, sizeof stream);
    int ret = deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                           -15 /*RFC1951*/, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) {
      std::ostringstream msg;
      msg << "deflateInit failed(" << ret << ")";
      *err = msg.str();
      return IoVec();
    }

    const size_t in_bytes = IoVecSize(in_iov);
    tmp_.resize(deflateBound(&stream, in_bytes));
    stream.avail_out = tmp_.size();
    stream.next_out = reinterpret_cast<Bytef*>(tmp_.data());
    ret = Z_STREAM_END;
    size_t iov_idx = 0;
    for (iov_idx = 0; iov_idx < in_iov.size(); ++iov_idx) {
      stream.avail_in = in_iov[iov_idx].size();
      stream.next_in = const_cast<Bytef*>(
          reinterpret_cast<const Bytef*>(in_iov[iov_idx].data()));
      int flag = (iov_idx == in_iov.size() - 1) ? Z_FINISH : Z_NO_FLUSH;
      ret = deflate(&stream, flag);
      if (ret != Z_OK && ret != Z_STREAM_END) {
        std::ostringstream msg;
        msg << "deflate failed(" << ret << ")";
        *err = msg.str();
        deflateEnd(&stream);
        return IoVec();
      }
      if (stream.avail_in != 0) {
        abort();
      }
    }
    if (stream.avail_in != 0 || iov_idx != in_iov.size()) {
      std::ostringstream msg;
      msg << "found trailing junk during deflate";
      *err = msg.str();
      return IoVec();
    }
    deflateEnd(&stream);
    tmp_.resize(tmp_.size() - stream.avail_out);
    tmp_span_ = ByteSpan(&tmp_);
    return IoVec(&tmp_span_, 1);
  }
  ByteSpan tmp_span_;
  std::vector<uint8_t> tmp_;
};

WriterOpts DefaultWriterOpts(const std::string& path) {
  WriterOpts r;
  switch (DetermineFileType(path)) {
    case FileType::GrailRIO:
      break;
    case FileType::GrailRIOPacked:
      r.packed = true;
      break;
    case FileType::GrailRIOPackedCompressed:
      r.packed = true;
      r.transformer = CompressTransformer();
      break;
    default:
      // Punt. The writer will cause an error.
      break;
  }
  return r;
}

std::unique_ptr<Writer> NewWriter(std::ostream* out, WriterOpts opts) {
  if (opts.packed) {
    return std::unique_ptr<Writer>(new PackedWriterImpl(
        out, std::move(opts.transformer), std::move(opts.indexer), nullptr,
        opts.max_packed_items, opts.max_packed_bytes));
  } else {
    return std::unique_ptr<Writer>(new UnpackedWriterImpl(
        out, std::move(opts.transformer), std::move(opts.indexer), nullptr));
  }
}

std::unique_ptr<Writer> NewWriter(const std::string& path) {
  auto opts = DefaultWriterOpts(path);

  std::unique_ptr<FileCloser> c(new FileCloser);
  c->out.open(path.c_str());
  if (opts.packed) {
    return std::unique_ptr<Writer>(new PackedWriterImpl(
        &c->out, std::move(opts.transformer), std::move(opts.indexer),
        std::move(c), opts.max_packed_items, opts.max_packed_bytes));
  } else {
    return std::unique_ptr<Writer>(
        new UnpackedWriterImpl(&c->out, std::move(opts.transformer),
                               std::move(opts.indexer), std::move(c)));
  }
}

std::unique_ptr<Transformer> CompressTransformer() {
  return std::unique_ptr<Transformer>(new CompressTransformerImpl());
}

}  // namespace recordio
}  // namespace grail
