#include <zlib.h>

#include <array>
#include <cstring>
#include <fstream>
#include <sstream>

#include "./portable_endian.h"
#include "lib/file/names.h"
#include "lib/recordio/recordio.h"
#include "lib/recordio/recordio_internal.h"

namespace grail {

RecordIOWriter::~RecordIOWriter() {}

namespace {

// class BaseWriter implements a raw writer w/o any transformation.
class BaseWriter {
 public:
  explicit BaseWriter(std::ostream* out, RecordIOMagic magic,
                      std::unique_ptr<RecordIOCleanup> cleanup)
      : out_(out), magic_(magic), cleanup_(std::move(cleanup)) {}

  bool Write(RecordIOSpan in) {
    if (!WriteHeader(in.size)) return false;

    out_->write(in.data, in.size);
    if (!out_->good()) {
      SetError(std::string("Failed to write data: ") + std::strerror(errno));
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
        RecordIOCrc32(reinterpret_cast<const char*>(&size_le), sizeof size_le);
    WriteLEUint32(size_crc32);
    if (!out_->good()) {
      SetError("Failed to write header size checksum");
      return false;
    }

    return true;
  }

  std::ostream* const out_;
  const RecordIOMagic magic_;
  const std::unique_ptr<RecordIOCleanup> cleanup_;
  std::string err_;
};

// Implementation of an unpacked writer.
class UnpackedWriterImpl : public RecordIOWriter {
 public:
  explicit UnpackedWriterImpl(std::ostream* out,
                              std::unique_ptr<RecordIOTransformer> transformer,
                              std::unique_ptr<RecordIOCleanup> cleanup)
      : r_(out, RecordIOMagicUnpacked, std::move(cleanup)),
        transformer_(std::move(transformer)) {}

  bool Write(RecordIOSpan in) {
    if (transformer_ != nullptr) {
      std::string err;
      in = transformer_->Transform(in, &err);
      if (!err.empty()) {
        r_.SetError(err);
        return false;
      }
    }
    return r_.Write(in);
  }

  std::string Error() { return r_.Error(); }

 private:
  BaseWriter r_;  // Underlying unpacked writer.
  const std::unique_ptr<RecordIOTransformer> transformer_;
};

class CompressTransformer : public RecordIOTransformer {
  RecordIOSpan Transform(RecordIOSpan in, std::string* err) {
    err->clear();
    z_stream stream;
    memset(&stream, 0, sizeof stream);
    int ret = deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                           -15 /*RFC1951*/, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) {
      std::ostringstream msg;
      msg << "deflateInit failed(" << ret << ")";
      *err = msg.str();
      return RecordIOSpan{nullptr, 0};
    }
    tmp_.resize(deflateBound(&stream, in.size));
    stream.avail_in = in.size;
    stream.next_in =
        const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data));
    stream.avail_out = tmp_.size();
    stream.next_out = reinterpret_cast<Bytef*>(tmp_.data());
    ret = deflate(&stream, Z_FINISH);
    if (ret != Z_STREAM_END) {
      std::ostringstream msg;
      msg << "deflate failed(" << ret << ")";
      *err = msg.str();
      deflateEnd(&stream);
      return RecordIOSpan{nullptr, 0};
    }
    deflateEnd(&stream);
    return RecordIOSpan{tmp_.data(), tmp_.size() - stream.avail_out};
  }
  std::vector<char> tmp_;
};

}  // namespace

RecordIOWriterOpts DefaultRecordIOWriterOpts(const std::string& path) {
  RecordIOWriterOpts r;
  switch (DetermineFileType(path)) {
    case FileType::GrailRIO:
      break;
    case FileType::GrailRIOPacked:
      r.packed = true;
      break;
    case FileType::GrailRIOPackedCompressed:
      r.packed = true;
      r.transformer = CompressRecordIOTransformer();
      break;
    default:
      // Punt. The writer will cause an error.
      break;
  }
  return r;
}

std::unique_ptr<RecordIOWriter> NewRecordIOWriter(std::ostream* out,
                                                  RecordIOWriterOpts opts) {
  if (opts.packed) {
    return nullptr;
  } else {
    return std::unique_ptr<RecordIOWriter>(
        new UnpackedWriterImpl(out, std::move(opts.transformer), nullptr));
  }
}

std::unique_ptr<RecordIOWriter> NewRecordIOWriter(const std::string& path) {
  class Closer : public RecordIOCleanup {
   public:
    std::ofstream out;
  };
  auto opts = DefaultRecordIOWriterOpts(path);

  std::unique_ptr<Closer> c(new Closer);
  c->out.open(path.c_str());
  if (opts.packed) {
    return nullptr;
  } else {
    return std::unique_ptr<RecordIOWriter>(new UnpackedWriterImpl(
        &c->out, std::move(opts.transformer), std::move(c)));
  }
}

std::unique_ptr<RecordIOTransformer> CompressRecordIOTransformer() {
  return std::unique_ptr<grail::RecordIOTransformer>(new CompressTransformer());
}

}  // namespace grail
