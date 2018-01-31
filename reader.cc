#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <vector>

#include "./portable_endian.h"
#include "lib/file/names.h"
#include "lib/recordio/chunk.h"
#include "lib/recordio/header.h"
#include "lib/recordio/recordio.h"

namespace grail {
namespace recordio {

Reader::~Reader() {}
Transformer::~Transformer() {}

ReaderOpts DefaultReaderOpts(const std::string& path) {
  ReaderOpts r;
  switch (DetermineFileType(path)) {
    case FileType::GrailRIOPackedCompressed:
      r.transformer = UncompressTransformer();
      break;
    default:
      // Punt. The reader will cause an error.
      break;
  }
  return r;
}

namespace internal {
std::unique_ptr<Reader> NewLegacyPackedReader(
    std::istream* in, std::unique_ptr<Transformer> transformer,
    std::unique_ptr<Cleanup> cleanup);
std::unique_ptr<Reader> NewLegacyUnpackedReader(
    std::istream* in, std::unique_ptr<Transformer> transformer,
    std::unique_ptr<Cleanup> cleanup);
namespace {

class ErrorReaderImpl : public Reader {
 public:
  explicit ErrorReaderImpl(std::string err) : err_(std::move(err)) {}
  bool Scan() { return false; }
  std::vector<uint8_t>* Mutable() { return nullptr; }
  ByteSpan Get() { return ByteSpan{nullptr, 0}; }
  std::string Error() { return err_; }
  std::vector<HeaderEntry> Header() { return std::vector<HeaderEntry>(); }
  ByteSpan Trailer() { return ByteSpan{nullptr, 0}; }

 private:
  std::string err_;
};

int ParseChunksToItems(const IoVec& iov, std::vector<std::vector<uint8_t>>* buf,
                       ErrorReporter* err) {
  ByteSpan data;
  std::vector<uint8_t> tmp;
  if (iov.size() == 0) {
    return 0;
  }
  data = iov[0];
  if (iov.size() > 1) {
    // TODO(saito) remove memcpy.
    tmp = IoVecFlatten(iov);
    data = ByteSpan(&tmp);
  }
  BinaryParser p(data.data(), data.size(), err);
  uint64_t n = p.ReadUVarint();
  std::vector<uint64_t> item_sizes;
  for (size_t i = 0; i < n; i++) {
    uint64_t size = p.ReadUVarint();
    item_sizes.push_back(size);
  }
  for (size_t i = 0; i < n; i++) {
    // TODO(saito) remove memcpy.
    const uint64_t item_size = item_sizes[i];
    const uint8_t* data = p.ReadBytes(item_size);
    if (data == nullptr) return 0;
    if (buf->size() <= i) {
      buf->resize((i + 1) * 2);
    }
    (*buf)[i].resize(item_size);
    memcpy((*buf)[i].data(), data, item_size);
  }
  return n;
}

class ReaderImpl : public Reader {
 public:
  ReaderImpl(std::istream* in, ReaderOpts opts, std::unique_ptr<Cleanup> closer)
      : cr_(new ChunkReader(in, &err_)), closer_(std::move(closer)) {
    readHeader();
    int64_t cur_off;
    err_.Set(Tell(in, &cur_off));
    bool has_trailer;
    err_.Set(HasTrailer(header_, &has_trailer));
    if (!err_.Ok()) return;

    if (has_trailer) {
      readTrailer();
    }
    cr_->Seek(cur_off);
    n_items_ = 0;
    next_item_ = 0;
  }

  bool Scan() {
    while (next_item_ >= n_items_) {
      next_item_ = 0;
      if (!ReadBlock()) {
        return false;
      }
    }
    item_ = &itembuf_[next_item_];
    next_item_++;
    return true;
  }
  // TODO(saito) this is unsafe. Change Mutable to return a vector<uint8_t>.
  std::vector<uint8_t>* Mutable() { return item_; }
  ByteSpan Get() { return ByteSpan{item_->data(), item_->size()}; }
  std::string Error() { return err_.Err(); }
  std::vector<HeaderEntry> Header() { return header_; }
  ByteSpan Trailer() { return ByteSpan{nullptr, 0}; }

 private:
  void readHeader() {
    std::vector<uint8_t>* payload;
    if (!ReadSpecialBlock(MagicHeader, &payload)) {
      return;
    }
    header_ = DecodeHeader(payload->data(), payload->size(), &err_);
  }

  void readTrailer() {
    std::vector<uint8_t>* payload;
    cr_->SeekLastBlock();
    if (!ReadSpecialBlock(MagicTrailer, &payload)) {
      return;
    }
    abort();
  }

  bool ReadBlock() {
    if (!err_.Ok()) return false;
    if (!cr_->Scan()) return false;
    const Magic magic = cr_->GetMagic();
    if (magic == MagicPacked) {
      n_items_ = ParseChunksToItems(cr_->Chunks(), &itembuf_, &err_);
      if (!err_.Ok()) return false;
      next_item_ = 0;
      return true;
    }
    if (magic == MagicTrailer) {  // EOF
      return false;
    }
    std::ostringstream msg;
    msg << "Bad magic: " << MagicDebugString(magic);
    err_.Set(msg.str());
    return false;
  }

  bool ReadSpecialBlock(const Magic expected_magic,
                        std::vector<uint8_t>** payload) {
    if (!cr_->Scan()) {
      err_.Set("Failed to read trailer block");
      return false;
    }
    const Magic magic = cr_->GetMagic();
    if (magic != expected_magic) {
      std::ostringstream msg;
      msg << "Failed to read header block, got " << MagicDebugString(magic);
      err_.Set(msg.str());
      return false;
    }
    n_items_ = ParseChunksToItems(cr_->Chunks(), &itembuf_, &err_);
    if (!err_.Ok()) return false;
    if (n_items_ != 1) {
      err_.Set("Wrong # of items in header block");
      return false;
    }
    *payload = &itembuf_[0];
    return true;
  }

 private:
  ErrorReporter err_;
  std::unique_ptr<ChunkReader> cr_;
  std::unique_ptr<Cleanup> closer_;
  int next_item_ = 0;
  std::vector<uint8_t>* item_ = nullptr;

  std::vector<std::vector<uint8_t>> itembuf_;
  int n_items_ = -1;
  std::vector<HeaderEntry> header_;
};

std::unique_ptr<Reader> NewReader(std::istream* in, ReaderOpts opts,
                                  std::unique_ptr<Cleanup> closer) {
  int64_t cur_off;
  internal::Error err = Tell(in, &cur_off);
  if (err != "") {
    return std::unique_ptr<Reader>(new ErrorReaderImpl(err));
  }
  Magic magic;
  err = ReadFull(in, magic.data(), magic.size());
  if (err != "") {
    return std::unique_ptr<Reader>(new ErrorReaderImpl(err));
  }
  err = AbsSeek(in, cur_off);
  if (err != "") {
    return std::unique_ptr<Reader>(new ErrorReaderImpl(err));
  }
  if (magic == MagicPacked) {
    return NewLegacyPackedReader(in, std::move(opts.transformer),
                                 std::move(closer));
  }
  if (magic == MagicUnpacked) {
    return internal::NewLegacyUnpackedReader(in, std::move(opts.transformer),
                                             std::move(closer));
  }
  return std::unique_ptr<Reader>(
      new ReaderImpl(in, std::move(opts), std::move(closer)));
}
}  // namespace
}  // namespace internal

std::unique_ptr<Reader> NewReader(std::istream* in, ReaderOpts opts) {
  return internal::NewReader(in, std::move(opts), nullptr);
}

std::unique_ptr<Reader> NewReader(const std::string& path) {
  class Closer : public internal::Cleanup {
   public:
    std::ifstream in;
  };
  auto opts = DefaultReaderOpts(path);
  std::unique_ptr<Closer> c(new Closer);
  c->in.open(path.c_str());
  std::ifstream* in = &c->in;
  return internal::NewReader(in, std::move(opts), std::move(c));
}

}  // namespace recordio
}  // namespace grail
