#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
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
      r.legacy_transformer = UnflateTransformer();
      break;
    default:
      // Punt. The reader will cause an error.
      break;
  }
  return r;
}

namespace internal {
std::unique_ptr<Reader> NewLegacyPackedReader(
    std::unique_ptr<ReadSeeker> in, std::unique_ptr<Transformer> transformer);
std::unique_ptr<Reader> NewLegacyUnpackedReader(
    std::unique_ptr<ReadSeeker> in, std::unique_ptr<Transformer> transformer);

namespace {
class ErrorReaderImpl : public Reader {
 public:
  explicit ErrorReaderImpl(std::string err) : err_(std::move(err)) {}
  bool Scan() override { return false; }
  void Seek(ItemLocation loc) override {}
  std::vector<uint8_t>* Mutable() override { return nullptr; }
  ByteSpan Get() override { return ByteSpan{nullptr, 0}; }
  Error GetError() override { return err_; }
  std::vector<HeaderEntry> Header() override {
    return std::vector<HeaderEntry>();
  }
  ByteSpan Trailer() override { return ByteSpan{nullptr, 0}; }

 private:
  Error err_;
};

int ParseChunksToItems(const IoVec& raw_iov, Transformer* tr,
                       std::vector<std::vector<uint8_t>>* buf,
                       ErrorReporter* err) {
  IoVec iov = raw_iov;
  if (tr != nullptr) {
    err->Set(tr->Transform(raw_iov, &iov));
    if (!err->Ok()) return 0;
  }
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
}  // namespace

class ReaderImpl : public Reader {
 public:
  ReaderImpl(std::unique_ptr<ReadSeeker> in, ReaderOpts opts)
      : cr_(new ChunkReader(in.get(), &err_)), in_(std::move(in)) {
    readHeader();
    int64_t cur_off;
    err_.Set(in_->Seek(0, SEEK_CUR, &cur_off));
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

  bool Scan() override {
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

  void Seek(ItemLocation loc) override {
    cr_->Seek(loc.block);
    if (!ReadBlock()) {
      return;
    }
    if (loc.item < 0 || loc.item >= n_items_) {
      std::ostringstream msg;
      msg << "Invalid location (" << loc.block << "," << loc.item
          << "): block has only %d items" << n_items_;
      err_.Set(msg.str());
      return;
    }
    next_item_ = loc.item;
  }

  // TODO(saito) this is unsafe. Change Mutable to return a vector<uint8_t>.
  std::vector<uint8_t>* Mutable() override { return item_; }
  ByteSpan Get() override { return ByteSpan{item_->data(), item_->size()}; }
  Error GetError() override { return err_.Err(); }
  std::vector<HeaderEntry> Header() override { return header_; }
  ByteSpan Trailer() override { return ByteSpan(&trailer_); }

 private:
  void readHeader() {
    std::vector<uint8_t>* payload;
    if (!ReadSpecialBlock(MagicHeader, &payload)) {
      return;
    }
    header_ = DecodeHeader(payload->data(), payload->size(), &err_);
    std::vector<std::string> transformers;
    for (const HeaderEntry& e : header_) {
      if (e.key == kKeyTransformer) {
        if (e.value.type != HeaderValue::STRING) {
          std::ostringstream msg;
          msg << "Wrong type for transformer: " << e.value.type;
          err_.Set(msg.str());
          return;
        } else {
          transformers.push_back(e.value.s);
        }
      }
    }
    err_.Set(GetUntransformer(transformers, &untransformer_));
  }

  void readTrailer() {
    cr_->SeekLastBlock();
    std::vector<uint8_t>* payload;
    if (ReadSpecialBlock(MagicTrailer, &payload)) {
      trailer_ = *payload;
    }
  }

  bool ReadBlock() {
    if (!err_.Ok()) return false;
    if (!cr_->Scan()) return false;
    const Magic magic = cr_->GetMagic();

    if (magic == MagicPacked) {
      n_items_ = ParseChunksToItems(cr_->Chunks(), untransformer_.get(),
                                    &itembuf_, &err_);
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
    n_items_ = ParseChunksToItems(cr_->Chunks(), untransformer_.get(),
                                  &itembuf_, &err_);
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
  std::unique_ptr<ReadSeeker> in_;
  int next_item_ = 0;
  std::vector<uint8_t>* item_ = nullptr;

  std::vector<std::vector<uint8_t>> itembuf_;
  int n_items_ = -1;
  std::vector<HeaderEntry> header_;
  std::vector<uint8_t> trailer_;
  std::unique_ptr<Transformer> untransformer_;
};

std::unique_ptr<Reader> NewReader(std::unique_ptr<ReadSeeker> in,
                                  ReaderOpts opts) {
  int64_t cur_off;
  internal::Error err = in->Seek(0, SEEK_CUR, &cur_off);
  if (err != "") {
    return std::unique_ptr<Reader>(new ErrorReaderImpl(err));
  }
  Magic magic;
  err = ReadFull(in.get(), magic.data(), magic.size());
  if (err != "") {
    return std::unique_ptr<Reader>(new ErrorReaderImpl(err));
  }
  err = AbsSeek(in.get(), cur_off);
  if (err != "") {
    return std::unique_ptr<Reader>(new ErrorReaderImpl(err));
  }
  if (magic == MagicPacked) {
    return NewLegacyPackedReader(std::move(in),
                                 std::move(opts.legacy_transformer));
  }
  if (magic == MagicUnpacked) {
    return internal::NewLegacyUnpackedReader(
        std::move(in), std::move(opts.legacy_transformer));
  }
  return std::unique_ptr<Reader>(
      new ReaderImpl(std::move(in), std::move(opts)));
}

class ReadSeekerAdapter : public ReadSeeker {
 public:
  explicit ReadSeekerAdapter(int fd) : fd_(fd) {}
  explicit ReadSeekerAdapter(Error err) : fd_(-1), err_(err) {}

  ~ReadSeekerAdapter() override {
    if (fd_ >= 0) {
      if (close(fd_) < 0) {
        std::cerr << "close " << fd_ << ": " << std::strerror(errno);
      }
    }
  }

  Error Seek(off_t off, int whence, off_t* new_off) {
    if (err_ != "") {
      *new_off = -1;
      return err_;
    }
    *new_off = lseek(fd_, off, whence);
    if (*new_off < 0) {
      std::ostringstream msg;
      msg << "lseek " << off << ": " << std::strerror(errno);
      return msg.str();
    }
    return "";
  }

  Error Read(uint8_t* buf, size_t bytes, ssize_t* bytes_read) {
    if (err_ != "") {
      *bytes_read = -1;
      return err_;
    }
    *bytes_read = read(fd_, buf, bytes);
    if (*bytes_read < 0) {
      std::ostringstream msg;
      msg << "read " << bytes << ": " << std::strerror(errno);
      return msg.str();
    }
    return "";
  }

 private:
  const int fd_;
  const Error err_;
};

}  // namespace
}  // namespace internal

std::unique_ptr<Reader> NewReader(std::unique_ptr<ReadSeeker> in,
                                  ReaderOpts opts) {
  return internal::NewReader(std::move(in), std::move(opts));
}

std::unique_ptr<ReadSeeker> NewReadSeekerFromDescriptor(int fd) {
  std::unique_ptr<ReadSeeker> x(new internal::ReadSeekerAdapter(fd));
  return x;
}

std::unique_ptr<Reader> NewReader(const std::string& path) {
  auto opts = DefaultReaderOpts(path);
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    std::ostringstream msg;
    msg << "open " << path << ": " << std::strerror(errno);
    std::unique_ptr<ReadSeeker> r(new internal::ReadSeekerAdapter(msg.str()));
    return internal::NewReader(std::move(r), std::move(opts));
  }
  return internal::NewReader(NewReadSeekerFromDescriptor(fd), std::move(opts));
}

}  // namespace recordio
}  // namespace grail
