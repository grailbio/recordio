#include "lib/recordio/chunk.h"

#include <cerrno>
#include <cstring>
#include <sstream>

namespace grail {
namespace recordio {

namespace {
const int ChunkHeaderSize = 28;
const int MaxChunkPayloadSize = internal::ChunkSize - ChunkHeaderSize;
}  // namespace

internal::ChunkReader::ChunkReader(ReadSeeker* in, ErrorReporter* err)
    : in_(in), err_(err), magic_(MagicInvalid), next_free_chunk_(0) {}

bool internal::ChunkReader::Scan() {
  magic_ = MagicInvalid;
  iov_.clear();
  next_free_chunk_ = 0;
  uint32_t total_chunks = 0;
  if (!err_->Ok()) {
    return false;
  }
  for (;;) {
    Magic magic;
    uint32_t index, total;
    ChunkFlag flag;
    ByteSpan payload;
    if (!ReadChunk(&magic, &index, &total, &flag, &payload)) {
      return false;
    }
    if (!err_->Ok()) {
      return false;
    }

    if (iov_.empty()) {  // First chunk of the block?
      magic_ = magic;
      total_chunks = total;
    }
    if (magic_ != magic) {
      std::ostringstream msg;
      msg << "Magic number changed in the middle of a chunk sequence, got "
          << MagicDebugString(magic) << " expect " << MagicDebugString(magic_);
      err_->Set(msg.str());
      return false;
    }
    if (index != iov_.size()) {
      std::ostringstream msg;
      msg << "Wrong chunk index " << index << ", expect " << iov_.size()
          << " for magic " << MagicDebugString(magic);
      err_->Set(msg.str());
      return false;
    }
    if (total_chunks != total) {
      std::ostringstream msg;
      msg << "Wrong total chunk header " << total << ", expect " << total_chunks
          << " for magic " << MagicDebugString(magic);
      err_->Set(msg.str());
      return false;
    }
    iov_.push_back(payload);
    if (index + 1 == total) {
      break;
    }
  }
  return true;
}

void internal::ChunkReader::SeekLastBlock() {
  int64_t unused;
  err_->Set(in_->Seek(-ChunkSize, SEEK_END, &unused));
  if (!err_->Ok()) {
    return;
  }
  Magic magic;
  uint32_t index, total;
  ChunkFlag flag;
  ByteSpan payload;
  if (!ReadChunk(&magic, &index, &total, &flag, &payload)) {
    err_->Set("Failed to read last chunk");
    return;
  }
  if (magic != MagicTrailer) {
    std::ostringstream msg;
    msg << "Wrong magic for the trailer block: " << MagicDebugString(magic);
    err_->Set(msg.str());
    return;
  }
  const off_t off = -ChunkSize * (static_cast<int>(index) + 1);
  err_->Set(in_->Seek(off, SEEK_END, &unused));
  if (!err_->Ok()) {
    return;
  }
}

void internal::ChunkReader::Seek(int64_t off) { err_->Set(AbsSeek(in_, off)); }

bool internal::ChunkReader::ReadChunk(Magic* magic, uint32_t* index,
                                      uint32_t* total, ChunkFlag* flag,
                                      ByteSpan* payload) {
  while (next_free_chunk_ >= static_cast<int>(free_chunks_.size())) {
    free_chunks_.push_back(std::unique_ptr<ChunkBuf>(new ChunkBuf));
  }
  ChunkBuf* buf = free_chunks_[next_free_chunk_].get();
  next_free_chunk_++;
  ssize_t n;
  Error err = in_->Read(buf->data(), ChunkSize, &n);
  if (err != "" || n <= 0) {
    std::cout << "read: " << n << " " << err << "\n";
    return false;
  }
  if (n != ChunkSize) {
    std::ostringstream msg;
    msg << "Failed to read chunk, got " << n << " byte, expect " << ChunkSize
        << "bytes: " << std::strerror(errno);
    err_->Set(msg.str());
    return false;
  }
  BinaryParser header(buf->data(), ChunkHeaderSize, err_);

  *magic = *reinterpret_cast<const Magic*>(header.ReadBytes(sizeof(Magic)));
  const uint32_t expected_csum = header.ReadLEUint32();
  *flag = header.ReadLEUint32();
  const uint32_t size = header.ReadLEUint32();
  *total = header.ReadLEUint32();
  *index = header.ReadLEUint32();
  if (!err_->Ok()) {
    return false;
  }
  if (size > MaxChunkPayloadSize) {
    std::ostringstream msg;
    msg << "Invalid chunk size " << size;
    err_->Set(msg.str());
    return false;
  }

  *payload = ByteSpan(buf->data() + ChunkHeaderSize, size);
  const uint32_t actual_csum =
      Crc32(buf->data() + 12, ChunkHeaderSize - 12 + size);
  if (expected_csum != actual_csum) {
    std::ostringstream msg;
    msg << "Chunk checksum mismatch, expect " << expected_csum << " got "
        << actual_csum;
    err_->Set(msg.str());
    return false;
  }
  return true;
}

}  // namespace recordio
}  // namespace grail
