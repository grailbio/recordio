#ifndef LIB_RECORDIO_CHUNK_H_
#define LIB_RECORDIO_CHUNK_H_

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "lib/recordio/internal.h"
#include "lib/recordio/recordio.h"

namespace grail {
namespace recordio {
namespace internal {

class ErrorReporter;
constexpr int ChunkSize = 32 << 10;
typedef uint32_t ChunkFlag;
typedef std::array<uint8_t, ChunkSize> ChunkBuf;

// Helper class for reading raw chunks and assembling into a block, without any
// transformation.
class ChunkReader {
 public:
  ChunkReader(std::istream* in, ErrorReporter* err);
  // Read the next block.
  bool Scan();
  // Read the chunks that constitute the current block.
  //
  // REQUIRE: Last call to Scan() returned true.
  IoVec Chunks() const { return IoVec(&iov_); }
  // Read the magic number of the current block.
  //
  // REQUIRE: Last call to Scan() returned true.
  Magic GetMagic() const { return magic_; }
  // Seek to the given offset. The next Scan() call will read the block at the
  // offset.
  void Seek(int64_t off);
  // Seek to the last block (i.e., trailer).
  void SeekLastBlock();

 private:
  // Read one chunk from in_.
  bool ReadChunk(Magic* magic, uint32_t* index, uint32_t* total,
                 ChunkFlag* flag, ByteSpan* payload);

  std::istream* in_;
  ErrorReporter* err_;
  Magic magic_;
  std::vector<ByteSpan> iov_;
  ChunkBuf buf_;  // tmp
  ChunkReader(const ChunkReader&) = delete;
};

}  // namespace internal
}  // namespace recordio
}  // namespace grail
#endif  // LIB_RECORDIO_CHUNK_H_
