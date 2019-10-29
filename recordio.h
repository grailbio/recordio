#ifndef LIB_RECORDIO_RECORDIO_H_
#define LIB_RECORDIO_RECORDIO_H_

// Recordio file reader and writer.
//
// https://github.com/grailbio/base/blob/master/recordio/README.md
//
// The C++ reader handles the old and the new file formats.
//
// The writer supports only the old file format as of 2018-02.
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "./header.h"
#include "./internal.h"

namespace grail {
namespace recordio {
using ByteSpan = internal::ByteSpan;
using IoVec = internal::IoVec;
using Error = internal::Error;
using ReadSeeker = internal::ReadSeeker;

// ItemLocation identifies the location of an item in a recordio file.
struct ItemLocation {
  // Location of the first byte of the block within the file. Unit is bytes.
  int64_t block;
  // Index of the item within the block. The Nth item in the block (N=1,2,...)
  // has value N-1.
  int item;
};

// Class Reader reads a recordio file.
//
// This class is thread compatible.
//
// Example:
//   auto r := recordio::NewReader("test.grail-rio");
//   while (r->Scan()) {
//     std::vector<uint8_t> data;
//     std::swap(data, *r->Mutable());
//     .. use data ..
//   }
//   CHECK_EQ(r->Error(), "");
class Reader {
 public:
  // Read the next record. Scan() must also be called to read the very first
  // record.
  virtual bool Scan() = 0;

  // Get the current record. The span contents are owned by the reader. The
  // record is invalidated on the next call to Scan or the destructor.
  //
  // REQUIRES: The last call to Scan() returned true.
  virtual ByteSpan Get() = 0;

  // Set up so that the next Scan() call causes the pointer to move to the given
  // location.  On any error, Error() will be set.
  //
  // REQUIRES: loc must be one of the values passed to the Index callback during
  // writes.
  virtual void Seek(ItemLocation loc) = 0;

  // Get the current record. The caller may take ownership of the data by
  // swapping the contents. The record is invalidated on the next call to Scan
  // or the destructor.
  //
  // REQUIRES: The last call to Scan() returned true.
  virtual std::vector<uint8_t>* Mutable() = 0;

  // Return the header-block contents. It returns an empty array if the header
  // doesn't exist, or on error. Check Error() distinguish the two cases.
  virtual std::vector<HeaderEntry> Header() = 0;

  // Return the header-block contents. It returns an empty array if the trailer
  // doesn't exist, or on error. Check Error() distinguish the two cases.
  virtual ByteSpan Trailer() = 0;

  // Get any error seen by the reader. It returns "" if there is no error.
  virtual Error GetError() = 0;

  Reader() = default;
  Reader(const Reader&) = delete;
  virtual ~Reader() = default;
};

// Transformer is invoked to (un)compress or (un)encrypt a block.
class Transformer {
 public:
  // Called on on every block read. "in" holds the data read from the file, and
  // this function should return another span. The span contents are owned by
  // this object, and it may be destroyed on the next call to Transform.  On
  // error, this function should set a nonempty *error.
  virtual Error Transform(IoVec in, IoVec* out) = 0;
  Transformer() = default;
  Transformer(const Transformer&) = delete;
  virtual ~Transformer() = default;
};

struct ReaderOpts {
  // If non-null, this function is called for every block read. It is called
  // sequentially.
  //
  // TODO(saito) This guarantee allows efficient implementations.  Maybe relax
  // the guarantee of sequential invocation in a future.
  std::unique_ptr<Transformer> legacy_transformer;
};

// Create a ReadSeeker object that reads from file "fd".  "fd" will be closed
// when the readseeker is destroyed.
std::unique_ptr<ReadSeeker> NewReadSeekerFromDescriptor(int fd);

// Create a new reader that reads from "in".
std::unique_ptr<Reader> NewReader(std::unique_ptr<ReadSeeker> in,
                                  ReaderOpts opts);

// Create a new reader for the given file. The options are auto-detected from
// the path suffix. This function always returns a non-null reader. Errors
// (e.g., nonexistent file) are reported through Reader::Error.
std::unique_ptr<Reader> NewReader(const std::string& path);

// Register callbacks to create a transformer and a reverse transformer.  Name
// is a string such as "flate", "zstd". The transformer_factory should create a
// closure that takes an iovec and produces another iovec suitable for storing
// at rest. The untransformer_factory should create a closure that does reverse.
//
// The transformer factory is invoked by
// the writer, and the untransformer factory is invoked by the reader.
//
// This function is usually invoked when the process starts.
void RegisterTransformer(
    const std::string& name,
    const std::function<Error(const std::string& args,
                              std::unique_ptr<Transformer>* tr)>&
        transformer_factory,
    const std::function<Error(const std::string& args,
                              std::unique_ptr<Transformer>* tr)>&
        untransformer_factory);

// Given string such as "flate 5", create a transformer. The transformer
// ("flate" in this example) must be registered already.
Error GetTransformer(const std::vector<std::string>& names,
                     std::unique_ptr<Transformer>* tr);
// Given string such as "flate 5", create a reverse transformer. The transformer
// ("flate" in this example) must be registered already.
Error GetUntransformer(const std::vector<std::string>& names,
                       std::unique_ptr<Transformer>* tr);

// Writer writes a recordio file. Recordio file format is defined below:
//
// https://github.com/grailbio/base/blob/master/recordio/doc.go
//
// This class is not thread safe.
class Writer {
 public:
  // Write a new record. Caller owns the data. The writer will not modify the
  // data, not even temporarily. Returns true if successful. Check Error() on
  // failure.
  virtual bool Write(ByteSpan in) = 0;

  // Close the writer and underlying resources. After Close(), callers must not
  // Write() anymore. Callers may still call Error(). To ensure the last block
  // is written, Close() must be called after the last Write().
  virtual bool Close() = 0;

  // Get any error seen by the writer. It returns "" if there is no error.
  virtual Error GetError() = 0;

  Writer() = default;
  Writer(const Writer&) = delete;
  virtual ~Writer();
};

constexpr int64_t WriterDefaultMaxPackedItems = 16 * 1024;
constexpr int64_t WriterDefaultMaxPackedBytes = 16 * 1024 * 1024;

// WriterIndexer defines a callback so users of Writer can
// build an index while Writer is writing a recordio file. It only
// allows indexing blocks, not items, regardless of whether the writer is
// creating a packed or unpacked file.
// TODO(josh): Consider adding item indexing support (need to handle block
// transformations).
class WriterIndexer {
 public:
  // IndexBlock is invoked when the writer finishes writing a block starting
  // at start_offset. If any error occurs, implementation must return a
  // non-empty message.
  virtual Error IndexBlock(uint64_t start_offset) = 0;

  virtual ~WriterIndexer();
};

// Caution: The writer only supports the V1 format.
//
// TODO(saito) Support V2.

struct WriterOpts {
  // If packed=true, then write the "packed" recordio file as defined in
  // https://github.com/grailbio/base/blob/master/recordio/doc.go, and callers
  // must pass one item to Write() on each invocation. Else, write an unpacked
  // recordio, and callers must pass a whole block to Write() on each
  // invocation.
  bool packed = false;

  // max_packed_items is the maximum number of items that will be packed into
  // a single block. This is ignored if packed == false.
  int64_t max_packed_items = WriterDefaultMaxPackedItems;

  // max_packed_bytes is the maximum total item size that will be packed into
  // a single block. This is ignored if packed == false. Note that size is
  // measured before transformation.
  int64_t max_packed_bytes = WriterDefaultMaxPackedBytes;

  // If non-null, this function is called for every block write. Users should
  // provide the inverse transformation to recover the original block.
  std::unique_ptr<Transformer> transformer = nullptr;

  // If non-null, this function is called after every block write.
  std::unique_ptr<WriterIndexer> indexer = nullptr;
};

// Create a new writer that writes to "out". "out" remains owned by the caller,
// and it must remain live while the writer is in use.
std::unique_ptr<Writer> NewWriter(std::ostream* out, WriterOpts opts);

// Create a new writer for the given file. The options are auto-detected from
// the path suffix. This function always returns a non-null writer. Errors
// (e.g., nonexistent file) are reported through Writer::Error.
std::unique_ptr<Writer> NewWriter(const std::string& path);

// Given a pathname, construct options for parsing the file contents.
WriterOpts DefaultWriterOpts(const std::string& path);

//
//  Following definitions are deprecated. Don't use in new code.
//

// Given a pathname, construct options for parsing the file contents.
ReaderOpts DefaultReaderOpts(const std::string& path);

// A transformer that uncompresses a block encoded in RFC 1951 format.
std::unique_ptr<Transformer> UnflateTransformer();

// A transformer that compresses a block encoded in RFC 1951 format.
std::unique_ptr<Transformer> FlateTransformer();

}  // namespace recordio
}  // namespace grail

#endif  // LIB_RECORDIO_RECORDIO_H_
