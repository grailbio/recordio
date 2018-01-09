#ifndef LIB_RECORDIO_RECORDIO_H_
#define LIB_RECORDIO_RECORDIO_H_

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace grail {

// RecordIOReader reads a recordio file. Recordio file format is defined below:
//
// https://github.com/grailbio/base/blob/master/recordio/doc.go
//
// This class is thread compatible.
//
// Example:
//   auto r := NewRecordIOReader("test.grail-rio");
//   while (r->Scan()) {
//     std::vector<char> data;
//     std::swap(data, *r->Mutable());
//     .. use data ..
//   }
//   CHECK_EQ(r->Error(), "");
class RecordIOReader {
 public:
  // Read the next record. Scan() must also be called to read the very first
  // record.
  virtual bool Scan() = 0;

  // Span is like vector<char>, but doesn't own data.
  //
  // TODO(saito) Replace with std::span<char> once c++-20 comes out.
  struct Span {
    const char* data;
    size_t size;
  };

  // Get the current record. The span contents are owned by the reader. The
  // record is invalidated on the next call to Scan or the destructor.
  //
  // REQUIRES: The last call to Scan() returned true.
  virtual Span Get() = 0;

  // Get the current record. The caller may take ownership of the data by
  // swapping the contents. The record is invalidated on the next call to Scan
  // or the destructor.
  //
  // REQUIRES: The last call to Scan() returned true.
  virtual std::vector<char>* Mutable() = 0;

  // Get any error seen by the reader. It returns "" if there is no error.
  virtual std::string Error() = 0;

  RecordIOReader() = default;
  RecordIOReader(const RecordIOReader&) = delete;
  virtual ~RecordIOReader();
};

// Transformer is a closure invoked after reading a block.
class RecordIOTransformer {
 public:
  // Called on on every block read. "in" holds the data read from the file, and
  // this function should return another span. The span contents are owned by
  // this object, and it may be destroyed on the next call to Transform.  On
  // error, this function should set a nonempty *error.
  virtual RecordIOReader::Span Transform(RecordIOReader::Span in,
                                         std::string* error) = 0;
  RecordIOTransformer() = default;
  RecordIOTransformer(const RecordIOTransformer&) = delete;
  ~RecordIOTransformer();
};

struct RecordIOReaderOpts {
  // If packed=true, then parse the "packed" recordio file as defined in
  // https://github.com/grailbio/base/blob/master/recordio/doc.go. Get() or
  // Mutable() yields an item.  Else, parse an unpacked recordio. Get() or
  // Mutable() yields a block.
  bool packed = false;

  // If non-null, this function is called for every block read. It is called
  // sequentially.
  //
  // TODO(saito) This guaratee allows efficient implementations.  Maybe relax
  // the guaratee of sequential invocation in a future.
  std::unique_ptr<RecordIOTransformer> transformer;
};

// Create a new reader that reads from "in". "in" remains owned by the caller,
// and it must remain live while the reader is in use.
std::unique_ptr<RecordIOReader> NewRecordIOReader(std::istream* in,
                                                  RecordIOReaderOpts opts);

// Create a new reader for the given file. The options are auto-detected from
// the path suffix. This function always returns a non-null reader. Errors
// (e.g., nonexistent file) are reported through RecordIOReader::Error.
std::unique_ptr<RecordIOReader> NewRecordIOReader(const std::string& path);

// Given a pathname, construct options for parsing the file contents.
RecordIOReaderOpts DefaultRecordIOReaderOpts(const std::string& path);

// A transformer that uncompresses a block encoded in RPC 1951 format.
std::unique_ptr<RecordIOTransformer> UncompressRecordIOTransformer();

}  // namespace grail

#endif  // LIB_RECORDIO_RECORDIO_H_
