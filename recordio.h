#ifndef LIB_RECORDIO_RECORDIO_H_
#define LIB_RECORDIO_RECORDIO_H_

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
//   std::ifstream in("test.recordio");
//   CHECK(!in.fail());
//   auto r := NewRecordIOReader(&in);
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

  // Get the current record. The caller may take ownership of the data by
  // swapping the contents.
  //
  // REQUIRE: The last call to Scan() returned true.
  virtual std::vector<char>* Mutable() = 0;

  // Get any error seen by the reader. It returns "" if there is no error.
  virtual std::string Error() = 0;

  RecordIOReader() = default;
  RecordIOReader(const RecordIOReader&) = delete;
  virtual ~RecordIOReader();
};

// Create a new reader that reads from "in". "in" remains owned by the caller,
// and it must remain live while the reader is in use.
std::unique_ptr<RecordIOReader> NewRecordIOReader(std::istream* in);

}  // namespace grail

#endif  // LIB_RECORDIO_RECORDIO_H_
