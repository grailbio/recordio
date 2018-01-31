
#ifndef LIB_RECORDIO_HEADER_H_
#define LIB_RECORDIO_HEADER_H_

#include <cstdint>
#include <string>
#include <vector>

namespace grail {
namespace recordio {

// HeaderEntry is a result of parsing the header block. The header block stores
// key-value pairs, and HeaderEntry represents a single key and value pair.
struct HeaderValue {
  enum Type { INVALID = 0, BOOL = 1, INT = 2, UINT = 3, STRING = 4 };
  Type type;
  bool b;         // Valid iff type==BOOL
  int64_t i;      // Valid iff type==INT
  uint64_t u;     // Valid iff type==UINT
  std::string s;  // Valid iff type==STRING
};

struct HeaderEntry {
  std::string key;
  HeaderValue value;
};

// Key "trailer". Indicates whether a trailer block is present.  The value is
// BOOL.
extern const char* const kKeyTrailer;

namespace internal {
class ErrorReporter;

// Decode the contents of a header item.
std::vector<HeaderEntry> DecodeHeader(const uint8_t* data, int size,
                                      ErrorReporter* err);

// See if the header has entry {"trailer", true}.
std::string HasTrailer(const std::vector<HeaderEntry>& header, bool* v);
}  // namespace internal
}  // namespace recordio
}  // namespace grail
#endif  // LIB_RECORDIO_HEADER_H_
