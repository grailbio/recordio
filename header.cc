#include "./header.h"

#include <sstream>
#include "./internal.h"
#include "./recordio.h"

namespace grail {
namespace recordio {

const char* const kKeyTrailer = "trailer";
const char* const kKeyTransformer = "transformer";
namespace {

HeaderValue ReadValue(internal::BinaryParser* parser) {
  HeaderValue v{HeaderValue::INVALID, false, 0, 0, ""};
  const uint8_t* type_ptr = parser->ReadBytes(1);
  if (type_ptr == nullptr) return v;
  switch (*type_ptr) {
    case HeaderValue::BOOL: {
      const uint8_t* b = parser->ReadBytes(1);
      if (b == nullptr) return v;
      v.b = (*b != 0);
      break;
    }
    case HeaderValue::INT: {
      v.i = parser->ReadVarint();
      break;
    }
    case HeaderValue::UINT: {
      v.u = parser->ReadUVarint();
      break;
    }
    case HeaderValue::STRING: {
      auto rn = ReadValue(parser);
      if (rn.type != HeaderValue::UINT) {
        parser->err()->Set("Failed to read string length");
        return v;
      }
      v.s = parser->ReadString(rn.u);
      break;
    }
    default:
      parser->err()->Set("Invalid value type");
      return v;
  }
  v.type = HeaderValue::INVALID;
  if (parser->err()->Ok()) {
    v.type = static_cast<HeaderValue::Type>(*type_ptr);
  }
  return v;
}

}  // namespace

std::vector<HeaderEntry> internal::DecodeHeader(const uint8_t* data, int size,
                                                ErrorReporter* err) {
  std::vector<HeaderEntry> entries;
  BinaryParser parser(data, size, err);
  auto rn = ReadValue(&parser);
  if (rn.type != HeaderValue::UINT) {
    err->Set("Failed to read # header entries");
    return entries;
  }
  for (int i = 0; i < static_cast<int>(rn.u); i++) {
    auto rkey = ReadValue(&parser);
    if (rkey.type != HeaderValue::STRING) {
      err->Set("Failed to read header key");
      return entries;
    }
    auto value = ReadValue(&parser);
    if (!err->Ok()) return entries;
    entries.push_back(HeaderEntry{rkey.s, value});
  }
  return entries;
}

internal::Error internal::HasTrailer(const std::vector<HeaderEntry>& header,
                                     bool* v) {
  *v = false;
  for (const auto& h : header) {
    if (h.key == kKeyTrailer) {
      if (h.value.type != HeaderValue::BOOL) {
        std::ostringstream msg;
        msg << "Wrong trailer value type: " << h.value.type;
        return msg.str();
      }
      *v = h.value.b;
      break;
    }
  }
  return "";
}

}  // namespace recordio
}  // namespace grail
