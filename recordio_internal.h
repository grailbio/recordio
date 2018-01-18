#ifndef LIB_RECORDIO_RECORDIO_INTERNAL_H_
#define LIB_RECORDIO_RECORDIO_INTERNAL_H_

#include <zlib.h>

#include <array>

namespace grail {

typedef std::array<uint8_t, 8> RecordIOMagic;

const RecordIOMagic RecordIOMagicUnpacked = {
    {0xfc, 0xae, 0x95, 0x31, 0xf0, 0xd9, 0xbd, 0x20}};
const RecordIOMagic RecordIOMagicPacked = {
    {0x2e, 0x76, 0x47, 0xeb, 0x34, 0x07, 0x3c, 0x2e}};

}  // namespace grail

inline uint32_t RecordIOCrc32(const char* data, int bytes) {
  return crc32(0, reinterpret_cast<const Bytef*>(data), bytes);
}

class RecordIOCleanup {
 public:
  virtual ~RecordIOCleanup() {}
};

#endif  // LIB_RECORDIO_RECORDIO_INTERNAL_H_
