#include <zlib.h>

#include <cstring>
#include <sstream>

#include "lib/recordio/recordio.h"

namespace grail {
namespace {

class CompressTransformer : public RecordIOTransformer {
  RecordIOSpan Transform(RecordIOSpan in, std::string* err) {
    err->clear();
    z_stream stream;
    memset(&stream, 0, sizeof stream);
    int ret = deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
        -15 /*RFC1951*/, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) {
      std::ostringstream msg;
      msg << "deflateInit failed(" << ret << ")";
      *err = msg.str();
      return RecordIOSpan{nullptr, 0};
    }
    tmp_.resize(deflateBound(&stream, in.size));
    stream.avail_in = in.size;
    stream.next_in =
        const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data));
    stream.avail_out = tmp_.size();
    stream.next_out = reinterpret_cast<Bytef*>(tmp_.data());
    ret = deflate(&stream, Z_FINISH);
    if (ret != Z_STREAM_END) {
      std::ostringstream msg;
      msg << "deflate failed(" << ret << ")";
      *err = msg.str();
      deflateEnd(&stream);
      return RecordIOSpan{nullptr, 0};
    }
    deflateEnd(&stream);
    return RecordIOSpan{tmp_.data(), tmp_.size() - stream.avail_out};
  }
  std::vector<char> tmp_;
};

}  // namespace
}  // namespace grail

std::unique_ptr<grail::RecordIOTransformer>
grail::CompressRecordIOTransformer() {
  return std::unique_ptr<grail::RecordIOTransformer>(
      new CompressTransformer());
}
