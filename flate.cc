#include <zlib.h>
#include <iostream>
#include <mutex>
#include <regex>
#include <sstream>
#include <unordered_map>

#include "lib/recordio/recordio.h"

namespace grail {
namespace recordio {
namespace {

// Flate decompression transformer.
class UnflateTransformerImpl : public Transformer {
  internal::Error Transform(IoVec in_iov, IoVec* out) {
    *out = IoVec();
    z_stream stream;
    memset(&stream, 0, sizeof stream);
    int ret = inflateInit2(&stream, -15 /*RFC1951*/);
    if (ret != Z_OK) {
      std::ostringstream msg;
      msg << "inflateInit failed(" << ret << ")";
      return msg.str();
    }
    const size_t in_bytes = IoVecSize(in_iov);
    if (tmp_.capacity() >= in_bytes * 2) {
      tmp_.resize(tmp_.capacity());
    } else {
      tmp_.resize(in_bytes);
    }
    stream.avail_out = tmp_.size();
    stream.next_out = tmp_.data();
    ret = Z_STREAM_END;
    size_t iov_idx = 0;
    for (iov_idx = 0; iov_idx < in_iov.size(); ++iov_idx) {
      stream.avail_in = in_iov[iov_idx].size();
      stream.next_in = const_cast<Bytef*>(in_iov[iov_idx].data());
      for (;;) {
        ret = inflate(&stream, Z_NO_FLUSH);
        if (ret != Z_OK && ret != Z_STREAM_END) {
          std::ostringstream msg;
          msg << "inflate failed(" << ret << ")";
          inflateEnd(&stream);
          return msg.str();
        }
        if (ret == Z_STREAM_END || stream.avail_in == 0) {
          break;
        }
        if (stream.avail_out == 0) {
          size_t cur_size = tmp_.size();
          tmp_.resize(cur_size * 2);
          stream.avail_out = tmp_.size() - cur_size;
          stream.next_out = tmp_.data() + cur_size;
          continue;
        }
        abort();  // shouldn't happen.
      }
      if (ret == Z_STREAM_END) {
        iov_idx++;
        break;
      }
    }
    if (stream.avail_in != 0 || iov_idx != in_iov.size()) {
      std::ostringstream msg;
      msg << "found trailing junk during inflate";
      return msg.str();
    }
    inflateEnd(&stream);

    tmp_span_ = ByteSpan(tmp_.data(), tmp_.size() - stream.avail_out);
    *out = IoVec(&tmp_span_, 1);
    return "";
  }

  ByteSpan tmp_span_;
  std::vector<uint8_t> tmp_;
};

// Flate compression transformer.
class FlateTransformerImpl : public Transformer {
  internal::Error Transform(IoVec in_iov, IoVec* out) {
    internal::Error err;
    *out = IoVec();

    z_stream stream;
    memset(&stream, 0, sizeof stream);
    int ret = deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                           -15 /*RFC1951*/, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) {
      std::ostringstream msg;
      msg << "deflateInit failed(" << ret << ")";
      return msg.str();
    }

    const size_t in_bytes = IoVecSize(in_iov);
    tmp_.resize(deflateBound(&stream, in_bytes));
    stream.avail_out = tmp_.size();
    stream.next_out = reinterpret_cast<Bytef*>(tmp_.data());
    ret = Z_STREAM_END;
    size_t iov_idx = 0;
    for (iov_idx = 0; iov_idx < in_iov.size(); ++iov_idx) {
      stream.avail_in = in_iov[iov_idx].size();
      stream.next_in = const_cast<Bytef*>(
          reinterpret_cast<const Bytef*>(in_iov[iov_idx].data()));
      int flag = (iov_idx == in_iov.size() - 1) ? Z_FINISH : Z_NO_FLUSH;
      ret = deflate(&stream, flag);
      if (ret != Z_OK && ret != Z_STREAM_END) {
        std::ostringstream msg;
        msg << "deflate failed(" << ret << ")";
        deflateEnd(&stream);
        return msg.str();
      }
      if (stream.avail_in != 0) {
        abort();
      }
    }
    if (stream.avail_in != 0 || iov_idx != in_iov.size()) {
      std::ostringstream msg;
      msg << "found trailing junk during deflate";
      return msg.str();
    }
    deflateEnd(&stream);
    tmp_.resize(tmp_.size() - stream.avail_out);
    tmp_span_ = ByteSpan(&tmp_);
    *out = IoVec(&tmp_span_, 1);
    return "";
  }
  ByteSpan tmp_span_;
  std::vector<uint8_t> tmp_;
};

// Hack to register the flate transformers with name "flate" on process startup.
struct FlateInit {
  FlateInit() {
    RegisterTransformer(
        "flate",
        [](const std::string& arg, std::unique_ptr<Transformer>* tr) -> Error {
          // TODO(saito) use args to set the compression level.
          tr->reset(new FlateTransformerImpl);
          return "";
        },
        [](const std::string& arg, std::unique_ptr<Transformer>* tr) -> Error {
          tr->reset(new UnflateTransformerImpl);
          return "";
        });
  }
};
static FlateInit flate_init __attribute__((unused));
}  // namespace

std::unique_ptr<Transformer> UnflateTransformer() {
  return std::unique_ptr<Transformer>(new UnflateTransformerImpl());
}

std::unique_ptr<Transformer> FlateTransformer() {
  return std::unique_ptr<Transformer>(new FlateTransformerImpl());
}
}  // namespace recordio
}  // namespace grail
