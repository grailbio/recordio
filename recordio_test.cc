#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <random>
#include <sstream>
#include <utility>

#include "lib/recordio/internal.h"
#include "lib/recordio/recordio.h"
#include "lib/test_util/test_util.h"

namespace grail {

std::string Str(recordio::Reader* r) {
  std::string s;
  for (char ch : *r->Mutable()) {
    s.append(1, ch);
  }
  return s;
}

const int TestBlockCount = 128;

std::string TestBlock(int n) {
  const std::string str =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  const int record_size = 8;
  const int start_index = n % (str.size() - record_size + 1);
  return str.substr(start_index, record_size);
}

void WriteContentsAndClose(recordio::Writer* w) {
  for (int i = 0; i < TestBlockCount; i++) {
    std::string block = TestBlock(i);
    ASSERT_TRUE(w->Write(recordio::ByteSpan{
        reinterpret_cast<const uint8_t*>(block.data()), block.size()}));
  }
  ASSERT_TRUE(w->Close());
}

void CheckContents(recordio::Reader* r) {
  int n = 0;
  while (r->Scan()) {
    const std::string expected = TestBlock(n);
    EXPECT_EQ(expected, Str(r));
    EXPECT_EQ("", r->Error());
    n++;
  }
  EXPECT_EQ("", r->Error());
  EXPECT_EQ(TestBlockCount, n);
}

void CheckHeader(recordio::Reader* r) {
  auto h = r->Header();
  ASSERT_EQ(h.size(), 4);
  ASSERT_EQ(h[0].key, "intflag");
  ASSERT_EQ(h[0].value.type, recordio::HeaderValue::INT);
  ASSERT_EQ(h[0].value.i, 12345);

  ASSERT_EQ(h[1].key, "uintflag");
  ASSERT_EQ(h[1].value.type, recordio::HeaderValue::UINT);
  ASSERT_EQ(h[1].value.u, 12345);

  ASSERT_EQ(h[1].key, "strflag");
  ASSERT_EQ(h[1].value.type, recordio::HeaderValue::STRING);
  ASSERT_EQ(h[1].value.s, "Hello");

  ASSERT_EQ(h[1].key, "boolflag");
  ASSERT_EQ(h[1].value.type, recordio::HeaderValue::BOOL);
  ASSERT_EQ(h[1].value.b, true);
}

TEST(Recordio, Read) {
  std::ifstream in("lib/recordio/testdata/test.grail-rio");
  ASSERT_FALSE(in.fail());
  auto r = recordio::NewReader(&in, recordio::ReaderOpts{});
  CheckContents(r.get());
}

std::string ReadFile(std::string filename) {
  std::ifstream file(filename);
  EXPECT_FALSE(file.fail());
  std::ostringstream contents;
  std::copy(std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>(),
            std::ostreambuf_iterator<char>(contents));
  return contents.str();
}

TEST(Recordio, Write) {
  std::string filename = test_util::GetTempDirPath() + "/test.grail-rio";
  {
    auto r = recordio::NewWriter(filename);
    WriteContentsAndClose(r.get());
  }

  EXPECT_EQ(ReadFile("lib/recordio/testdata/test.grail-rio"),
            ReadFile(filename));

  remove(filename.c_str());
}

TEST(Recordio, WritePacked) {
  std::string filename = test_util::GetTempDirPath() + "/test.grail-rpk";
  {
    auto r = recordio::NewWriter(filename);
    WriteContentsAndClose(r.get());
  }

  EXPECT_EQ(ReadFile("lib/recordio/testdata/test.grail-rpk"),
            ReadFile(filename));

  remove(filename.c_str());
}

TEST(Recordio, WritePackedGz) {
  std::string filename = test_util::GetTempDirPath() + "/test.grail-rpk-gz";
  {
    auto r = recordio::NewWriter(filename);
    WriteContentsAndClose(r.get());
  }

  {
    auto r = recordio::NewReader(filename);
    CheckContents(r.get());
  }

  remove(filename.c_str());
}

TEST(Recordio, WritePackingOptions) {
  std::string filename = test_util::GetTempDirPath() + "/test.grail-rpk-gz";
  {
    auto opts = recordio::DefaultWriterOpts(filename);
    opts.max_packed_items = 3;
    opts.max_packed_bytes = 100;
    std::ofstream out(filename);
    auto r = recordio::NewWriter(&out, std::move(opts));
    WriteContentsAndClose(r.get());
  }

  {
    auto r = recordio::NewReader(filename);
    CheckContents(r.get());
  }

  remove(filename.c_str());
}

class TestIndexer : public recordio::WriterIndexer {
 public:
  // Caller retains ownership of block_offsets.
  explicit TestIndexer(std::vector<uint64_t>* block_offsets)
      : block_offsets_(block_offsets) {}

  std::string IndexBlock(uint64_t start_offset) override {
    block_offsets_->push_back(start_offset);
    return "";
  }

  ~TestIndexer() {}

 private:
  std::vector<uint64_t>* block_offsets_;
};

TEST(Recordio, WriteIndex) {
  std::string filename = test_util::GetTempDirPath() + "/test.grail-rio";
  std::vector<uint64_t> block_offsets;
  {
    auto opts = recordio::DefaultWriterOpts(filename);
    opts.indexer.reset(new TestIndexer(&block_offsets));

    std::ofstream out(filename);
    auto r = recordio::NewWriter(&out, std::move(opts));
    WriteContentsAndClose(r.get());
  }

  ASSERT_GT(block_offsets.size(), 0);

  // To check that block offsets are correct, create a reader at some offsets,
  // read a few blocks, and check their contents. This also demonstrates how to
  // read a recordio file concurrently.
  for (int block = 0; block < TestBlockCount;) {
    std::ifstream in(filename);
    in.seekg(static_cast<std::streampos>(block_offsets[block]));
    ASSERT_FALSE(in.fail());
    ASSERT_FALSE(in.eof());
    auto r = recordio::NewReader(&in, recordio::DefaultReaderOpts(filename));

    for (int i = 0; i < 10 && block < TestBlockCount; i++) {
      ASSERT_TRUE(r->Scan());
      EXPECT_EQ(TestBlock(block), Str(r.get())) << "i=" << i;
      block++;
    }
  }
}

TEST(Recordio, ReadPacked) {
  auto r = recordio::NewReader("lib/recordio/testdata/test.grail-rpk");
  CheckContents(r.get());
}

TEST(Recordio, ReadPackedGz) {
  auto r = recordio::NewReader("lib/recordio/testdata/test.grail-rpk-gz");
  CheckContents(r.get());
}

TEST(Recordio, Read2) {
  auto r = recordio::NewReader("lib/recordio/testdata/test.grail-rio2");
  CheckContents(r.get());
}

TEST(Recordio, ReadError) {
  auto r = recordio::NewReader("/non/existent/file");
  EXPECT_FALSE(r->Scan());
  EXPECT_THAT(r->Error(), ::testing::HasSubstr("No such file or directory"));
}

void DoCompressTest(const std::string& str, int n_iov) {
  std::string err("");

  std::vector<recordio::ByteSpan> in(n_iov);
  int chunk_len = str.size() / n_iov;
  auto compressor = recordio::CompressTransformer();

  int start = 0;
  for (int i = 0; i < n_iov; i++) {
    const int len = (i < n_iov - 1) ? chunk_len : (str.size() - start);
    in[i] = recordio::ByteSpan(
        reinterpret_cast<const uint8_t*>(str.data()) + start, len);
    start += len;
  }
  const recordio::IoVec compressed =
      compressor->Transform(recordio::IoVec{&in}, &err);
  ASSERT_EQ(std::string(""), err);
  ASSERT_GT(recordio::internal::IoVecSize(compressed), 0);

  auto uncompressor = recordio::UncompressTransformer();
  auto uncompressed = uncompressor->Transform(compressed, &err);
  ASSERT_EQ(std::string(""), err);
  ASSERT_GT(recordio::internal::IoVecSize(uncompressed), 0);
  auto flattened = recordio::internal::IoVecFlatten(uncompressed);
  const std::string str2(reinterpret_cast<const char*>(flattened.data()),
                         flattened.size());
  ASSERT_EQ(str, str2);
}

TEST(Recordio, CompressSmall) {
  DoCompressTest("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", 1);
  DoCompressTest("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", 2);
}

TEST(Recordio, CompressRandom) {
  std::default_random_engine r;
  for (int i = 0; i < 20; i++) {
    const int len = std::uniform_int_distribution<int>(128, 100000)(r);
    const int num_iov = std::uniform_int_distribution<int>(1, 10)(r);
    std::vector<char> data(len);
    std::uniform_int_distribution<int> d(0, 64);
    for (int j = 0; j < len; j++) {
      data[j] = 'A' + d(r);
    }
    DoCompressTest(std::string(&data[0], len), num_iov);
  }
}

}  // namespace grail
