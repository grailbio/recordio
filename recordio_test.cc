#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <sstream>
#include <utility>

#include "lib/recordio/recordio.h"
#include "lib/test_util/test_util.h"

namespace grail {

std::string Str(RecordIOReader* r) {
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

void WriteContentsAndClose(RecordIOWriter* w) {
  for (int i = 0; i < TestBlockCount; i++) {
    std::string block = TestBlock(i);
    ASSERT_TRUE(w->Write(RecordIOSpan{block.data(), block.size()}));
  }
  ASSERT_TRUE(w->Close());
}

void CheckContents(RecordIOReader* r) {
  int n = 0;
  while (r->Scan()) {
    const std::string expected = TestBlock(n);
    EXPECT_EQ(expected, Str(r));
    EXPECT_EQ("", r->Error());
    n++;
  }
  EXPECT_EQ(TestBlockCount, n);
  EXPECT_EQ("", r->Error());
}

TEST(Recordio, Read) {
  std::ifstream in("lib/recordio/testdata/test.grail-rio");
  ASSERT_FALSE(in.fail());
  auto r = NewRecordIOReader(&in, RecordIOReaderOpts{});
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
    auto r = NewRecordIOWriter(filename);
    WriteContentsAndClose(r.get());
  }

  EXPECT_EQ(ReadFile("lib/recordio/testdata/test.grail-rio"),
            ReadFile(filename));

  remove(filename.c_str());
}

TEST(Recordio, WritePacked) {
  std::string filename = test_util::GetTempDirPath() + "/test.grail-rpk";
  {
    auto r = NewRecordIOWriter(filename);
    WriteContentsAndClose(r.get());
  }

  EXPECT_EQ(ReadFile("lib/recordio/testdata/test.grail-rpk"),
            ReadFile(filename));

  remove(filename.c_str());
}

TEST(Recordio, WritePackedGz) {
  std::string filename = test_util::GetTempDirPath() + "/test.grail-rpk-gz";
  {
    auto r = NewRecordIOWriter(filename);
    WriteContentsAndClose(r.get());
  }

  {
    auto r = NewRecordIOReader(filename);
    CheckContents(r.get());
  }

  remove(filename.c_str());
}

TEST(Recordio, WritePackingOptions) {
  std::string filename = test_util::GetTempDirPath() + "/test.grail-rpk-gz";
  {
    auto opts = DefaultRecordIOWriterOpts(filename);
    opts.max_packed_items = 3;
    opts.max_packed_bytes = 100;
    std::ofstream out(filename);
    auto r = NewRecordIOWriter(&out, std::move(opts));
    WriteContentsAndClose(r.get());
  }

  {
    auto r = NewRecordIOReader(filename);
    CheckContents(r.get());
  }

  remove(filename.c_str());
}

class TestIndexer : public RecordIOWriterIndexer {
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
    auto opts = DefaultRecordIOWriterOpts(filename);
    opts.indexer.reset(new TestIndexer(&block_offsets));

    std::ofstream out(filename);
    auto r = NewRecordIOWriter(&out, std::move(opts));
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
    auto r = NewRecordIOReader(&in, DefaultRecordIOReaderOpts(filename));

    for (int i = 0; i < 10 && block < TestBlockCount; i++) {
      ASSERT_TRUE(r->Scan());
      EXPECT_EQ(TestBlock(block), Str(r.get()));
      block++;
    }
  }
}

TEST(Recordio, ReadPacked) {
  auto r = NewRecordIOReader("lib/recordio/testdata/test.grail-rpk");
  CheckContents(r.get());
}

TEST(Recordio, ReadPackedGz) {
  auto r = NewRecordIOReader("lib/recordio/testdata/test.grail-rpk-gz");
  CheckContents(r.get());
}

TEST(Recordio, ReadError) {
  auto r = NewRecordIOReader("/non/existent/file");
  EXPECT_FALSE(r->Scan());
  EXPECT_THAT(r->Error(), ::testing::HasSubstr("No such file or directory"));
}

TEST(Recordio, CompressTransformers) {
  const std::string str =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::string err("");

  auto compressor = CompressRecordIOTransformer();
  auto compressed =
      compressor->Transform(RecordIOSpan{str.data(), str.size()}, &err);
  ASSERT_EQ(std::string(""), err);
  ASSERT_NE(compressed.data, nullptr);
  ASSERT_GT(compressed.size, 0);

  auto uncompressor = UncompressRecordIOTransformer();
  auto uncompressed = uncompressor->Transform(compressed, &err);
  ASSERT_EQ(std::string(""), err);
  ASSERT_NE(uncompressed.data, nullptr);
  ASSERT_GT(uncompressed.size, 0);

  const std::string str2(uncompressed.data, uncompressed.size);
  ASSERT_EQ(str, str2);
}

}  // namespace grail
