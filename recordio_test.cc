#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <sstream>

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
    for (int i = 0; i < TestBlockCount; i++) {
      std::string block = TestBlock(i);
      ASSERT_TRUE(r->Write(RecordIOSpan{block.data(), block.size()}));
    }
  }

  EXPECT_EQ(ReadFile("lib/recordio/testdata/test.grail-rio"),
            ReadFile(filename));

  remove(filename.c_str());
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
