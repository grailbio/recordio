#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <cstdio>
#include <fstream>

#include "lib/recordio/recordio.h"

namespace grail {

std::string Str(RecordIOReader* r) {
  std::string s;
  for (char ch : *r->Mutable()) {
    s.append(1, ch);
  }
  return s;
}

void CheckContents(RecordIOReader* r) {
  int n = 0;
  const std::string str =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  const int record_size = 8;
  while (r->Scan()) {
    const int start_index = n % (str.size() - record_size + 1);
    const std::string expected = str.substr(start_index, record_size);
    EXPECT_EQ(expected, Str(r));
    EXPECT_EQ("", r->Error());
    n++;
  }
  EXPECT_EQ(128, n);
  EXPECT_EQ("", r->Error());
}

TEST(Recordio, Read) {
  std::ifstream in("lib/recordio/testdata/test.grail-rio");
  ASSERT_FALSE(in.fail());
  auto r = NewRecordIOReader(&in, RecordIOReaderOpts{});
  CheckContents(r.get());
}

TEST(Recordio, Packed) {
  auto r = NewRecordIOReader("lib/recordio/testdata/test.grail-rpk");
  CheckContents(r.get());
}

TEST(Recordio, PackedGz) {
  auto r = NewRecordIOReader("lib/recordio/testdata/test.grail-rpk-gz");
  CheckContents(r.get());
}

TEST(Recordio, Error) {
  auto r = NewRecordIOReader("/non/existent/file");
  EXPECT_FALSE(r->Scan());
  EXPECT_THAT(r->Error(), ::testing::HasSubstr("No such file or directory"));
}

TEST(Recordio, CompressTransformers) {
  const std::string str =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::string err("");

  auto compressor = CompressRecordIOTransformer();
  auto compressed = compressor->Transform(
      RecordIOSpan{str.data(), str.size()}, &err);
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
