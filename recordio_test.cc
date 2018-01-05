#include <gtest/gtest.h>

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

TEST(Recordio, Read) {
  std::ifstream in("lib/recordio/testdata/test.recordio");
  ASSERT_FALSE(in.fail());
  auto r = NewRecordIOReader(&in);
  const std::string str =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  const int record_size = 8;

  int n = 0;
  while (r->Scan()) {
    const int start_index = n % (str.size() - record_size + 1);
    const std::string expected = str.substr(start_index, record_size);
    EXPECT_EQ(expected, Str(r.get()));
    EXPECT_EQ("", r->Error());
    n++;
  }
  EXPECT_EQ(128, n);
  in.close();
}

}  // namespace grail
