#include "util/block.h"
#include <stdint.h>
#include <gtest/gtest.h>

using namespace yodb;

TEST(Block, RW)
{
    char buf[1024];
    Block block(Slice(buf, 1024));
    BlockWriter writer(block);

    bool v = true;
    uint8_t u8 = 1;
    uint16_t u16 = 1000;
    uint32_t u32 = 100000;
    uint64_t u64 = 100000000;

    writer << v << u8 << u16 << u32 << u64;
    EXPECT_TRUE(writer.ok());

    Slice s1("abcedf");
    Slice s2("cdefgh");

    writer << s1 << s2;
    EXPECT_TRUE(writer.ok());

    bool rv;
    uint8_t ru8;
    uint16_t ru16;
    uint32_t ru32;
    uint64_t ru64;

    BlockReader reader(block);

    reader >> rv >> ru8 >> ru16 >> ru32 >> ru64;
    EXPECT_TRUE(reader.ok());
    EXPECT_EQ(rv, v);
    EXPECT_EQ(ru8, u8);
    EXPECT_EQ(ru16, u16);
    EXPECT_EQ(ru32, u32);
    EXPECT_EQ(ru64, u64);

    Slice s3, s4;
    reader >> s3 >> s4;
    EXPECT_EQ(s1, s3);
    EXPECT_EQ(s2, s4);
}
