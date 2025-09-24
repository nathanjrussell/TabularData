#include <gtest/gtest.h>
#include "TabularData.h"
#include <filesystem>
#include <string>

#ifndef PROJECT_ROOT_DIR
#define PROJECT_ROOT_DIR "."
#endif

TEST(TabularDataTest, ParsesSimpleHeader) {
    std::string path = std::string(PROJECT_ROOT_DIR) + "/tests/sample_csv/simple.csv";
    std::string outdir = std::string(PROJECT_ROOT_DIR) + "/tests/output";
    //delete existing output directory if it exists
    if (std::filesystem::exists(outdir)) {
        std::filesystem::remove_all(outdir);
    }

    TabularData t;
    t.setOutputDirectory(outdir);
    ASSERT_TRUE(t.parseHeaderFromCsv(path));
    EXPECT_EQ(t.columnCount(), 5u);
    EXPECT_EQ(t.getColumnHeader(0), "id");
    EXPECT_EQ(t.getColumnHeader(1), "name");
    EXPECT_EQ(t.getColumnHeader(2), "quote");
    EXPECT_EQ(t.getColumnHeader(3), "notes");
    EXPECT_EQ(t.getColumnHeader(4), "extra");
    EXPECT_EQ(t.getColumnIndex("id"), 0);
    EXPECT_EQ(t.getColumnIndex("name"), 1);
    EXPECT_EQ(t.getColumnIndex("quote"), 2);
    EXPECT_EQ(t.getColumnIndex("notes"), 3);
    EXPECT_EQ(t.getColumnIndex("extra"), 4);
    EXPECT_EQ(t.getColumnIndex("nonexistent"), -1);

}

TEST(TabularDataTest, ParsesQuotedHeader) {
    std::string path = std::string(PROJECT_ROOT_DIR) + "/tests/sample_csv/homes.csv";
    std::string outdir = std::string(PROJECT_ROOT_DIR) + "/tests/output";
    if (std::filesystem::exists(outdir)) {
        std::filesystem::remove_all(outdir);
    }

    TabularData t;
    t.setOutputDirectory(outdir);
    ASSERT_TRUE(t.parseHeaderFromCsv(path));
    EXPECT_EQ(t.columnCount(), 9u);
    EXPECT_EQ(t.getColumnHeader(0), "Sell");
    EXPECT_EQ(t.getColumnHeader(1), "List");
    EXPECT_EQ(t.getColumnHeader(2), "Living");
    EXPECT_EQ(t.getColumnHeader(3), "Rooms");
    EXPECT_EQ(t.getColumnHeader(4), "Beds");
    EXPECT_EQ(t.getColumnHeader(5), "Baths");
    EXPECT_EQ(t.getColumnHeader(6), "Age");
    EXPECT_EQ(t.getColumnHeader(7), "Acres");
    EXPECT_EQ(t.getColumnHeader(8), "Taxes");
    EXPECT_EQ(t.getColumnIndex("Sell"), 0);
    EXPECT_EQ(t.getColumnIndex("List"), 1);
    EXPECT_EQ(t.getColumnIndex("Living"), 2);
    EXPECT_EQ(t.getColumnIndex("Rooms"), 3);
    EXPECT_EQ(t.getColumnIndex("Beds"), 4);
    EXPECT_EQ(t.getColumnIndex("Baths"), 5);
    EXPECT_EQ(t.getColumnIndex("Age"), 6);
    EXPECT_EQ(t.getColumnIndex("Acres"), 7);
    EXPECT_EQ(t.getColumnIndex("Taxes"), 8);
    EXPECT_EQ(t.getColumnIndex("nonexistent"), -1);
}

TEST (TabularDataTest, ParseSpaces) {
    std::string path = std::string(PROJECT_ROOT_DIR) + "/tests/sample_csv/trees.csv";
    std::string outdir = std::string(PROJECT_ROOT_DIR) + "/tests/output";
    if (std::filesystem::exists(outdir)) {
        std::filesystem::remove_all(outdir);
    }

    TabularData t;
    t.setOutputDirectory(outdir);
    ASSERT_TRUE(t.parseHeaderFromCsv(path));
    EXPECT_EQ(t.columnCount(), 4u);
    EXPECT_EQ(t.getColumnHeader(0), "Index");
    EXPECT_EQ(t.getColumnHeader(1), "Girth (in)");
    EXPECT_EQ(t.getColumnHeader(2), "Height (ft)");
    EXPECT_EQ(t.getColumnHeader(3), "Volume(ft^3)");
    EXPECT_EQ(t.getColumnIndex("Index"), 0);
    EXPECT_EQ(t.getColumnIndex("Girth (in)"), 1);
    EXPECT_EQ(t.getColumnIndex("Height (ft)"), 2);
    EXPECT_EQ(t.getColumnIndex("Volume(ft^3)"), 3);
    EXPECT_EQ(t.getColumnIndex("nonexistent"), -1);
}


