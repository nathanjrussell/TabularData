#include <gtest/gtest.h>
#include "TabularData/TabularData.hpp"
#include <filesystem>

namespace fs = std::filesystem;
using tabular::TabularData;

TEST(HeaderTest, SimpleCsv) {
    fs::path csv = fs::path("tests/sample_csv/homes.csv");
    fs::path outdir = fs::temp_directory_path() / "tabular_meta";
    fs::create_directories(outdir);

    TabularData td(csv.string(), outdir.string());
    td.parseHeaderRow();

    EXPECT_EQ(td.getHeader(0), "Sell");
    EXPECT_EQ(td.getHeader(1), "List");
    EXPECT_EQ(td.getHeader(2), "Living");
    EXPECT_EQ(td.getHeader(3), "Rooms");
    EXPECT_EQ(td.getHeader(4), "Beds");
    EXPECT_EQ(td.getHeader(5), "Baths");
    EXPECT_EQ(td.getHeader(6), "Age");
    EXPECT_EQ(td.getHeader(7), "Acres");
    EXPECT_EQ(td.getHeader(8), "Taxes");
}

