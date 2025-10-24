#include "TabularData/TabularData.hpp"
#include <iostream>
#include <cstdint>
#include <fstream>
#include <iostream>

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <csv-file> <output-dir>\n";
        return 1;
    }

    tabular::TabularData data(argv[1], argv[2]);
    data.skipFaultyRows(false);
    data.parseHeaderRow();
        std::cout << "Total columns: " << data.getColumnCount() << "\n";
    std::cout << "Total columns (CC): " << data.getCCcount() << "\n";
    data.findRowOffsets();
    std::cout<<"Total Rows :" << data.getRowCount() << "\n";

    for (int i = 0; i < 5; ++i) {
        try {
            std::cout << "Header[" << i << "]: " << data.getHeader(i) << "\n";
        } catch (std::exception& e) {
            std::cerr << "Error retrieving header for column " << i << ": " << e.what() << "\n";
        }
    }
       std::ifstream csv("../phd_research/darkome/tests/data_sets/alldata_merged.csv", std::ios::binary);
    std::ifstream offs("output/new/row_offsets.bin", std::ios::binary);
    if (!csv || !offs) return 1;

    std::uint64_t offset[10];
    std::size_t n = 0;
    while (n < 10 && offs.read(reinterpret_cast<char*>(&offset[n]), sizeof(std::uint64_t))) {
        ++n;
    }
    //print offsets
    for (int i = 0; i < n; i++) {
        std::cout << "Offset[" << i << "] = " << offset[i] << "\n";
    }
    char buf[10];
    for (std::size_t i = 0; i < n; ++i) {
        csv.clear();
        csv.seekg(static_cast<std::streamoff>(offset[i]), std::ios::beg);
        csv.read(buf, 10);
        std::cout.write(buf, csv.gcount());
        std::cout << '\n';
    }
    csv.close();
    offs.close();
    data.mapIntTranspose();

    // const std::string path = (argc > 1) ? argv[1] : "column_chunk_meta.bin";
    const std::string path = "output/new/column_chunk_meta.bin";
    // Print file size (bytes) and expected pair count
    std::error_code ec;
    auto sz = std::filesystem::file_size(path, ec);
    if (ec) {
        std::cerr << "Failed to stat file: " << path << " (" << ec.message() << ")\n";
        return 1;
    }
    std::cout << "File: " << path << "\n"
              << "Size (bytes): " << sz << "\n"
              << "Pairs (u32,u32): " << (sz / 8) << (sz % 8 ? "  [WARNING: trailing bytes]" : "") << "\n";

    std::ifstream in(path, std::ios::binary);
    if (!in) {
        std::cerr << "Cannot open " << path << "\n";
        return 1;
    }

    std::uint32_t a, b;
    std::uint32_t count = 0;
    while (true) {
        if (!in.read(reinterpret_cast<char*>(&a), sizeof(a))) break;
        if (!in.read(reinterpret_cast<char*>(&b), sizeof(b))) break; // handles odd trailing bytes
        count += a;
        std::cout << a << " " << b << "\n";
    }
    std::cout << "Total count: " << count << "\n";

    return 0;
}
