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
        } catch (...) {
            break;
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

    return 0;
}
