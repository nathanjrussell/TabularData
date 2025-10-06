#include "TabularData/TabularData.hpp"
#include <iostream>

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <csv-file> <output-dir>\n";
        return 1;
    }

    tabular::TabularData data(argv[1], argv[2]);
    data.parseHeaderRow();

    for (int i = 0; i < 5; ++i) {
        try {
            std::cout << "Header[" << i << "]: " << data.getHeader(i) << "\n";
        } catch (...) {
            break;
        }
    }
    std::cout << "Total columns: " << data.getColumnCount() << "\n";
    std::cout << "Total columns (CC): " << data.getCCcount() << "\n";
    return 0;
}
