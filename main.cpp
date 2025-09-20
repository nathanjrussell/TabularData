#include "TabularData.h"
#include <iostream>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " input.csv [output.json]\n";
        return 1;
    }

    std::string input  = argv[1];
    std::string output = (argc > 2) ? argv[2] : "column_headers.json";

    TabularData t;
    if (!t.parseHeaderFromCsv(input, output)) {
        std::cerr << "Failed to parse header from " << input << "\n";
        return 1;
    }

    std::cout << "Header parsed successfully. Columns detected: "
              << t.columnCount() << "\n";
    std::cout << "Column headers written to: " << output << "\n";
}
