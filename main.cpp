#include "TabularData.h"
#include <iostream>

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>

using namespace std;



int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " input.csv [output_directory]\n";
        return 1;
    }

    string input  = argv[1];
    string output = (argc > 2) ? argv[2] : "output";

    TabularData t;
    t.setOutputDirectory(output);
    if (!t.parseHeaderFromCsv(input)) {
        cerr << "Failed to parse header from " << input << "\n";
        return 1;
    }
    cout << t.getColumnIndex("quote") << endl; // example usage
    cout << t.getColumnHeader(3) << endl;    // example usage
    cout << "Header parsed successfully. Columns detected: "
              << t.columnCount() << "\n";
    cout << "Column headers written to: " << output << "\n";
    t.findNewLineOffsets();
    cout << "Number of lines in CSV: " << t.totalLines() << "\n";

    const std::string binPath = "output/row_byte_offsets.bin";  // adjust if you write elsewhere
    const std::string csvPath = "../tests/sample_csv/trees.csv";             // your CSV file

    std::ifstream bin(binPath, std::ios::binary);
    std::ifstream csv(csvPath, std::ios::binary);

    if (!bin || !csv) {
        std::cerr << "Could not open files\n";
        return 1;
    }

    std::uint64_t offset;
    int idx = 0;
    while (bin.read(reinterpret_cast<char*>(&offset), sizeof(std::uint64_t))) {
        csv.clear();
        csv.seekg(static_cast<std::streamoff>(offset), std::ios::beg);

        char buf[21] = {0}; // 20 chars + terminator
        csv.read(buf, 20);
        std::streamsize got = csv.gcount();
        buf[got] = '\0';

        std::cout << "Row " << idx
                  << " offset=" << offset
                  << " snippet=\"" << buf << "\"\n";

        ++idx;
    }

}
