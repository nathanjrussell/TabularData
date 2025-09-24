#pragma once

#include <string>
#include <ostream>
#include <filesystem>

/**
* Configurable options for TabularData parsing and output.
*/
#ifndef TABULARDATA_NUM_THREADS
#define TABULARDATA_NUM_THREADS 4
#endif

#ifndef TABULARDATA_MAX_BUFFER_BYTES
#define TABULARDATA_MAX_BUFFER_BYTES (1 << 20) // 1 MiB
#endif

#ifndef TABULARDATA_COL_HEADERS_BIN
#define TABULARDATA_COL_HEADERS_BIN "col_headers_lookup_offsets.bin"
#endif


/**
 * Class for parsing and storing tabular data from CSV files too large to fit into memory.
 */
class TabularData {
public:
    struct CsvOptions {
        char delimiter = ',';   // field delimiter
        char quote     = '"';   // quote character
        bool rfc4180   = true;  // potential less strict standard compliance
        bool has_header = true; // csv has a header row
    };

    TabularData() = default;

    bool findNewLineOffsets();
    std::uint64_t totalLines() const noexcept { return total_lines_; }

    void setOutputDirectory(const std::string& dir);

    bool parseHeaderFromCsv(const std::string& csv_path);
    bool parseHeaderFromCsv(const std::string& csv_path, const CsvOptions&);

    size_t columnCount() const noexcept { return num_columns_; }


    // Returns 0-based index, or -1 if not found.
    int getColumnIndex(const std::string& name) const;

    // Get the raw header string for a given column index (0-based).
    // Returns empty string on error or out of range.
    std::string getColumnHeader(int columnIndex) const;

private:
    size_t num_columns_ = 0;
    std::uint64_t total_lines_ = 0;
    // File paths (set after parse)
    std::string csv_path_;          // original CSV path
    std::string output_directory_;  // directory where .bin files are written
    std::string bin_path_;          // col_headers_lookup_offsets.bin path

    static void writeJsonEscapedChar(std::ostream& os, unsigned char ch);

    // Helper to construct the default .bin file path
    static inline std::string defaultBinPath(const std::string& out_dir) {
        return out_dir + "/" + TABULARDATA_COL_HEADERS_BIN;
    }

    // Not copyable
    TabularData(const TabularData&) = delete;
    TabularData& operator=(const TabularData&) = delete;
};
