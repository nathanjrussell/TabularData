#ifndef TABULARDATA_H
#define TABULARDATA_H

#include <string>
#include <vector>
#include <ostream>

#ifndef TABULARDATA_NUM_THREADS
#define TABULARDATA_NUM_THREADS 4
#endif
#ifndef TABULARDATA_MAX_BUFFER_BYTES
#define TABULARDATA_MAX_BUFFER_BYTES (1 << 20)
#endif

class TabularData {
public:
    struct CsvOptions {
        char delimiter = ',';
        char quote = '"';
        bool rfc4180 = true;
        bool has_header = true;
    };

    TabularData() = default;

    // Overload with explicit options (NO default here)
    bool parseHeaderFromCsv(const std::string& csv_path,
                            const std::string& out_json_path,
                            const CsvOptions& opt);

    // Convenience overload that uses default options (and default out path)
    bool parseHeaderFromCsv(const std::string& csv_path,
                            const std::string& out_json_path = "column_headers.json");

    size_t columnCount() const noexcept { return num_columns_; }
    const std::vector<std::string>& row() const { return current_row_; }

private:
    size_t num_columns_ = 0;
    std::vector<std::string> current_row_;

    static void writeJsonEscapedChar(std::ostream& os, unsigned char ch);

    TabularData(const TabularData&) = delete;
    TabularData& operator=(const TabularData&) = delete;
};



#endif // TABULARDATA_H