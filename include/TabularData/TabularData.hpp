#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <utility>

namespace tabular {

class TabularData {
public:
    int colCount = -1;
    using u64 = std::uint64_t;

    TabularData(std::string csvPath, std::string outputDir);

    void parseHeaderRow();

    std::string getHeader(std::size_t colNum) const;

    const std::string& csvPath()   const { return _csvPath;   }
    const std::string& outputDir() const { return _outputDir; }
    const u64 getColumnCount() const;
    const u64 getCCcount() const { return colCount; }

private:
    std::pair<u64,u64> readPair(std::size_t colNum) const;

    // Helper: replace doubled quotes ("") → ("), used by getHeader()
    static std::string unescapeCsvField(std::string_view raw);

    std::string _csvPath;
    std::string _outputDir;
    std::string _headersbinFilePath;
};

} // namespace tabular
