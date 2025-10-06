#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <utility>

namespace tabular {

class TabularData {
public:
    int colCount = -1;
    using u32 = std::uint32_t;
    using u16 = std::uint16_t;

    TabularData(std::string csvPath, std::string outputDir);

    void parseHeaderRow();

    std::string getHeader(std::size_t colNum) const;

    const std::string& csvPath()   const { return _csvPath;   }
    const std::string& outputDir() const { return _outputDir; }
    const u32 getColumnCount() const;
    const u32 getCCcount() const { return colCount; }

private:
    std::pair<u32,u16> readPair(std::size_t colNum) const;

    // Helper: replace doubled quotes ("") → ("), used by getHeader()
    static std::string unescapeCsvField(std::string_view raw);

    std::string _csvPath;
    std::string _outputDir;
    std::string _headersbinFilePath;
};

} // namespace tabular
