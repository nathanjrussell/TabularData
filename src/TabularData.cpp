#include "TabularData/TabularData.hpp"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

namespace tabular {

static constexpr const char* kHeaderIndexFileName = "header_string_lookup_offsets.bin";

TabularData::TabularData(std::string csvPath, std::string outputDir)
    : _csvPath(std::move(csvPath)), _outputDir(std::move(outputDir)) {
    if (_csvPath.empty())  throw std::invalid_argument("csvPath is empty");
    if (_outputDir.empty()) throw std::invalid_argument("outputDir is empty");

    fs::create_directories(_outputDir);
    _headersbinFilePath = (fs::path(_outputDir) / kHeaderIndexFileName).string();
}

void TabularData::parseHeaderRow() {
    this->colCount = 0;
    std::ifstream in(_csvPath, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV file: " + _csvPath);

    std::ofstream binFile(_headersbinFilePath, std::ios::binary | std::ios::trunc);
    if (!binFile) throw std::runtime_error("Failed to open headers index file: " + _headersbinFilePath);

#ifndef CHUNK_SIZE
#define CHUNK_SIZE 1048576 // 1 MiB default if not provided by CMake
#endif
    std::vector<char> buf(CHUNK_SIZE);

    // State across chunk boundaries
    bool inQuotes = false;       // currently inside a quoted field
    bool atFieldStart = true;    // cursor is at start of a field (no content seen yet)
    bool pendingQuote = false;   // saw a quote while inQuotes; need next char to resolve
    bool headerDone = false;     // header row fully parsed

    u64 pos = 0;                 // absolute byte offset from start of file
    u64 fieldStart = 0;          // offset of first content byte (excludes opening quote)
    u64 lastContent = 0;         // offset of last content byte seen

    auto write_pair = [&](u64 start, u64 end) {
        binFile.write(reinterpret_cast<const char*>(&start), sizeof(u64));
        binFile.write(reinterpret_cast<const char*>(&end),   sizeof(u64));
    };

    auto close_field = [&]() {
        // If empty, write end < start by using start-1 (consumer treats as empty)
        const u64 end = (lastContent >= fieldStart) ? lastContent : (fieldStart - 1);
        write_pair(fieldStart, end);
    };

    while (!headerDone) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        const std::streamsize got = in.gcount();
        if (got <= 0) {
            // EOF reached: if we were in the middle of a field, close it.
            if (!atFieldStart) close_field();
            break;
        }

        for (std::streamsize i = 0; i < got && !headerDone; ++i, ++pos) {
            const char c = buf[static_cast<size_t>(i)];

            if (inQuotes) {
                if (pendingQuote) {
                    if (c == '"') {
                        // Escaped quote ("") -> counts as content (the second quote is content byte)
                        lastContent = pos;
                        pendingQuote = false;
                        continue;
                    } else {
                        // Previous quote ended the quoted section
                        inQuotes = false;
                        pendingQuote = false;
                        // Reprocess this character as non-quoted
                        --i; --pos;
                        continue;
                    }
                }

                if (c == '"') {
                    // Could be end of quoted field or start of escaped quote; decide next byte
                    pendingQuote = true;
                } else {
                    // Any byte inside quotes (including commas and newlines) is content
                    lastContent = pos;
                }
            } else {
                // Not in quotes
                if (atFieldStart) {
                    if (c == '"') {
                        inQuotes = true;
                        atFieldStart = false;
                        fieldStart = pos + 1;   // skip the opening quote
                        pendingQuote = false;
                        continue;
                    } else {
                        atFieldStart = false;
                        fieldStart = pos;       // first content byte (may end up empty if immediately comma/newline)
                        if (c != ',' && c != '\r' && c != '\n') {
                            lastContent = pos;
                        }
                        // fall-through to delimiter checks
                    }
                }

                if (c == ',') {
                    // End of current header field
                    close_field();
                    atFieldStart = true;
                    colCount++;
                    continue;
                } else if (c == '\n') {
                    // LF terminates header row
                    close_field();
                    headerDone = true;
                    colCount++;
                    continue;
                } else if (c == '\r') {
                    // CR (possibly CRLF) terminates header row
                    close_field();
                    headerDone = true;
                    colCount++;
                    continue;
                } else {
                    // Regular content in unquoted field
                    lastContent = pos;
                }
            }
        }
    }
}

std::pair<TabularData::u64, TabularData::u64>
TabularData::readPair(std::size_t colNum) const {
    std::ifstream binFile(_headersbinFilePath, std::ios::binary);
    if (!binFile) throw std::runtime_error("Missing headers index file. Run parseHeaderRow() first.");

    const std::size_t stride = sizeof(u64) * 2;
    binFile.seekg(static_cast<std::streamoff>(colNum * stride), std::ios::beg);

    u64 start = 0, end = 0;
    binFile.read(reinterpret_cast<char*>(&start), sizeof(u64));
    binFile.read(reinterpret_cast<char*>(&end),   sizeof(u64));

    if (!binFile) throw std::out_of_range("Column index out of range or corrupted index file");
    return {start, end};
}

std::string TabularData::unescapeCsvField(std::string_view raw) {
    // raw: exact bytes of the field content (no surrounding quotes); "" inside quoted fields -> "
    std::string out;
    out.reserve(raw.size());
    for (size_t i = 0; i < raw.size(); ++i) {
        char ch = raw[i];
        if (ch == '"' && (i + 1) < raw.size() && raw[i + 1] == '"') {
            out.push_back('"');
            ++i; // skip the second quote
        } else {
            out.push_back(ch);
        }
    }
    return out;
}

static inline std::string trim(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;

    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;

    return s.substr(start, end - start);
}

std::string TabularData::getHeader(std::size_t colNum) const {
    auto [start, end] = readPair(colNum);
    if (end < start) return std::string(); // empty header

    std::ifstream in(_csvPath, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV file: " + _csvPath);

    const u64 len = end - start + 1;
    std::string buffer;
    buffer.resize(static_cast<size_t>(len));

    in.seekg(static_cast<std::streamoff>(start), std::ios::beg);
    in.read(buffer.data(), static_cast<std::streamsize>(len));
    if (!in) throw std::runtime_error("Failed to read header slice from CSV");

    // Unescape doubled quotes in the content slice
    return trim(unescapeCsvField(buffer));
}

const TabularData::u64 TabularData::getColumnCount() const {
    //get file size
    std::ifstream binFile(_headersbinFilePath, std::ios::binary | std::ios::ate);
    if (!binFile) throw std::runtime_error("Missing headers index file.");
    const auto fileSize = binFile.tellg();
    if (fileSize < 0) throw std::runtime_error("Failed to get size of headers index file.");
    return static_cast<TabularData::u64>(fileSize) / (sizeof(TabularData::u64) * 2);
}

} // namespace tabular
