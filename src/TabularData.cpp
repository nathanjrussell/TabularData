#include "TabularData/TabularData.hpp"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>
#include <thread>
#include <sstream>
#include <iomanip>
#include <map>
#include <iostream>
#include <cstring>   // memcpy
#include <algorithm> // max

namespace fs = std::filesystem;

namespace tabular {

#ifndef NUM_THREADS
#define NUM_THREADS 4
#endif

#ifndef CHUNK_SIZE
#define CHUNK_SIZE (1u<<20) // 1 MiB
#endif

#ifndef HEADER_INDEX_FILE_NAME
#define HEADER_INDEX_FILE_NAME "header_string_lookup_offsets.bin"
#endif

#ifndef COLUMNS_PER_CHUNK
#define COLUMNS_PER_CHUNK 100000
#endif

static constexpr const char* kHeaderIndexFileName = "header_string_lookup_offsets.bin";
    static const std::vector<std::string> kOutputSubdirs = {"jsonData"};

// ------------------------- ctor & simple toggles -------------------------
TabularData::TabularData(std::string csvPath, std::string outputDir)
    : TabularData(std::move(csvPath), std::move(outputDir), true) {}

TabularData::TabularData(std::string csvPath, std::string outputDir, bool createStandAloneFiles = true)
    : _csvPath(std::move(csvPath)), _outputDir(std::move(outputDir)) {
    if (_csvPath.empty())  throw std::invalid_argument("csvPath is empty");
    if (_outputDir.empty()) throw std::invalid_argument("outputDir is empty");
    this->createStandAloneDataFiles = createStandAloneFiles;

    fs::create_directories(_outputDir);
    for (const auto &name : kOutputSubdirs) {
        if (name.empty()) continue;
        fs::create_directories(fs::path(_outputDir) / name);
    }
    _headersbinFilePath = (fs::path(_outputDir) / kHeaderIndexFileName).string();
}

void TabularData::skipFaultyRows(bool skip) { this->skipRows = skip; }

// ------------------------------ header parse -----------------------------

    static inline std::string trim(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;

    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;

    return s.substr(start, end - start);
}

void TabularData::createHeaderJSON() {
    std::ofstream jsonFile((fs::path(_outputDir) / "headers.json").string(), std::ios::trunc);
    //new offset index for quick access from JSON file
    std::ofstream JSONIndexFile((fs::path(_outputDir) / "headers_json_index.bin").string(), std::ios::binary | std::ios::trunc);
    if (!jsonFile) throw std::runtime_error("Failed to open headers.json for writing");

    const u32 colCount = getColumnCount();
    //track bytes written to json file to create new header offset index for quick access from JSON file
    jsonFile << "[\n";
    u32 bytesWritten = 2; // account for "[\n"
    for (u32 col = 0; col < colCount; ++col) {
        auto [start, end] = readPair(col);
        if (end < start) continue;

        std::ifstream in(_csvPath, std::ios::binary);
        if (!in) throw std::runtime_error("Failed to open CSV file: " + _csvPath);

        const u32 len = end - start + 1;
        std::string buffer(len, '\0');

        in.seekg(static_cast<std::streamoff>(start), std::ios::beg);
        in.read(buffer.data(), static_cast<std::streamsize>(len));
        if (!in) throw std::runtime_error("Failed to read header slice from CSV");

        in.close();
        std::string headerStr = unescapeCsvField(buffer);
        //trim whitespace
        headerStr = trim(headerStr);

        //write current byte offset to JSON index file
        JSONIndexFile.write(reinterpret_cast<const char*>(&bytesWritten), sizeof(u32));
        u16 headerLen = static_cast<u16>(headerStr.size());
        //write header length to JSON index file
        JSONIndexFile.write(reinterpret_cast<const char*>(&headerLen), sizeof(u16));

        jsonFile << headerStr;
        bytesWritten += static_cast<u32>(headerStr.size());
        if (col + 1 < colCount) {
            jsonFile << ",\n";
            bytesWritten += 2; // account for ",\n"
        }
    }
    jsonFile << "\n]\n";
    jsonFile.close();
    JSONIndexFile.close();
}




void TabularData::parseHeaderRow() {
    this->colCount = 0;

    std::ifstream in(_csvPath, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV file: " + _csvPath);

    std::ofstream binFile(_headersbinFilePath, std::ios::binary | std::ios::trunc);
    if (!binFile) throw std::runtime_error("Failed to open headers index file: " + _headersbinFilePath);

    std::vector<char> buf(CHUNK_SIZE);

    bool inQuotes = false;
    bool atFieldStart = true;
    bool pendingQuote = false;
    bool headerDone = false;

    u32 pos = 0;
    u32 fieldStart = 0;
    u32 lastContent = 0;

    auto close_field = [&]() {
        const u16 length = (lastContent >= fieldStart) ? lastContent : (fieldStart - 1);
        binFile.write(reinterpret_cast<const char*>(&fieldStart), sizeof(u32));
        binFile.write(reinterpret_cast<const char*>(&length),     sizeof(u16));
    };

    while (!headerDone) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        const std::streamsize got = in.gcount();
        if (got <= 0) {
            if (!atFieldStart) close_field();
            break;
        }

        for (std::streamsize i = 0; i < got && !headerDone; ++i, ++pos) {
            const char c = buf[static_cast<size_t>(i)];

            if (inQuotes) {
                if (pendingQuote) {
                    if (c == '"') { lastContent = pos; pendingQuote = false; continue; }
                    inQuotes = false; pendingQuote = false; --i; --pos; continue;
                }
                if (c == '"') { pendingQuote = true; }
                else { lastContent = pos; }
            } else {
                if (atFieldStart) {
                    if (c == '"') { inQuotes = true; atFieldStart = false; fieldStart = pos + 1; pendingQuote = false; continue; }
                    atFieldStart = false;
                    fieldStart = pos;
                    if (c != ',' && c != '\r' && c != '\n') lastContent = pos;
                }

                if (c == ',') { close_field(); atFieldStart = true; colCount++; continue; }
                if (c == '\n') { close_field(); headerDone = true; colCount++; continue; }
                if (c == '\r') { close_field(); headerDone = true; colCount++; continue; }
                lastContent = pos;
            }
        }
    }

    in.close();
    binFile.close();
}

// ---------------------------- header accessors ---------------------------

std::pair<TabularData::u32, TabularData::u16>
TabularData::readPair(std::size_t colNum) const {
    std::ifstream binFile(_headersbinFilePath, std::ios::binary);
    if (!binFile) throw std::runtime_error("Missing headers index file. Run parseHeaderRow() first.");

    const std::size_t stride = sizeof(u32) + sizeof(u16);
    binFile.seekg(static_cast<std::streamoff>(colNum * stride), std::ios::beg);

    u32 start = 0;
    u16 end = 0;
    binFile.read(reinterpret_cast<char*>(&start), sizeof(u32));
    binFile.read(reinterpret_cast<char*>(&end),   sizeof(u16));

    if (!binFile) throw std::out_of_range("Column index out of range or corrupted index file");
    binFile.close();
    return {start, end};
}

std::string TabularData::unescapeCsvField(std::string_view raw) {
    std::string out;
    out.reserve(raw.size());
    for (size_t i = 0; i < raw.size(); ++i) {
        char ch = raw[i];
        if (ch == '"' && (i + 1) < raw.size() && raw[i + 1] == '"') { out.push_back('"'); ++i; }
        else { out.push_back(ch); }
    }
    return out;
}



std::string TabularData::getHeader(std::size_t colNum) const {
    auto [start, end] = readPair(colNum);
    if (end < start) return std::string();

    std::ifstream in(_csvPath, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV file: " + _csvPath);

    const u32 len = end - start + 1;
    std::string buffer(len, '\0');

    in.seekg(static_cast<std::streamoff>(start), std::ios::beg);
    in.read(buffer.data(), static_cast<std::streamsize>(len));
    if (!in) throw std::runtime_error("Failed to read header slice from CSV");

    in.close();
    return trim(unescapeCsvField(buffer));
}

const TabularData::u32 TabularData::getColumnCount() const {
    std::ifstream binFile(_headersbinFilePath, std::ios::binary | std::ios::ate);
    if (!binFile) throw std::runtime_error("Missing headers index file.");
    const auto fileSize = binFile.tellg();
    if (fileSize < 0) throw std::runtime_error("Failed to get size of headers index file.");
    binFile.close();
    return static_cast<TabularData::u32>(fileSize) / (sizeof(TabularData::u32) + sizeof(TabularData::u16));
}

const TabularData::u32 TabularData::getRowCount() const { return this->rowCount; }

// ============================== helpers =================================

namespace {

using u64 = std::uint64_t;

struct CsvState {
    bool inQuotes      = false;
    bool pendingQuote  = false; // saw " while inQuotes; next char decides
};

inline bool feed_csv(char c, CsvState &st) {
    if (st.inQuotes) {
        if (st.pendingQuote) {
            st.pendingQuote = false;
            if (c == '"') { return false; } // "" => literal quote, stay in quotes
            st.inQuotes = false;            // end quotes; reprocess this char unquoted
            return feed_csv(c, st);
        }
        if (c == '"') { st.pendingQuote = true; return false; }
        return false;
    }
    if (c == '"') { st.inQuotes = true; st.pendingQuote = false; return false; }
    if (c == '\n' || c == '\r') return true; // row terminator when unquoted
    return false;
}

inline int maybe_consume_LF_after_CR(std::ifstream &in) {
    int next = in.peek();
    if (next == '\n') { in.get(); return 1; }
    return 0;
}

inline u64 file_size_bytes(const std::string &path) {
    return static_cast<u64>(fs::file_size(fs::path(path)));
}

// Find offset of the first byte AFTER the header row terminator.
u64 find_first_data_offset(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV: " + path);

    std::vector<char> buf(CHUNK_SIZE);
    CsvState st;
    u64 pos = 0;

    while (true) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = in.gcount();
        if (got <= 0) break;

        for (std::streamsize i = 0; i < got; ++i, ++pos) {
            char c = buf[static_cast<size_t>(i)];
            if (feed_csv(c, st)) {
                u64 nextStart = pos + 1;
                if (c == '\r') {
                    if (i + 1 < got && buf[static_cast<size_t>(i + 1)] == '\n') { ++i; ++pos; ++nextStart; }
                    else { nextStart += maybe_consume_LF_after_CR(in); pos += (nextStart - (pos + 1)); }
                }
                in.close();
                return nextStart;
            }
        }
    }
    in.close();
    return pos; // header without newline
}

// Robust resync from arbitrary offset S to the first byte of the NEXT row.
u64 resync_to_next_row_start(const std::string &path, u64 S) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV for resync");
    u64 fsize = file_size_bytes(path);
    if (S >= fsize) { in.close(); return fsize; }

    in.seekg(static_cast<std::streamoff>(S), std::ios::beg);
    u64 pos = S;
    CsvState st;

    {   // one-shot disambiguation at S
        int c0i = in.get();
        if (c0i == EOF) { in.close(); return pos; }
        char c0 = static_cast<char>(c0i);
        ++pos;

        if (c0 == '"') {
            int n1 = in.peek();
            if (n1 == ',' || n1 == '\n' || n1 == '\r' || n1 == EOF) {
                if (n1 == ',') { in.get(); ++pos; }
                else if (n1 == '\n') { in.get(); ++pos; in.close(); return pos; }
                else if (n1 == '\r') { in.get(); ++pos; pos += maybe_consume_LF_after_CR(in); in.close(); return pos; }
                else { in.close(); return fsize; }
            } else if (n1 == '"') {
                in.get(); ++pos;
                int n2 = in.peek();
                if (n2 == ',' || n2 == '\n' || n2 == '\r' || n2 == EOF) {
                    if (n2 == ',') { in.get(); ++pos; }
                    else if (n2 == '\n') { in.get(); ++pos; in.close(); return pos; }
                    else if (n2 == '\r') { in.get(); ++pos; pos += maybe_consume_LF_after_CR(in); in.close(); return pos; }
                    else { in.close(); return fsize; }
                } else {
                    st.inQuotes = true; // "" then data => literal quote inside quoted
                }
            } else {
                st.inQuotes = true; // opening quote
            }
        }
    }

    std::vector<char> buf(CHUNK_SIZE);
    while (true) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = in.gcount();
        if (got <= 0) { in.close(); return pos; }

        for (std::streamsize i = 0; i < got; ++i, ++pos) {
            char c = buf[static_cast<size_t>(i)];
            if (feed_csv(c, st)) {
                u64 nextStart = pos + 1;
                if (c == '\r') {
                    if (i + 1 < got && buf[static_cast<size_t>(i + 1)] == '\n') { ++i; ++pos; ++nextStart; }
                    else { nextStart += maybe_consume_LF_after_CR(in); pos += (nextStart - (pos + 1)); }
                }
                in.close();
                return nextStart;
            }
        }
    }
}

// Parse [start, stop) and write row-start offsets to a binary file.
// Also validates row width and optionally skips faulty rows.
void parse_slice_to_file(const std::string &path,
                         u64 start, u64 stop,
                         const std::string &outPath,
                         tabular::TabularData::u32 expectedCols,
                         std::uint32_t *rowsOut,
                         bool skipFaultyRows)
{
    *rowsOut = 0;

    std::ofstream out(outPath, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Failed to open output: " + outPath);
    if (start >= stop) return;

    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV in worker");
    in.seekg(static_cast<std::streamoff>(start), std::ios::beg);

    std::vector<char> buf(CHUNK_SIZE);
    CsvState st;
    u64 pos = start;

    u64 currentRowStart = start;
    std::uint32_t commaCount = 0;
    bool rowNotBlank = false;

    auto handle_row_end = [&](u64 nextStart) {
        if (!rowNotBlank) { // blank row -> ignore
            currentRowStart = nextStart;
            commaCount = 0; rowNotBlank = false; return;
        }

        std::uint32_t fields = commaCount + 1;
        if (fields != expectedCols) {
            if (skipFaultyRows) {
                // skip: do not emit, just reset
                currentRowStart = nextStart;
                commaCount = 0; rowNotBlank = false;
                return;
            }
            std::ostringstream msg;
            msg << "Column count mismatch at row starting offset " << currentRowStart
                << ": expected " << expectedCols << ", found " << fields;
            std::cerr << msg.str() << std::endl;
            std::terminate();
        }

        out.write(reinterpret_cast<const char*>(&currentRowStart), sizeof(u64));
        ++(*rowsOut);

        currentRowStart = nextStart;
        commaCount = 0; rowNotBlank = false;
    };

    while (true) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = in.gcount();
        if (got <= 0) break;

        for (std::streamsize i = 0; i < got; ++i, ++pos) {
            char c = buf[static_cast<size_t>(i)];

            if (!st.inQuotes) {
                if (c == ',') { ++commaCount; rowNotBlank = true; }
                else if (c == '"') { rowNotBlank = true; }
                else if (c != ' ' && c != '\t' && c != '\r' && c != '\n') { rowNotBlank = true; }
            }

            if (feed_csv(c, st)) {
                u64 nextStart = pos + 1;
                if (c == '\r') {
                    if (i + 1 < got && buf[static_cast<size_t>(i + 1)] == '\n') { ++i; ++pos; ++nextStart; }
                    else { nextStart += maybe_consume_LF_after_CR(in); pos += (nextStart - (pos + 1)); }
                }
                handle_row_end(nextStart);
                if (nextStart >= stop) return;
            }
        }
    }

    // EOF row without newline
    if (pos > currentRowStart) handle_row_end(pos);
}

} // end anon

// --------------------------- findRowOffsets ------------------------------

void TabularData::findRowOffsets() {
    const std::string prefix = (fs::path(_outputDir) / "row_offsets").string();

    const u64 fsize = file_size_bytes(_csvPath);
    if (fsize == 0) {
        for (int t = 0; t < NUM_THREADS; ++t) {
            std::ostringstream oss; oss << prefix << ".part-" << t << ".bin";
            std::ofstream(oss.str(), std::ios::binary | std::ios::trunc).close();
        }
        return;
    }

    const u64 firstData = find_first_data_offset(_csvPath);
    if (firstData >= fsize) {
        for (int t = 0; t < NUM_THREADS; ++t) {
            std::ostringstream oss; oss << prefix << ".part-" << t << ".bin";
            std::ofstream(oss.str(), std::ios::binary | std::ios::trunc).close();
        }
        return;
    }

    const u64 dataBytes = fsize - firstData;
    const u64 base = dataBytes / NUM_THREADS;
    const u64 rem  = dataBytes % NUM_THREADS;

    std::vector<u64> S(NUM_THREADS);
    {
        u64 cur = firstData;
        for (int t = 0; t < NUM_THREADS; ++t) {
            S[t] = cur;
            cur += base + (t < static_cast<int>(rem) ? 1 : 0);
        }
    }

    std::vector<u64> handoff(NUM_THREADS + 1);
    handoff[0] = firstData;
    handoff[NUM_THREADS] = fsize;

    std::vector<std::thread> disc;
    disc.reserve(NUM_THREADS - 1);
    for (int t = 1; t < NUM_THREADS; ++t) {
        disc.emplace_back([&, t]{
            handoff[t] = resync_to_next_row_start(_csvPath, S[t]);
        });
    }
    for (auto &th : disc) th.join();

    std::vector<std::thread> workers;
    uint32_t threadRowCounts[NUM_THREADS] = {0};
    workers.reserve(NUM_THREADS);
    for (int t = 0; t < NUM_THREADS; ++t) {
        std::ostringstream oss; oss << prefix << ".part-" << t << ".bin";
        const std::string outPath = oss.str();
        workers.emplace_back([&, t, outPath]{
            parse_slice_to_file(_csvPath, handoff[t], handoff[t + 1],
                                outPath, this->colCount,
                                &threadRowCounts[t], this->skipRows);
        });
    }
    for (auto &th : workers) th.join();

    this->rowCount = 0;
    for (int t = 0; t < NUM_THREADS; ++t) this->rowCount += threadRowCounts[t];

    // Merge + delete parts
    {
        const fs::path pfx = fs::path(_outputDir) / "row_offsets";
        const fs::path merged = fs::path(_outputDir) / "row_offsets.bin";

        std::ofstream out(merged, std::ios::binary | std::ios::trunc);
        if (!out) throw std::runtime_error("Failed to open merged output: " + merged.string());

        for (int t = 0; t < NUM_THREADS; ++t) {
            std::ostringstream oss; oss << pfx.string() << ".part-" << t << ".bin";
            std::ifstream in(oss.str(), std::ios::binary);
            if (!in) throw std::runtime_error("Missing part file: " + oss.str());
            out << in.rdbuf();
        }
        out.close();

        std::error_code ec;
        for (int t = 0; t < NUM_THREADS; ++t) {
            std::ostringstream oss; oss << pfx.string() << ".part-" << t << ".bin";
            fs::remove(oss.str(), ec);
            ec.clear();
        }
    }
}

// ======================== column chunk mapping ==========================

struct ColumnChunk {
    std::string   filePath;
    uint64_t*     rowOffsets = nullptr;  // invariant: start-of-row offsets
    uint64_t*     rowCursor  = nullptr;  // mutable per-row cursor (advances across chunks)
    int           rowCount   = 0;
    int           start      = 0;
    int           end        = 0;        // [start,end)
    int         **data       = nullptr;  // [ncols][rowCount]
    std::map<std::string,int> **localMaps = nullptr; // [NUM_THREADS][ncols]
};

// read up to maxTokensNeeded tokens from a row; advance *currentRowByteOffset
static std::vector<std::string>
getTokens(uint64_t* currentRowByteOffset, const std::string& filePath, int maxTokensNeeded) {
    std::vector<std::string> tokens;
    if (maxTokensNeeded <= 0) return tokens;

    std::ifstream in(filePath, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV file: " + filePath);

    const uint64_t startOff = *currentRowByteOffset;
    in.seekg(static_cast<std::streamoff>(startOff), std::ios::beg);
    if (!in) throw std::runtime_error("Failed to seek in CSV file");

    std::vector<char> buffer(CHUNK_SIZE);
    in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const int bytesRead = static_cast<int>(in.gcount());
    if (bytesRead <= 0) return tokens;

    bool inQuotes = false;
    bool pendingQuote = false;
    int tokenStart = 0;

    auto push_token = [&](int s, int e) {
        if (e < s) e = s;
        std::string tok(buffer.data() + s, e - s);
        // cheap trim
        size_t a = 0, b = tok.size();
        while (a < b && std::isspace((unsigned char)tok[a])) ++a;
        while (b > a && std::isspace((unsigned char)tok[b-1])) --b;
        tokens.emplace_back(tok.substr(a, b - a));
    };

    for (int i = 0; i < bytesRead; ++i) {
        char c = buffer[i];

        if (inQuotes) {
            if (pendingQuote) {
                pendingQuote = false;
                if (c != '"') { inQuotes = false; --i; } // reprocess as unquoted
                continue;
            }
            if (c == '"') { pendingQuote = true; continue; }
            continue;
        }

        if (c == '"') { inQuotes = true; pendingQuote = false; continue; }

        if (c == ',') {
            push_token(tokenStart, i);
            tokenStart = i + 1;
            if ((int)tokens.size() == maxTokensNeeded) {
                *currentRowByteOffset = startOff + tokenStart; // after comma
                return tokens;
            }
            continue;
        }

        if (c == '\n' || c == '\r') {
            push_token(tokenStart, i);
            uint64_t adv = i + 1;
            if (c == '\r' && i + 1 < bytesRead && buffer[i + 1] == '\n') ++adv;
            *currentRowByteOffset = startOff + adv; // next row
            return tokens;
        }
    }

    if (!tokens.empty()) {
        *currentRowByteOffset = startOff + tokenStart;
    } else {
        *currentRowByteOffset = startOff + static_cast<uint64_t>(bytesRead);
    }
    return tokens;
}

static void processColumnChunkMap(ColumnChunk& chunk, int threadIndex) {
    const int startingCol = chunk.start;
    const int endingCol   = chunk.end;
    const int ncols       = endingCol - startingCol;

    const int rowChunkSize = chunk.rowCount / NUM_THREADS;
    const int startingRow  = threadIndex * rowChunkSize;
    const int endingRow    = (threadIndex == NUM_THREADS - 1) ? chunk.rowCount
                                                              : (threadIndex + 1) * rowChunkSize;

    for (int row = startingRow; row < endingRow; ++row) {
        int currentCol = startingCol;

        while (currentCol < endingCol) {
            auto tokens = getTokens(&chunk.rowCursor[row], chunk.filePath, endingCol - currentCol);
            if (tokens.empty()) continue; // moved forward; try again

            for (const auto& token : tokens) {
                if (token == "3") {
                    std::cout << "Found token '3' in row " << row << ", column " << currentCol << std::endl;
                }
                const int colIndex = currentCol - startingCol;
                if (colIndex < 0 || colIndex >= ncols) break;

                // thread-local per-column dict
                auto& mp = chunk.localMaps[threadIndex][colIndex];

                int localId;
                auto it = mp.find(token);
                if (it != mp.end()) localId = it->second;
                else {
                    localId = static_cast<int>(mp.size());
                    mp.emplace(token, localId);
                }
                chunk.data[colIndex][row] = localId;
                ++currentCol;
                if (currentCol >= endingCol) break;
            }
        }
    }
}

static void processColumnChunk(ColumnChunk& chunk, uint32_t& outMaxGlobalIdInChunk) {
    std::thread threads[NUM_THREADS];
    for (int t = 0; t < NUM_THREADS; ++t)
        threads[t] = std::thread(processColumnChunkMap, std::ref(chunk), t);
    for (auto& th : threads) th.join();

    const int ncols = chunk.end - chunk.start;

    // 1) Build global dicts per column
    std::vector<std::map<std::string,int>> globalDict(ncols);
    for (int c = 0; c < ncols; ++c) {
        auto& g = globalDict[c];
        for (int t = 0; t < NUM_THREADS; ++t) {
            for (const auto& kv : chunk.localMaps[t][c]) {
                if (g.find(kv.first) == g.end())
                    g.emplace(kv.first, static_cast<int>(g.size()));
            }
        }
    }

    // 2) Per-thread remap LUT: local -> global
    std::vector<std::vector<std::vector<int>>> remap(NUM_THREADS);
    for (int t = 0; t < NUM_THREADS; ++t) {
        remap[t].resize(ncols);
        for (int c = 0; c < ncols; ++c) {
            const auto& local = chunk.localMaps[t][c];
            auto&       g     = globalDict[c];
            std::vector<int> lut(local.size(), -1);
            for (const auto& kv : local) {
                lut[kv.second] = g.at(kv.first);
            }
            remap[t][c] = std::move(lut);
        }
    }

    // 3) Relabel to global ids
    uint32_t maxId = 0;
    const int rowChunkSize = chunk.rowCount / NUM_THREADS;
    for (int t = 0; t < NUM_THREADS; ++t) {
        const int srow = t * rowChunkSize;
        const int erow = (t == NUM_THREADS - 1) ? chunk.rowCount : (t + 1) * rowChunkSize;
        for (int c = 0; c < ncols; ++c) {
            int* colData = chunk.data[c];
            const auto& lut = remap[t][c];
            for (int r = srow; r < erow; ++r) {
                int localId = colData[r];
                if (localId >= 0 && localId < (int)lut.size()) colData[r] = lut[localId];
                else colData[r] = -1;
                if (colData[r] > maxId) {
                    maxId = colData[r];
                    std::cout << "New maxId found: " << maxId << std::endl;
                }
            }
        }
    }

    // 4) compute chunk max global id
    // uint32_t maxId = 0;
    // bool any = false;

    // for (int c = 0; c < ncols; ++c) {
    //     int* colData = chunk.data[c];
    //     for (int r = 0; r < chunk.rowCount; ++r) {
    //         int v = colData[r];
    //         if (v >= 0) {           // ignore our sentinel -1
    //             if (!any || (uint32_t)v > maxId) {
    //                 maxId = (uint32_t)v;
    //                 any = true;
    //             }
    //         }
    //     }
    // }

    outMaxGlobalIdInChunk = maxId;
}

// --------------------------- mapIntTranspose ----------------------------

void TabularData::mapIntTranspose() {
    // read row offsets
    ColumnChunk chunk;
    chunk.filePath = _csvPath;
    chunk.rowCount = static_cast<int>(this->rowCount);
    chunk.rowOffsets = new uint64_t[this->rowCount];
    chunk.rowCursor  = new uint64_t[this->rowCount];

    {
        std::ifstream offs((fs::path(_outputDir) / "row_offsets.bin").string(), std::ios::binary);
        if (!offs) throw std::runtime_error("Failed to open row offsets file");
        offs.read(reinterpret_cast<char*>(chunk.rowOffsets), this->rowCount * sizeof(uint64_t));
        offs.close();
    }
    // initialize rowCursor to the beginning of each row
    std::memcpy(chunk.rowCursor, chunk.rowOffsets, this->rowCount * sizeof(uint64_t));

    // prepare meta file path
    const fs::path metaPath = fs::path(_outputDir) / "column_chunk_meta.bin";
    // truncate once at the beginning (fresh run)
    { std::ofstream(metaPath, std::ios::binary | std::ios::trunc).close(); }

    for (int col = 0; col < static_cast<int>(colCount); col += COLUMNS_PER_CHUNK) {
        std::cout << "Processing column chunk starting at column: " << col << std::endl;
        chunk.start = col;
        chunk.end   = std::min(col + COLUMNS_PER_CHUNK, static_cast<int>(colCount));
        const int ncols = chunk.end - chunk.start;

        // allocate matrix [ncols][rowCount]
        chunk.data = new int*[ncols];
        for (int c = 0; c < ncols; ++c) {
            chunk.data[c] = new int[this->rowCount];
            // Optional initialize:
            // std::fill(chunk.data[c], chunk.data[c] + this->rowCount, -1);
        }

        // per-thread / per-column maps
        chunk.localMaps = new std::map<std::string,int>*[NUM_THREADS];
        for (int t = 0; t < NUM_THREADS; ++t) {
            chunk.localMaps[t] = new std::map<std::string,int>[ncols];
        }

        // run threads + merge/relabel
        uint32_t maxGlobalIdInChunk = 0;
        processColumnChunk(chunk, maxGlobalIdInChunk);

        // append meta immediately: {ncols, maxGlobalIdInChunk}
        {
            std::ofstream meta(metaPath, std::ios::binary | std::ios::app);
            if (!meta) throw std::runtime_error("Failed to open column_chunk_meta.bin for append");
            uint32_t ncols_u32 = static_cast<uint32_t>(ncols);
            meta.write(reinterpret_cast<const char*>(&ncols_u32), sizeof(uint32_t));
            meta.write(reinterpret_cast<const char*>(&maxGlobalIdInChunk), sizeof(uint32_t));
            meta.flush();
        }

        // free per-chunk buffers
        for (int c = 0; c < ncols; ++c) delete[] chunk.data[c];
        delete[] chunk.data;      chunk.data = nullptr;

        for (int t = 0; t < NUM_THREADS; ++t) delete[] chunk.localMaps[t];
        delete[] chunk.localMaps; chunk.localMaps = nullptr;
    }

    delete[] chunk.rowOffsets; chunk.rowOffsets = nullptr;
    delete[] chunk.rowCursor;  chunk.rowCursor  = nullptr;
}

} // namespace tabular
