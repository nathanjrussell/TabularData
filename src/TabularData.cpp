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

namespace fs = std::filesystem;

namespace tabular {

    #ifndef NUM_THREADS
    #define NUM_THREADS 14
    #endif

    #ifndef CHUNK_SIZE
    #define CHUNK_SIZE (1<<20)u//  1 MiB if not set by CMake
    #endif

    #ifndef HEADER_INDEX_FILE_NAME
    #define HEADER_INDEX_FILE_NAME "header_string_lookup_offsets.bin"
    #endif

    #ifndef COLUMNS_PER_CHUNK
    #define COLUMNS_PER_CHUNK 100000
    #endif

static constexpr const char* kHeaderIndexFileName = "header_string_lookup_offsets.bin";

TabularData::TabularData(std::string csvPath, std::string outputDir)
    : _csvPath(std::move(csvPath)), _outputDir(std::move(outputDir)) {
    if (_csvPath.empty())  throw std::invalid_argument("csvPath is empty");
    if (_outputDir.empty()) throw std::invalid_argument("outputDir is empty");

    fs::create_directories(_outputDir);
    _headersbinFilePath = (fs::path(_outputDir) / kHeaderIndexFileName).string();
}

void TabularData::skipFaultyRows(bool skip) {
    this->skipRows = skip;
}

void TabularData::parseHeaderRow() {
    this->colCount = 0;
    std::ifstream in(_csvPath, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV file: " + _csvPath);

    std::ofstream binFile(_headersbinFilePath, std::ios::binary | std::ios::trunc);
    if (!binFile) throw std::runtime_error("Failed to open headers index file: " + _headersbinFilePath);


    std::vector<char> buf(CHUNK_SIZE);

    // State across chunk boundaries
    bool inQuotes = false;       // currently inside a quoted field
    bool atFieldStart = true;    // cursor is at start of a field (no content seen yet)
    bool pendingQuote = false;   // saw a quote while inQuotes; need next char to resolve
    bool headerDone = false;     // header row fully parsed

    u32 pos = 0;                 // absolute byte offset from start of file
    u32 fieldStart = 0;          // offset of first content byte (excludes opening quote)
    u32 lastContent = 0;         // offset of last content byte seen

   

    auto close_field = [&]() {
        // If empty, write end < start by using start-1 (consumer treats as empty)
        const u16 length = (lastContent >= fieldStart) ? lastContent : (fieldStart - 1);
        binFile.write(reinterpret_cast<const char*>(&fieldStart), sizeof(u32));
        binFile.write(reinterpret_cast<const char*>(&length),   sizeof(u16));
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
    in.close();
    binFile.close();
}

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

    const u32 len = end - start + 1;
    std::string buffer;
    buffer.resize(static_cast<size_t>(len));

    in.seekg(static_cast<std::streamoff>(start), std::ios::beg);
    in.read(buffer.data(), static_cast<std::streamsize>(len));
    if (!in) throw std::runtime_error("Failed to read header slice from CSV");

    in.close();
    // Unescape doubled quotes in the content slice
    return trim(unescapeCsvField(buffer));
}

const TabularData::u32 TabularData::getColumnCount() const {
    //get file size
    std::ifstream binFile(_headersbinFilePath, std::ios::binary | std::ios::ate);
    if (!binFile) throw std::runtime_error("Missing headers index file.");
    const auto fileSize = binFile.tellg();
    if (fileSize < 0) throw std::runtime_error("Failed to get size of headers index file.");
    binFile.close();
    return static_cast<TabularData::u32>(fileSize) / (sizeof(TabularData::u32) + sizeof(TabularData::u16));
}

// const TabularData::u32 TabularData::getRowCount() const {
//     const fs::path p = fs::path(_outputDir) / "row_offsets.bin";
//     const auto sz = fs::file_size(p);
//     if (sz % sizeof(std::uint64_t) != 0) throw std::runtime_error("Corrupt merged offsets file");
//     return static_cast<std::uint64_t>(sz / sizeof(std::uint64_t));
// }


const TabularData::u32 TabularData::getRowCount() const {
    return this->rowCount;
}


namespace { // --------- file-local helpers (no header pollution)

using u64 = std::uint64_t;

struct CsvState {
    bool inQuotes = false;
    bool pendingQuote = false; // saw " while inQuotes; next char decides
};

inline bool feed_csv(char c, CsvState &st) {
    if (st.inQuotes) {
        if (st.pendingQuote) {
            st.pendingQuote = false;
            if (c == '"') { // "" => literal quote
                return false;
            } else {        // end of quoted section; reprocess c unquoted
                st.inQuotes = false;
                return feed_csv(c, st);
            }
        }
        if (c == '"') { st.pendingQuote = true; return false; }
        return false; // commas/newlines inside quotes are data
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

u64 file_size_bytes(const std::string &path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) throw std::runtime_error("Failed to open file for size: " + path);
    f.close();
    return static_cast<u64>(f.tellg());
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
                    if (i + 1 < got && buf[static_cast<size_t>(i + 1)] == '\n') {
                        ++i; ++pos; ++nextStart;
                    } else {
                        nextStart += maybe_consume_LF_after_CR(in);
                        pos += (nextStart - (pos + 1));
                    }
                }
                in.close();
                return nextStart;
            }
        }
    }
    in.close();
    return pos; // no newline: header only with no terminator
}

// Robust resync from arbitrary offset S to the first byte of the NEXT row.
// Handles the "starting at a closing quote" ambiguity with small lookahead.
u64 resync_to_next_row_start(const std::string &path, u64 S) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV for resync");
    u64 fsize = file_size_bytes(path);

    if (S >= fsize) {
        in.close();
        return fsize;
    }

    in.seekg(static_cast<std::streamoff>(S), std::ios::beg);
    u64 pos = S;
    CsvState st;

    // --- Initial disambiguation around leading quotes at S (only once) ---
    {
        int c0i = in.get();
        if (c0i == EOF) {
            in.close();
            return pos;
        }
        char c0 = static_cast<char>(c0i);
        ++pos;

        if (c0 == '"') {
            int n1 = in.peek();

            // Case: closing quote before delimiter/newline/EOF
            if (n1 == ',' || n1 == '\n' || n1 == '\r' || n1 == EOF) {
                if (n1 == ',') { in.get(); ++pos; /* stay unquoted */ }
                else if (n1 == '\n') { 
                    in.get(); 
                    ++pos; 
                    in.close();
                    return pos; 
                }
                else if (n1 == '\r') {
                    in.get(); ++pos;
                    pos += maybe_consume_LF_after_CR(in);
                    in.close();
                    return pos;
                } else /* EOF */ {
                    in.close();
                    return fsize; // last row ended at EOF; no next row start
                }
                // fall-through: continue unquoted after comma
            } else if (n1 == '"') {
                // Could be "" (empty field) or "" followed by data (escaped quote within quoted field)
                in.get(); ++pos; // consume second quote
                int n2 = in.peek();
                if (n2 == ',' || n2 == '\n' || n2 == '\r' || n2 == EOF) {
                    // Empty quoted field ""
                    if (n2 == ',') { 
                        in.get(); 
                        ++pos; /* continue unquoted */ }
                    else if (n2 == '\n') { 
                        in.get(); 
                        ++pos;
                        in.close(); 
                        return pos; 
                    }
                    else if (n2 == '\r') { 
                        in.get(); 
                        ++pos; 
                        pos += maybe_consume_LF_after_CR(in); 
                        in.close();
                        return pos; }
                    else /* EOF */ {
                        in.close();
                        return fsize;
                    }
                } else {
                    // "" followed by data => we are inside a quoted field with a literal "
                    st.inQuotes = true;
                }
            } else {
                // Opening quote at start of a field
                st.inQuotes = true;
            }
        }
        // else: non-quote; just proceed (we intentionally don't treat c0 specially)
    }

    // --- Buffered forward scan until first unquoted newline ---
    std::vector<char> buf(CHUNK_SIZE);
    while (true) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = in.gcount();
        if (got <= 0) {
            in.close();
            return pos; // EOF (no later row start)
        }

        for (std::streamsize i = 0; i < got; ++i, ++pos) {
            char c = buf[static_cast<size_t>(i)];
            if (feed_csv(c, st)) {
                u64 nextStart = pos + 1;
                if (c == '\r') {
                    if (i + 1 < got && buf[static_cast<size_t>(i + 1)] == '\n') {
                        ++i; ++pos; ++nextStart;
                    } else {
                        nextStart += maybe_consume_LF_after_CR(in);
                        pos += (nextStart - (pos + 1));
                    }
                }
                in.close();
                return nextStart;
            }
        }
    }
}

// Parse [start, stop) and write row-start offsets to a binary file.
void parse_slice_to_file(const std::string &path,
                         u64 start, u64 stop,
                         const std::string &outPath,
                         tabular::TabularData::u32 expectedCols,
                         std::uint32_t *rowsOut,
                        bool skipFaultyRows
                        )
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
    std::uint32_t commaCount = 0; // unquoted commas in current row
    bool rowNotBlank = false;     // row has non-whitespace content (or comma/quote)

    auto handle_row_end = [&](u64 nextStart) {
        // ignore blank rows (only spaces/tabs + newline)
        if (!rowNotBlank) {
            currentRowStart = nextStart;
            commaCount = 0;
            rowNotBlank = false;
            return;
        }
        // validate width
        std::uint32_t fields = commaCount + 1;
        if (fields != expectedCols) {
            std::ostringstream msg;
            msg << "Column count mismatch at row "<<*rowsOut + 1<< " (starting offset " << currentRowStart
                << "): expected " << expectedCols << ", found " << fields;
            std::cerr << msg.str() << std::endl;
            std::terminate();
   
        }
        // emit offset + count it
        out.write(reinterpret_cast<const char*>(&currentRowStart), sizeof(u64));
        ++(*rowsOut);

        // reset for next row
        currentRowStart = nextStart;
        commaCount = 0;
        rowNotBlank = false;
    };

    while (true) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = in.gcount();
        if (got <= 0) break;

        for (std::streamsize i = 0; i < got; ++i, ++pos) {
            char c = buf[static_cast<size_t>(i)];

            // mark row as non-blank (in unquoted context)
            if (!st.inQuotes) {
                if (c == ',') { ++commaCount; rowNotBlank = true; }
                else if (c == '"') { rowNotBlank = true; } // opening quote implies a field
                else if (c != ' ' && c != '\t' && c != '\r' && c != '\n') { rowNotBlank = true; }
            } else {
                // already non-blank once we saw the opening quote
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

    // EOF: finalize a trailing row with no newline
    if (pos > currentRowStart) {
        handle_row_end(pos);
    }
}


} // end anonymous namespace

void TabularData::findRowOffsets()  {
    // Where we’ll write per-thread outputs
    const std::string prefix = (fs::path(_outputDir) / "row_offsets").string();

    const u64 fsize = file_size_bytes(_csvPath);
    if (fsize == 0) {
        // Emit empty part files for consistency
        for (int t = 0; t < NUM_THREADS; ++t) {
            std::ostringstream oss;
            oss << prefix << ".part-"<< t << ".bin";
            std::ofstream(oss.str(), std::ios::binary | std::ios::trunc).close();
        }
        return;
    }

    // Skip header row; start indexing at first data row
    const u64 firstData = find_first_data_offset(_csvPath);
    // If header extends to EOF, still emit empty parts
    if (firstData >= fsize) {
        for (int t = 0; t < NUM_THREADS; ++t) {
            std::ostringstream oss;
            oss << prefix << ".part-" <<t << ".bin";
            std::ofstream(oss.str(), std::ios::binary | std::ios::trunc).close();
        }
        return;
    }

    // Nominal byte-slice starts across the DATA region [firstData, fsize)
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

    // Phase 1: find handoff boundaries (true row starts).
    // handoff[0] = firstData; handoff[N] = fsize; handoff[t] for t∈(1..N-1) found by resync from S[t].
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

    // Phase 2: each thread parses [handoff[t], handoff[t+1]) and writes its own part file
    std::vector<std::thread> workers;
    uint32_t threadRowCounts[NUM_THREADS] = {0};
    workers.reserve(NUM_THREADS);
    for (int t = 0; t < NUM_THREADS; ++t) {
        std::ostringstream oss;
        oss << prefix << ".part-" << t << ".bin";
        const std::string outPath = oss.str();
        workers.emplace_back([&, t, outPath]{
            parse_slice_to_file(
                _csvPath, 
                handoff[t], 
                handoff[t + 1], 
                outPath, 
                this->colCount, 
                &threadRowCounts[t], 
                this->skipRows
            );
        });
    }
    for (auto &th : workers) th.join();
    this->rowCount = 0;
    for (int t = 0; t < NUM_THREADS; ++t) {
        this->rowCount += threadRowCounts[t];
    }


// --- Merge parts, then delete them ---
    {
        const fs::path prefix = fs::path(_outputDir) / "row_offsets";
        const fs::path merged = fs::path(_outputDir) / "row_offsets.bin";

        // Merge (simple append in thread index order)
        {
            std::ofstream out(merged, std::ios::binary | std::ios::trunc);
            if (!out) throw std::runtime_error("Failed to open merged output: " + merged.string());

            for (int t = 0; t < NUM_THREADS; ++t) {
                std::ostringstream oss;
                oss << prefix.string()
                    << ".part-" << t << ".bin";
                std::ifstream in(oss.str(), std::ios::binary);
                if (!in) throw std::runtime_error("Missing part file: " + oss.str());
                out << in.rdbuf();  // append
            }
            out.close();
        } // <- out closes here (important on Windows before deleting files)

        // Delete parts (best-effort)
        std::error_code ec;
        for (int t = 0; t < NUM_THREADS; ++t) {
            std::ostringstream oss;
            oss << prefix.string()
                << ".part-" << t << ".bin";
            fs::remove(oss.str(), ec);
            ec.clear();
        }
    }

}

struct ColumnChunk {
    std::string filePath;
    uint64_t *rowOffsets;
    int rowCount;
    int start;
    int end;
    int **data;
    std::map<std::string,int> **localMaps = nullptr;   // each thread has a collection of maps
                            // number of columns in this chunk
};




// std::string trim(std::string str) {
//     // Remove leading whitespace
//     str.erase(str.begin(), std::find_if(str.begin(), str.end(), [](unsigned char ch) {
//         return !std::isspace(ch);
//     }));
//     // Remove trailing whitespace
//     str.erase(std::find_if(str.rbegin(), str.rend(), [](unsigned char ch) {
//         return !std::isspace(ch);
//     }).base(), str.end());
//     return str;
// }

std::vector<std::string> getTokens(uint64_t* currentRowByteOffset,
                                   const std::string& filePath,
                                   int maxTokensNeeded)
{
    std::vector<std::string> tokens;
    if (maxTokensNeeded <= 0) return tokens;

    std::ifstream in(filePath, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV file: " + filePath);

    const uint64_t startOff = *currentRowByteOffset;
    in.seekg(static_cast<std::streamoff>(startOff), std::ios::beg);
    if (!in) throw std::runtime_error("Failed to seek to position in CSV file.");

    std::vector<char> buffer(CHUNK_SIZE);
    in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const int bytesRead = static_cast<int>(in.gcount());
    if (bytesRead <= 0) return tokens; // EOF, no progress to make

    bool inQuotes = false;
    bool pendingQuote = false; // saw " while inQuotes
    int tokenStart = 0;

    auto push_token = [&](int start, int end) {
        if (end < start) end = start;
        std::string tok(buffer.data() + start, end - start);
        tok = trim(tok);
        tokens.emplace_back(std::move(tok));
    };

    for (int i = 0; i < bytesRead; ++i) {  // <-- FIX: initialize i = 0
        char c = buffer[i];

        if (inQuotes) {
            if (pendingQuote) {
                pendingQuote = false;
                if (c == '"') {
                    // Escaped quote "" -> literal; keep parsing quoted
                } else {
                    // closing quote ended; reprocess this char as unquoted
                    inQuotes = false;
                    --i; // re-handle this char in the unquoted state
                }
                continue;
            }
            if (c == '"') { pendingQuote = true; continue; }
            // otherwise literal inside quotes
            continue;
        }

        // Unquoted
        if (c == '"') { inQuotes = true; pendingQuote = false; continue; }

        if (c == ',') {
            push_token(tokenStart, i);
            tokenStart = i + 1;
            if ((int)tokens.size() == maxTokensNeeded) {
                *currentRowByteOffset = startOff + tokenStart;  // after the comma
                return tokens;
            }
            continue;
        }

        if (c == '\n' || c == '\r') {
            // End of row: push the last field for this row
            push_token(tokenStart, i);
            // Advance beyond newline (normalize CRLF)
            uint64_t adv = i + 1;
            if (c == '\r' && i + 1 < bytesRead && buffer[i + 1] == '\n') ++adv;
            *currentRowByteOffset = startOff + adv;
            return tokens;
        }
    }

    // No newline encountered in this buffer
    if (!tokens.empty()) {
        // We consumed tokens up to the last comma; continue next call from tokenStart
        *currentRowByteOffset = startOff + tokenStart;
    } else {
        // Huge first field (no comma/newline yet). Make progress to avoid a spin.
        *currentRowByteOffset = startOff + static_cast<uint64_t>(bytesRead);
    }
    return tokens;
}



void processColumnChunkMap(ColumnChunk& chunk, int threadIndex) {
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
            auto tokens = getTokens(&chunk.rowOffsets[row], chunk.filePath, endingCol - currentCol);
            if (tokens.empty()) continue; // offset advanced; try again

            for (const auto& token : tokens) {
                int colIndex = currentCol - startingCol;
                if (colIndex < 0 || colIndex >= ncols) break;

                // thread-local dictionary
                auto& mp = chunk.localMaps[threadIndex][colIndex];

                auto it = mp.find(token);
                int localId;
                if (it != mp.end()) {
                    localId = it->second;
                } else {
                    localId = static_cast<int>(mp.size());
                    mp.emplace(token, localId);
                }
                chunk.data[colIndex][row] = localId; // store LOCAL id for now
                ++currentCol;
                if (currentCol >= endingCol) break;
            }
        }
    }
}


void processColumnChunk(ColumnChunk& chunk) {
    std::thread threads[NUM_THREADS];
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads[t] = std::thread(processColumnChunkMap, std::ref(chunk), t);
    }
    for (auto& th : threads) th.join();

    // ----- Merge per-column -----
    const int ncols = chunk.end - chunk.start;

    // 1) Build global dict per column
    std::vector<std::map<std::string,int>> globalDict(ncols);
    for (int c = 0; c < ncols; ++c) {
        auto& g = globalDict[c];
        for (int t = 0; t < NUM_THREADS; ++t) {
            for (const auto& kv : chunk.localMaps[t][c]) {
                // assign new global id if first time seen
                if (g.find(kv.first) == g.end()) {
                    g.emplace(kv.first, static_cast<int>(g.size()));
                }
            }
        }
    }

    // 2) Build local->global remap vectors
    //    remap[t][c][localId] = globalId
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

    // 3) Relabel chunk.data from local IDs to global IDs
    const int rowChunkSize = chunk.rowCount / NUM_THREADS;
    for (int t = 0; t < NUM_THREADS; ++t) {
        const int srow = t * rowChunkSize;
        const int erow = (t == NUM_THREADS - 1) ? chunk.rowCount : (t + 1) * rowChunkSize;

        for (int c = 0; c < ncols; ++c) {
            int* colData = chunk.data[c];
            const auto& lut = remap[t][c];

            for (int r = srow; r < erow; ++r) {
                int localId = colData[r];
                if (localId >= 0 && localId < (int)lut.size())
                    colData[r] = lut[localId];
                else
                    colData[r] = -1; // or handle error
            }
        }
    }
}



void TabularData::mapIntTranspose() {
    ColumnChunk chunk;
    chunk.filePath = _csvPath;
    chunk.rowCount = this->rowCount;
    chunk.rowOffsets = new uint64_t[this->rowCount];

    std::ifstream offs((fs::path(_outputDir) / "row_offsets.bin").string(), std::ios::binary);
    if (!offs) throw std::runtime_error("Failed to open row offsets file");
    offs.read(reinterpret_cast<char*>(chunk.rowOffsets), this->rowCount * sizeof(uint64_t));
    offs.close();

    for (int col = 0; col < colCount; col += COLUMNS_PER_CHUNK) {
        std::cout << "Processing column chunk starting at column: " << col << std::endl;
        chunk.start = col;
        chunk.end   = std::min(col + COLUMNS_PER_CHUNK, colCount);

        const int ncols = chunk.end - chunk.start;

        // ==== ALLOCATE PER-CHUNK BUFFERS (Step 2) ====

        // data matrix: [ncols][rowCount]
        chunk.data = new int*[ncols];
        for (int c = 0; c < ncols; ++c) {
            chunk.data[c] = new int[this->rowCount];
            // (optional) std::fill(chunk.data[c], chunk.data[c] + this->rowCount, -1);
        }

        // per-thread, per-column maps: localMaps[NUM_THREADS][ncols]
        chunk.localMaps = new std::map<std::string,int>*[NUM_THREADS];
        for (int t = 0; t < NUM_THREADS; ++t) {
            chunk.localMaps[t] = new std::map<std::string,int>[ncols];
        }

        // ==== RUN THREADS (your existing entry) ====
        processColumnChunk(chunk);   // this should use chunk.localMaps[t][c]

        // (You’ll do the merge/relabel after the join, in processColumnChunk or here.)

        // ==== FREE PER-CHUNK BUFFERS (Step 2 cleanup) ====
        for (int c = 0; c < ncols; ++c) delete[] chunk.data[c];
        delete[] chunk.data;      chunk.data = nullptr;

        for (int t = 0; t < NUM_THREADS; ++t) delete[] chunk.localMaps[t];
        delete[] chunk.localMaps; chunk.localMaps = nullptr;
    }

    delete[] chunk.rowOffsets; // if you won't reuse them after this
}




} // namespace tabular
