#include "TabularData/TabularData.hpp"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>
#include <thread>
#include <sstream>
#include <iomanip>

namespace fs = std::filesystem;

namespace tabular {

    #ifndef NUM_THREADS
    #define NUM_THREADS 8
    #endif

    #ifndef CHUNK_SIZE
    #define CHUNK_SIZE (1<<20)u// 1 MiB if not set by CMake
    #endif

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

    // Unescape doubled quotes in the content slice
    return trim(unescapeCsvField(buffer));
}

const TabularData::u32 TabularData::getColumnCount() const {
    //get file size
    std::ifstream binFile(_headersbinFilePath, std::ios::binary | std::ios::ate);
    if (!binFile) throw std::runtime_error("Missing headers index file.");
    const auto fileSize = binFile.tellg();
    if (fileSize < 0) throw std::runtime_error("Failed to get size of headers index file.");
    return static_cast<TabularData::u32>(fileSize) / (sizeof(TabularData::u32) + sizeof(TabularData::u16));
}

const TabularData::u32 TabularData::getRowCount() const {
    const fs::path p = fs::path(_outputDir) / "row_offsets.bin";
    const auto sz = fs::file_size(p);
    if (sz % sizeof(std::uint64_t) != 0) throw std::runtime_error("Corrupt merged offsets file");
    return static_cast<std::uint64_t>(sz / sizeof(std::uint64_t));
}

// #ifndef NUM_THREADS
// #define NUM_THREADS 16
// #endif

// #ifndef CHUNK_SIZE
// #define CHUNK_SIZE 1048576u // 1 MiB if not set by CMake
// #endif

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
                return nextStart;
            }
        }
    }
    return pos; // no newline: header only with no terminator
}

// Robust resync from arbitrary offset S to the first byte of the NEXT row.
// Handles the "starting at a closing quote" ambiguity with small lookahead.
u64 resync_to_next_row_start(const std::string &path, u64 S) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV for resync");
    u64 fsize = file_size_bytes(path);

    if (S >= fsize) return fsize;

    in.seekg(static_cast<std::streamoff>(S), std::ios::beg);
    u64 pos = S;
    CsvState st;

    // --- Initial disambiguation around leading quotes at S (only once) ---
    {
        int c0i = in.get();
        if (c0i == EOF) return pos;
        char c0 = static_cast<char>(c0i);
        ++pos;

        if (c0 == '"') {
            int n1 = in.peek();

            // Case: closing quote before delimiter/newline/EOF
            if (n1 == ',' || n1 == '\n' || n1 == '\r' || n1 == EOF) {
                if (n1 == ',') { in.get(); ++pos; /* stay unquoted */ }
                else if (n1 == '\n') { in.get(); ++pos; return pos; }
                else if (n1 == '\r') {
                    in.get(); ++pos;
                    pos += maybe_consume_LF_after_CR(in);
                    return pos;
                } else /* EOF */ {
                    return fsize; // last row ended at EOF; no next row start
                }
                // fall-through: continue unquoted after comma
            } else if (n1 == '"') {
                // Could be "" (empty field) or "" followed by data (escaped quote within quoted field)
                in.get(); ++pos; // consume second quote
                int n2 = in.peek();
                if (n2 == ',' || n2 == '\n' || n2 == '\r' || n2 == EOF) {
                    // Empty quoted field ""
                    if (n2 == ',') { in.get(); ++pos; /* continue unquoted */ }
                    else if (n2 == '\n') { in.get(); ++pos; return pos; }
                    else if (n2 == '\r') { in.get(); ++pos; pos += maybe_consume_LF_after_CR(in); return pos; }
                    else /* EOF */ { return fsize; }
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
        if (got <= 0) return pos; // EOF (no later row start)

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
                return nextStart;
            }
        }
    }
}

// Parse [start, stop) and write row-start offsets to a binary file.
void parse_slice_to_file(const std::string &path, u64 start, u64 stop, const std::string &outPath) {
    std::ofstream out(outPath, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Failed to open output: " + outPath);

    if (start >= stop) return;

    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open CSV in worker");
    in.seekg(static_cast<std::streamoff>(start), std::ios::beg);

    // Always emit the first row-start in this slice.
    out.write(reinterpret_cast<const char*>(&start), sizeof(u64));

    std::vector<char> buf(CHUNK_SIZE);
    CsvState st;
    u64 pos = start;

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
                if (nextStart >= stop) return;
                out.write(reinterpret_cast<const char*>(&nextStart), sizeof(u64));
            }
        }
    }
}

} // end anonymous namespace

void TabularData::findRowOffsets() const {
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
    workers.reserve(NUM_THREADS);
    for (int t = 0; t < NUM_THREADS; ++t) {
        std::ostringstream oss;
        oss << prefix << ".part-" << t << ".bin";
        const std::string outPath = oss.str();
        workers.emplace_back([&, t, outPath]{
            parse_slice_to_file(_csvPath, handoff[t], handoff[t + 1], outPath);
        });
    }
    for (auto &th : workers) th.join();


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



} // namespace tabular
