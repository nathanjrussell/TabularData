#include <thread>
#include <algorithm>
#include <cstdio>
#include <limits>

// ---- helpers ----
static inline std::size_t per_thread_buf_size() {
    const std::size_t n = (TABULARDATA_NUM_THREADS > 0) ? TABULARDATA_NUM_THREADS : 1;
    const std::size_t base = static_cast<std::size_t>(TABULARDATA_MAX_BUFFER_BYTES) / (n ? n : 1);
    return std::max<std::size_t>(base, 4096);
}
static inline std::uint64_t file_size_u64(const std::string& path) {
    return static_cast<std::uint64_t>(fs::file_size(path));
}
static inline std::string thread_offsets_path(const std::string& dir, unsigned tidx) {
    return dir + "/row_offsets_thread_" + std::to_string(tidx) + ".bin";
}



#include <limits>
#include <cstdio>

// CSV-aware: parse header end, then collect logical row starts for data rows.
// Validates each row has the same number of columns as the header.
// Empty rows (0 columns) are ignored. Header row is excluded.
// In TabularData.cpp (or create_offsets_file.cpp) replace your findNewLineOffsets() with this:

#include <cstdio>     // std::remove (if you later want to remove temps)
#include <limits>
#include <tuple>

// Stores the byte offset where each DATA row starts (header excluded).
// Writes raw uint64_t offsets to <output_dir>/row_byte_offsets.bin
// Returns false on I/O error or if any data row's column count != header's.
bool TabularData::findNewLineOffsets() {
    if (csv_path_.empty()) {
        std::cerr << "findNewLineOffsets: csv_path_ is empty.\n"; return false;
    }
    if (num_columns_ == 0) {
        std::cerr << "findNewLineOffsets: header not parsed; call parseHeaderFromCsv() first.\n";
        return false;
    }

    const std::string out_dir = output_directory_.empty() ? "." : output_directory_;
    if (!std::filesystem::exists(out_dir)) std::filesystem::create_directories(out_dir);

    std::ifstream in(csv_path_, std::ios::binary);
    if (!in) { std::cerr << "findNewLineOffsets: cannot open " << csv_path_ << "\n"; return false; }

    const std::string out_path = out_dir + "/row_byte_offsets.bin";
    std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
    if (!out) { std::cerr << "findNewLineOffsets: cannot open " << out_path << "\n"; return false; }

    // ---- Config (match your header parsing) ----
    const char DELIM = ',';       // adjust if you used a different delimiter
    const char QUOTE = '"';

    // ---- BOM handling ----
    std::uint64_t file_start = 0;
    {
        unsigned char bom[3] = {};
        in.read(reinterpret_cast<char*>(bom), 3);
        if (in.gcount() == 3 && bom[0]==0xEF && bom[1]==0xBB && bom[2]==0xBF) file_start = 3;
        in.clear();
        in.seekg(static_cast<std::streamoff>(file_start), std::ios::beg);
    }

    std::vector<char> buf(static_cast<size_t>(TABULARDATA_MAX_BUFFER_BYTES));
    std::uint64_t abs_pos = file_start;

    // CSV state
    bool in_quotes = false, pending_quote = false, pending_cr = false, header_done = false;

    // Current logical row (the one we’re scanning right now):
    std::uint64_t cur_row_start = file_start;
    std::uint64_t delim_count = 0;
    bool any_byte_in_row = false;

    // The start offset of the *current data row* (byte AFTER the header newline,
    // and after each subsequent newline). We only WRITE it when the row that just
    // ended validates (correct column count).
    bool have_pending_start = false;
    std::uint64_t pending_start = 0;

    total_lines_ = 0;

    auto reset_row = [&](){
        pending_cr = false;
        pending_quote = false;
        in_quotes = false;
        delim_count = 0;
        any_byte_in_row = false;
    };

    auto finalize_current_row = [&](std::uint64_t next_row_start) -> bool {
        // Determine columns for the row that started at cur_row_start (just ended)
        const bool row_has_cols = (any_byte_in_row || delim_count > 0);
        const std::uint64_t cols = row_has_cols ? (delim_count + 1) : 0;

        if (!header_done) {
            // Header just ended: arm pending_start to the FIRST data-row start
            header_done = true;
            have_pending_start = true;
            pending_start = next_row_start;      // byte AFTER the header newline
        } else if (row_has_cols) {
            // Validate the data row that just ended
            if (cols != num_columns_) {
                std::cerr << "CSV row at byte " << cur_row_start
                          << " has " << cols << " columns; expected " << num_columns_ << "\n";
                return false;
            }
            // Row is valid → WRITE the start of THIS data row
            if (have_pending_start) {
                out.write(reinterpret_cast<const char*>(&pending_start), sizeof(std::uint64_t));
                ++total_lines_;
            }
            // And arm pending_start for the NEXT data row
            have_pending_start = true;
            pending_start = next_row_start;
        } else {
            // Empty logical row: do not write; just move on
            have_pending_start = true;
            pending_start = next_row_start;
        }

        cur_row_start = next_row_start;
        reset_row();
        return true;
    };

    while (in) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        const std::streamsize got = in.gcount();
        if (got <= 0) break;

        for (std::streamsize i = 0; i < got; ++i) {
            unsigned char c = static_cast<unsigned char>(buf[static_cast<size_t>(i)]);

            // Resolve “” vs close when in quotes
            if (pending_quote) {
                pending_quote = false;
                if (c == static_cast<unsigned char>(QUOTE)) { any_byte_in_row = true; ++abs_pos; continue; }
                in_quotes = false; // closing quote; reprocess this byte
                continue;
            }

            if (!in_quotes) {
                // If previous was CR, decide CRLF vs lone CR
                if (pending_cr) {
                    pending_cr = false;
                    if (c == '\n') {
                        // CRLF → next row starts at LF+1
                        const std::uint64_t next_start = abs_pos + 1;
                        if (!finalize_current_row(next_start)) return false;
                        ++abs_pos; // consume LF
                        continue;
                    } else {
                        // Lone CR → next row starts at current byte
                        const std::uint64_t next_start = abs_pos;
                        if (!finalize_current_row(next_start)) return false;
                        // Reprocess current byte as first of new row
                        continue;
                    }
                }

                if (c == '\r') { pending_cr = true; ++abs_pos; continue; }
                if (c == '\n') {
                    // LF-only → next row starts at LF+1
                    const std::uint64_t next_start = abs_pos + 1;
                    if (!finalize_current_row(next_start)) return false;
                    ++abs_pos;
                    continue;
                }

                if (c == static_cast<unsigned char>(DELIM)) { ++delim_count; any_byte_in_row = true; ++abs_pos; continue; }
                if (c == static_cast<unsigned char>(QUOTE)) { in_quotes = true; any_byte_in_row = true; ++abs_pos; continue; }
                any_byte_in_row = true; ++abs_pos; continue;
            } else {
                if (c == static_cast<unsigned char>(QUOTE)) { pending_quote = true; ++abs_pos; continue; }
                any_byte_in_row = true; ++abs_pos; continue; // CR/LF in quotes are content
            }
        }
    }

    // EOF finalization (no newline needed to include the last row)
    if (pending_quote) in_quotes = false; // treat trailing quote as close

    if (pending_cr && !in_quotes) {
        // File ended right after CR → next row would start at abs_pos
        const std::uint64_t next_start = abs_pos;
        if (!finalize_current_row(next_start)) return false;
    } else {
        // EOF without newline: if the current row has any content, finalize it
        const bool row_has_cols = (any_byte_in_row || delim_count > 0);
        if (row_has_cols) {
            const std::uint64_t next_start = abs_pos; // theoretical start of next row
            if (!finalize_current_row(next_start)) return false;
        }
    }

    out.flush();
    return true;
}
