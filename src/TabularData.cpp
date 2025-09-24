#include "TabularData.h"

#include <fstream>
#include <iostream>
#include <vector>
#include <cstdint>
#include <filesystem>
#include <string>


namespace fs = std::filesystem;


void TabularData::setOutputDirectory(const std::string& dir) {
    output_directory_ = dir.empty() ? std::string(".") : dir;
    if (!fs::exists(output_directory_)) {
        fs::create_directories(output_directory_);
    }
}

// (Utility retained; currently unused by the .bin path but handy if you later
// add JSON or text utilities.)
void TabularData::writeJsonEscapedChar(std::ostream& os, unsigned char ch) {
    switch (ch) {
        case '\"': os << "\\\""; return;
        case '\\': os << "\\\\"; return;
        case '\b': os << "\\b";  return;
        case '\f': os << "\\f";  return;
        case '\n': os << "\\n";  return;
        case '\r': os << "\\r";  return;
        case '\t': os << "\\t";  return;
        default:
            if (ch < 0x20) {
                static const char* hex = "0123456789abcdef";
                os << "\\u00" << hex[(ch >> 4) & 0xF] << hex[ch & 0xF];
            } else {
                os << static_cast<char>(ch);
            }
    }
}

// -----------------------------------------------------------------------------
// Parse header → write (start,end) pairs to bin outut file
// -----------------------------------------------------------------------------
bool TabularData::parseHeaderFromCsv(const std::string& csv_path) {
    CsvOptions opt; // defaults
    return parseHeaderFromCsv(csv_path, opt);
}

bool TabularData::parseHeaderFromCsv(const std::string& csv_path, const CsvOptions& opt) {
    num_columns_ = 0;
    csv_path_ = csv_path;

    std::ifstream in(csv_path_, std::ios::binary);
    if (!in) {
        std::cerr << "TabularData: cannot open input: " << csv_path_ << "\n";
        return false;
    }

    // check / create output directory and .bin path
    const std::string out_dir = output_directory_.empty() ? std::string(".") : output_directory_;
    if (!fs::exists(out_dir)) {
        fs::create_directories(out_dir);
    }
    bin_path_ = defaultBinPath(out_dir);

    std::ofstream binaryOutput(bin_path_, std::ios::binary | std::ios::trunc);
    if (!binaryOutput) {
        std::cerr << "TabularData: cannot open output: " << bin_path_ << "\n";
        return false;
    }

    // ---- Parser state Flags (RFC 4180 standard compliance) ----
    const char DELIM = opt.delimiter;
    const char QUOTE = opt.quote;

    bool in_quotes      = false;   // inside a quoted field?
    bool pending_quote  = false;   // encountered quote while in_quotes; next byte decides "" vs close
    bool at_field_start = true;    // no content yet for this field
    bool header_done    = false;   // newline (outside quotes) encountered
    bool pending_cr     = false;   // saw CR (outside quotes); waiting to see if next is LF

    // track a closed quoted field waiting for delimiter/newline
    bool after_closing_quote = false;
    std::uint64_t pending_end_excl = 0; // end-of-content (exclusive) for that closed field

    // Offsets
    std::uint64_t abs_pos = 0;          // absolute file position of the CURRENT byte
    std::uint64_t field_start = 0;      // first byte of field content (excl. opening quote)
    bool     field_has_started = false;

    auto start_unquoted_if_needed = [&](std::uint64_t pos) {
        if (at_field_start && !in_quotes && !field_has_started && !after_closing_quote) {
            field_start = pos;     // first content byte
            field_has_started = true;
            at_field_start = false;
        }
    };

    auto start_quoted = [&](std::uint64_t pos_of_open_quote) {
        // Content starts AFTER the opening quote
        field_start = pos_of_open_quote + 1;
        field_has_started = true;
        at_field_start = false;
        in_quotes = true;
        after_closing_quote = false;
    };

    auto finalize_field = [&](std::uint64_t end_excl) {
        // If empty (no content), start=end=end_excl
        std::uint64_t start = field_has_started ? field_start : end_excl;
        binaryOutput.write(reinterpret_cast<const char*>(&start), sizeof(std::uint64_t));
        binaryOutput.write(reinterpret_cast<const char*>(&end_excl), sizeof(std::uint64_t));
        ++num_columns_;
        // Reset per-field state
        at_field_start = true;
        field_has_started = false;
        after_closing_quote = false;
    };

    // Read in chunks
    std::vector<char> buffer(static_cast<size_t>(TABULARDATA_MAX_BUFFER_BYTES));

    while (!header_done) {
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        std::streamsize got = in.gcount();
        if (got <= 0) { break; }

        for (std::streamsize i = 0; i < got && !header_done; ++i) {
            unsigned char c = static_cast<unsigned char>(buffer[static_cast<size_t>(i)]);

            //abs_pos is the position of 'c' right now.
            // increment abs_pos only when  actually consume 'c'.

            // Resolve a pending quote inside a quoted field
            if (pending_quote) {
                pending_quote = false;
                if (c == static_cast<unsigned char>(QUOTE)) {
                    // Escaped quote "" => literal '"' in content. Offsets unaffected.
                    ++abs_pos;      //  consumed this second quote
                    continue;
                } else {
                    // The previous quote was a CLOSING quote.
                    // Closing quote is at position (abs_pos - 1).
                    in_quotes = false;
                    after_closing_quote = true;
                    pending_end_excl = (abs_pos - 1); // exclude the closing quote

                    // Reprocess this current byte 'c' in the new (unquoted) context.
                    // Do NOT consume it here.
                    --i;            // so the loop will see the same byte again
                    continue;       // and abs_pos stays pointing to 'c'
                }
            }

            // Newline handling (only outside quotes)
            if (!in_quotes) {
                if (pending_cr) {
                    pending_cr = false;
                    if (c == '\n') {
                        // CRLF: header ends just before the CR (at abs_pos-1)
                        std::uint64_t end_excl_for_final = after_closing_quote ? pending_end_excl : (abs_pos - 1);
                        finalize_field(end_excl_for_final);
                        header_done = true;
                        ++abs_pos;  // consume the LF
                        continue;
                    } else {
                        // Lone CR: header ends at CR (abs_pos-1). Reprocess 'c'.
                        std::uint64_t end_excl_for_final = after_closing_quote ? pending_end_excl : (abs_pos - 1);
                        finalize_field(end_excl_for_final);
                        header_done = true;
                        // Reprocess current byte; do not consume it yet.
                        --i;
                        continue;
                    }
                }
                if (c == '\r') {
                    pending_cr = true;
                    ++abs_pos;      // consumed CR
                    continue;
                }
                if (c == '\n') {
                    // LF-only newline: header ends before this LF (at abs_pos)
                    std::uint64_t end_excl_for_final = after_closing_quote ? pending_end_excl : abs_pos;
                    finalize_field(end_excl_for_final);
                    header_done = true;
                    ++abs_pos;      // consume LF
                    continue;
                }

                // Skip initial spaces/tabs at field start, and also after a closing quote
                if ((at_field_start || after_closing_quote) && (c == ' ' || c == '\t')) {
                    ++abs_pos;      // consume the whitespace
                    continue;       // still waiting for delimiter/newline or real data
                }
            } else {
                // In quotes: CR is data, not a pending CR
                if (pending_cr) { pending_cr = false; }
            }

            // Quote handling
            if (c == static_cast<unsigned char>(QUOTE)) {
                if (at_field_start && !in_quotes && !after_closing_quote) {
                    start_quoted(abs_pos);
                    ++abs_pos;
                    continue;
                }
                if (in_quotes) {
                    // Could be escaped-quote or closing-quote; decide on next byte (maybe next chunk)
                    pending_quote = true;
                    ++abs_pos;
                    continue;
                } else {
                    // Quote inside unquoted field OR after_closing_quote:
                    // treat as data only if  in an unquoted field (rare).
                    if (!after_closing_quote) {
                        start_unquoted_if_needed(abs_pos);
                        ++abs_pos;
                        continue;
                    }
                    // If after_closing_quote and  see a quote, that's malformed;
                    // ignore as data to avoid crashing.
                    ++abs_pos;
                    continue;
                }
            }

            // Delimiter ends field only when outside quotes
            if (!in_quotes && c == static_cast<unsigned char>(DELIM)) {
                std::uint64_t end_excl_for_final = after_closing_quote ? pending_end_excl : abs_pos;
                finalize_field(end_excl_for_final);
                ++abs_pos;          // consume delimiter
                continue;
            }

            // Regular data
            if (!in_quotes) {
                start_unquoted_if_needed(abs_pos);
            }
            ++abs_pos;              // consumed this data byte
        }
    }

    // EOF / finalize if no newline encountered yet
    if (!header_done) {
        if (pending_quote) {
            // File ended immediately after a quote in a quoted field: that quote closes the field.
            // The quote was at position (abs_pos - 1). Exclude it, then finalize once.
            in_quotes = false;
            after_closing_quote = true;
            pending_end_excl = abs_pos - 1;
        }
        if (pending_cr && !in_quotes) {
            // File ended right after CR outside quotes -> treat CR as newline
            std::uint64_t end_excl_for_final = after_closing_quote ? pending_end_excl : (abs_pos - 1);
            finalize_field(end_excl_for_final);
        } else {
            // EOF without newline: field runs to EOF, unless  already closed on a quote
            std::uint64_t end_excl_for_final = after_closing_quote ? pending_end_excl : abs_pos;
            finalize_field(end_excl_for_final);
        }
    }

    binaryOutput.flush();
    return true;
}


// Returns 0-based index for 'name' or -1 if not found.
// Compares RAW BYTES [start,end) from the CSV to 'name' (no unescaping).
int TabularData::getColumnIndex(const std::string& name) const {
    if (bin_path_.empty() || csv_path_.empty()) { return -1; }

    std::ifstream bin(bin_path_, std::ios::binary);
    if (!bin) { return -1; }

    std::ifstream csv(csv_path_, std::ios::binary);
    if (!csv) { return -1; }

    std::uint64_t start = 0, end = 0;
    int idx = 0;

    while (bin.read(reinterpret_cast<char*>(&start), sizeof(std::uint64_t))) {
        if (!bin.read(reinterpret_cast<char*>(&end), sizeof(std::uint64_t))) { break; }
        if (end < start) { return -1; } // corrupt

        const std::uint64_t len = end - start;
        std::string buffer;
        buffer.resize(static_cast<size_t>(len));

        if (len > 0) {
            csv.clear();
            csv.seekg(static_cast<std::streamoff>(start), std::ios::beg);
            if (!csv.read(&buffer[0], static_cast<std::streamsize>(len))) {
                return -1;
            }
        }

        if (buffer == name) { return idx; } 
        ++idx;
    }
    return -1;
}

// Fetch raw header bytes for the given column index (quotes already excluded by offsets).
std::string TabularData::getColumnHeader(int columnIndex) const {
    if (columnIndex < 0) { return {}; }
    if (bin_path_.empty() || csv_path_.empty()) { return {}; }

    std::ifstream bin(bin_path_, std::ios::binary);
    if (!bin) { return {}; }

    // Each record is 16 bytes: [uint64 start][uint64 end]
    const std::uint64_t pair_offset = static_cast<std::uint64_t>(columnIndex) * 16ULL;
    bin.seekg(static_cast<std::streamoff>(pair_offset), std::ios::beg);
    if (!bin) { return {}; }

    std::uint64_t start = 0, end = 0;
    if (!bin.read(reinterpret_cast<char*>(&start), sizeof(std::uint64_t))) { return {}; }
    if (!bin.read(reinterpret_cast<char*>(&end), sizeof(std::uint64_t))) { return {}; }
    if (end < start) { return {}; }

    const std::uint64_t len = end - start;
    std::string buffer;
    buffer.resize(static_cast<size_t>(len));
    if (len == 0) { return buffer; } // empty header

    std::ifstream csv(csv_path_, std::ios::binary);
    if (!csv) { return {}; }
    csv.seekg(static_cast<std::streamoff>(start), std::ios::beg);
    if (!csv.read(&buffer[0], static_cast<std::streamsize>(len))) { return {}; }

    return buffer; // raw bytes (doubled quotes remain doubled if the field was quoted)
}

#include "create_offsets_file.cpp"
