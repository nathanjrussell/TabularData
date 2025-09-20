#include "TabularData.h"

#include <fstream>
#include <iostream>
#include <vector>

// JSON escaping for a single byte
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

// Convenience overload (default options + optional output path)
bool TabularData::parseHeaderFromCsv(const std::string& csv_path,
                                     const std::string& out_json_path) {
    CsvOptions opt; // use default member initializers
    return parseHeaderFromCsv(csv_path, out_json_path, opt);
}

// Core implementation
bool TabularData::parseHeaderFromCsv(const std::string& csv_path,
                                     const std::string& out_json_path,
                                     const CsvOptions& opt) {
    num_columns_ = 0;

    std::ifstream in(csv_path, std::ios::binary);
    if (!in) {
        std::cerr << "TabularData: cannot open input: " << csv_path << "\n";
        return false;
    }
    std::ofstream out(out_json_path, std::ios::binary);
    if (!out) {
        std::cerr << "TabularData: cannot open output: " << out_json_path << "\n";
        return false;
    }

    // ---- Parser state (streaming, chunk-friendly) ----
    const char DELIM = opt.delimiter;
    const char QUOTE = opt.quote;

    bool in_quotes       = false;  // currently inside a quoted field?
    bool pending_quote   = false;  // last byte was QUOTE while in_quotes; need next byte to decide escaped vs close
    bool at_field_start  = true;   // before any bytes of the current field
    bool first_field_out = true;   // for JSON commas
    bool header_done     = false;  // newline (outside quotes) encountered
    bool pending_cr      = false;  // saw a CR outside quotes; waiting to see if next is LF

    auto open_json_field_if_needed = [&]() {
        if (at_field_start) {
            if (first_field_out) { out << "\""; first_field_out = false; }
            else                 { out << ",\""; }
            at_field_start = false;
        }
    };
    auto close_json_field_if_open = [&]() {
        if (!at_field_start) {
            out << "\"";
            at_field_start = true;
        } else {
            // field was empty; we still need to emit "" with correct comma handling
            if (first_field_out) { out << "\"\""; first_field_out = false; }
            else                 { out << ",\"\""; }
        }
        ++num_columns_;
    };

    // Begin JSON array
    out << "[";

    std::vector<char> buf(static_cast<size_t>(TABULARDATA_MAX_BUFFER_BYTES));

    while (!header_done && in) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = in.gcount();
        if (got <= 0) break;

        for (std::streamsize i = 0; i < got && !header_done; ++i) {
            unsigned char c = static_cast<unsigned char>(buf[static_cast<size_t>(i)]);

            // If a previous char was a quote while in_quotes, resolve it now
            if (pending_quote) {
                pending_quote = false;
                if (c == static_cast<unsigned char>(QUOTE)) {
                    // Escaped quote "" -> literal "
                    open_json_field_if_needed();
                    writeJsonEscapedChar(out, '\"');
                    // Still inside quoted field
                    continue;
                } else {
                    // The previous quote closed the quoted field.
                    in_quotes = false;
                    // Reprocess this current character with updated state.
                    --i;
                    continue;
                }
            }

            // Newline logic: only applies when not in quotes
            if (!in_quotes) {
                if (pending_cr) {
                    pending_cr = false;
                    if (c == '\n') {
                        // CRLF terminates header
                        close_json_field_if_open();
                        header_done = true;
                        continue;
                    } else {
                        // Lone CR terminates header; current byte belongs to next row
                        close_json_field_if_open();
                        header_done = true;
                        // Reprocess current byte as start of next row (ignored here)
                        --i;
                        continue;
                    }
                }
                if (c == '\r') { pending_cr = true; continue; }
                if (c == '\n') {
                    close_json_field_if_open();
                    header_done = true;
                    continue;
                }
            } else {
                // Inside quotes: any CR was data, not a pending CR
                if (pending_cr) pending_cr = false;
            }

            // Quote handling
            if (c == static_cast<unsigned char>(QUOTE)) {
                if (at_field_start && !in_quotes) {
                    // Start of a quoted field
                    open_json_field_if_needed(); // opens and writes leading quote in JSON
                    in_quotes = true;
                    continue;
                }
                if (in_quotes) {
                    // Could be escaped-quote or closing-quote; decide on next byte (may be in next chunk)
                    pending_quote = true;
                    continue;
                } else {
                    // Quote inside unquoted field: treat as literal
                    open_json_field_if_needed();
                    writeJsonEscapedChar(out, '\"');
                    continue;
                }
            }

            // Delimiter ends field only when not in quotes
            if (!in_quotes && c == static_cast<unsigned char>(DELIM)) {
                close_json_field_if_open();
                continue;
            }

            // Regular data byte
            open_json_field_if_needed();
            writeJsonEscapedChar(out, c);
        }
    }

    // Finalize at EOF or when loop exits without marking header_done
    if (!header_done) {
        if (pending_quote) {
            // No byte followed the quote: treat it as a closing quote
            in_quotes = false;
            pending_quote = false;
        }
        if (pending_cr && !in_quotes) {
            // CR at EOF counts as a newline
            pending_cr = false;
            close_json_field_if_open();
            header_done = true;
        } else {
            // No terminating newline: close current field if any bytes or if we already emitted something
            if (!first_field_out || !at_field_start) {
                // If field has content, close it; else emit an empty field
                if (!at_field_start) {
                    out << "\"";
                    at_field_start = true;
                    ++num_columns_;
                } else {
                    out << "\"\"";
                    ++num_columns_;
                }
            }
        }
    }

    // Close JSON array
    out << "]";
    out.flush();
    return true;
}
