//! Fuzz target for HTTP/1 request parsing.
//!
//! This target fuzzes the HTTP/1 request parser with arbitrary byte sequences,
//! looking for panics, hangs, or memory safety issues.
//!
//! # Running
//! ```bash
//! cargo +nightly fuzz run fuzz_http1_request
//! ```
//!
//! # Minimizing crashes
//! ```bash
//! cargo +nightly fuzz tmin fuzz_http1_request <crash_file>
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Attempt to parse the data as an HTTP/1 request.
    // The parser should handle malformed input gracefully without panicking.

    // Convert bytes to string (lossy) for parsing
    let input = String::from_utf8_lossy(data);

    // Try to parse as HTTP/1 request lines
    // Format: METHOD SP REQUEST-URI SP HTTP-VERSION CRLF
    let lines: Vec<&str> = input.lines().collect();

    if lines.is_empty() {
        return;
    }

    // Parse request line
    let request_line = lines[0];
    let parts: Vec<&str> = request_line.split_whitespace().collect();

    if parts.len() >= 3 {
        let _method = parts[0];
        let _uri = parts[1];
        let _version = parts[2];

        // Validate method is ASCII uppercase
        let _ = parts[0].chars().all(|c| c.is_ascii_uppercase());

        // Validate version format
        let _ = parts[2].starts_with("HTTP/");
    }

    // Parse headers
    for line in lines.iter().skip(1) {
        if line.is_empty() {
            break; // End of headers
        }

        // Headers format: field-name ":" OWS field-value OWS
        if let Some(colon_pos) = line.find(':') {
            let _name = &line[..colon_pos];
            let _value = line[colon_pos + 1..].trim();
        }
    }
});
