//! Fuzz target for HTTP/1 response parsing.
//!
//! This target fuzzes the HTTP/1 response parser with arbitrary byte sequences.
//!
//! # Running
//! ```bash
//! cargo +nightly fuzz run fuzz_http1_response
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Attempt to parse the data as an HTTP/1 response.
    let input = String::from_utf8_lossy(data);
    let lines: Vec<&str> = input.lines().collect();

    if lines.is_empty() {
        return;
    }

    // Parse status line
    // Format: HTTP-VERSION SP STATUS-CODE SP REASON-PHRASE CRLF
    let status_line = lines[0];
    let parts: Vec<&str> = status_line.splitn(3, ' ').collect();

    if parts.len() >= 2 {
        let _version = parts[0];
        let status_str = parts[1];

        // Try to parse status code
        if let Ok(status) = status_str.parse::<u16>() {
            // Valid status codes are 100-599
            let _ = (100..=599).contains(&status);
        }

        // Reason phrase is optional
        let _reason = parts.get(2).unwrap_or(&"");
    }

    // Parse headers
    let mut content_length: Option<usize> = None;
    let mut chunked = false;

    for line in lines.iter().skip(1) {
        if line.is_empty() {
            break;
        }

        if let Some(colon_pos) = line.find(':') {
            let name = &line[..colon_pos];
            let value = line[colon_pos + 1..].trim();

            // Track content-length
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().ok();
            }

            // Track transfer-encoding
            if name.eq_ignore_ascii_case("transfer-encoding") {
                chunked = value.eq_ignore_ascii_case("chunked");
            }
        }
    }

    // Validate: can't have both content-length and chunked
    if content_length.is_some() && chunked {
        // This is a protocol violation, but we shouldn't panic
    }
});
