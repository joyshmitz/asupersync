//! HTTP/1 client regression tests.

#![allow(clippy::items_after_statements)]

#[macro_use]
mod common;

use asupersync::http::h1::{Http1Client, Method, Request, Version};
use asupersync::http::h1::{Http1Server, HttpClient, Response};
use asupersync::io::{AsyncReadExt, AsyncWriteExt};
use asupersync::net::TcpListener as AsyncTcpListener;
use asupersync::net::TcpStream;
use asupersync::time::timeout;
use asupersync::types::Time;
use common::*;
use futures_lite::future::block_on;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// Regression: `Http1Client::request` must flush encoded request bytes before waiting on a
/// response, otherwise the server never receives the request and both sides hang until timeout.
#[test]
fn http1_client_request_flushes_request_bytes() {
    init_test_logging();
    test_phase!("http1_client_request_flushes_request_bytes");

    let timeout_duration = Duration::from_secs(5);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    listener
        .set_nonblocking(true)
        .expect("set_nonblocking listener");
    let addr = listener.local_addr().expect("listener local_addr");

    let server = thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let deadline = Instant::now() + timeout_duration;
        let (mut conn, _peer) = loop {
            match listener.accept() {
                Ok(value) => break value,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "server accept timed out",
                        ));
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(err) => return Err(err),
            }
        };

        conn.set_read_timeout(Some(timeout_duration))?;
        conn.set_write_timeout(Some(timeout_duration))?;

        let mut buf = Vec::with_capacity(2048);
        let mut scratch = [0u8; 1024];
        loop {
            let n = conn.read(&mut scratch)?;
            if n == 0 {
                break;
            }

            buf.extend_from_slice(&scratch[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }

        conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")?;
        conn.flush()?;

        Ok(buf)
    });

    run_test(|| async move {
        let stream = TcpStream::connect(addr).await.expect("client connect");

        let req = Request {
            method: Method::Get,
            uri: "/".to_owned(),
            version: Version::Http11,
            headers: vec![("Host".to_owned(), addr.to_string())],
            body: Vec::new(),
            trailers: Vec::new(),
            peer_addr: None,
        };

        let fut = Box::pin(Http1Client::request(stream, req));
        let resp = timeout(Time::ZERO, timeout_duration, fut)
            .await
            .expect("client request timed out")
            .expect("client request errored");

        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"OK");
    });

    let raw = server
        .join()
        .expect("server thread panicked")
        .expect("server io error");
    let raw_str = String::from_utf8_lossy(&raw);

    assert!(
        raw_str.starts_with("GET / HTTP/1.1\r\n"),
        "expected request line, got: {raw_str:?}"
    );

    test_complete!("http1_client_request_flushes_request_bytes");
}

fn read_until_headers_end(stream: &mut std::net::TcpStream) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut scratch = [0u8; 256];

    loop {
        let n = stream.read(&mut scratch)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&scratch[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    Ok(buf)
}

#[test]
fn http_client_connect_tunnel_end_to_end_roundtrip() {
    init_test_logging();
    test_phase!("http_client_connect_tunnel_end_to_end_roundtrip");

    let timeout_duration = Duration::from_secs(5);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    listener
        .set_nonblocking(true)
        .expect("set_nonblocking listener");
    let addr = listener.local_addr().expect("listener local_addr");

    let proxy = thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let deadline = Instant::now() + timeout_duration;
        let (mut conn, _peer) = loop {
            match listener.accept() {
                Ok(value) => break value,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "proxy accept timed out",
                        ));
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(err) => return Err(err),
            }
        };

        conn.set_read_timeout(Some(timeout_duration))?;
        conn.set_write_timeout(Some(timeout_duration))?;

        let request = read_until_headers_end(&mut conn)?;
        conn.write_all(b"HTTP/1.1 200 Connection Established\r\nProxy-Agent: test\r\n\r\nHELLO")?;
        conn.flush()?;

        let mut tunneled = [0u8; 4];
        conn.read_exact(&mut tunneled)?;
        if tunneled != *b"PING" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected tunnel payload",
            ));
        }

        conn.write_all(b"PONG")?;
        conn.flush()?;

        Ok(request)
    });

    run_test(|| async move {
        let client = HttpClient::new();
        let mut tunnel = client
            .connect_tunnel(
                &format!("http://{addr}"),
                "example.com:443",
                vec![("X-Test-Trace".to_string(), "h4".to_string())],
            )
            .await
            .expect("connect tunnel should succeed");

        assert_eq!(tunnel.prefetched_len(), 5);

        let mut prefetched = [0u8; 5];
        tunnel
            .read_exact(&mut prefetched)
            .await
            .expect("read prefetched bytes");
        assert_eq!(&prefetched, b"HELLO");

        tunnel
            .write_all(b"PING")
            .await
            .expect("write tunnel payload");
        tunnel.flush().await.expect("flush tunnel payload");

        let mut echoed = [0u8; 4];
        tunnel
            .read_exact(&mut echoed)
            .await
            .expect("read tunneled echo");
        assert_eq!(&echoed, b"PONG");
    });

    let request = proxy
        .join()
        .expect("proxy thread panicked")
        .expect("proxy io failed");
    let request = String::from_utf8_lossy(&request);

    assert!(request.starts_with("CONNECT example.com:443 HTTP/1.1\r\n"));
    assert!(
        request.contains("X-Test-Trace: h4\r\n"),
        "missing custom header in CONNECT request: {request:?}"
    );

    test_complete!("http_client_connect_tunnel_end_to_end_roundtrip");
}

#[test]
fn http1_server_expect_100_continue_full_flow() {
    init_test_logging();
    test_phase!("http1_server_expect_100_continue_full_flow");

    let timeout_duration = Duration::from_secs(5);
    let (addr_tx, addr_rx) = mpsc::channel();

    let server = thread::spawn(move || -> std::io::Result<()> {
        block_on(async {
            let listener = AsyncTcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            addr_tx.send(addr).expect("send server addr");

            let (stream, _) = listener.accept().await?;
            let server = Http1Server::new(|req| async move {
                assert_eq!(req.body, b"hello");
                Response::new(200, "OK", b"done".to_vec())
            });
            let state = server
                .serve(stream)
                .await
                .map_err(|err| std::io::Error::other(err.to_string()))?;

            if state.requests_served != 1 {
                return Err(std::io::Error::other(format!(
                    "expected exactly one served request, got {}",
                    state.requests_served
                )));
            }

            Ok(())
        })
    });

    let addr = addr_rx
        .recv_timeout(timeout_duration)
        .expect("receive server addr");
    let mut client = std::net::TcpStream::connect(addr).expect("client connect");
    client
        .set_read_timeout(Some(timeout_duration))
        .expect("set read timeout");
    client
        .set_write_timeout(Some(timeout_duration))
        .expect("set write timeout");

    client
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
        )
        .expect("write request");
    client.flush().expect("flush request");

    let mut final_bytes = Vec::new();
    client
        .read_to_end(&mut final_bytes)
        .expect("read final response");
    let final_text = String::from_utf8_lossy(&final_bytes);

    let continue_idx = final_text
        .find("HTTP/1.1 100 Continue\r\n")
        .expect("expected 100-continue response");
    let ok_idx = final_text
        .find("HTTP/1.1 200 OK\r\n")
        .expect("expected final 200 response");
    assert!(
        continue_idx < ok_idx,
        "expected 100-continue before final response, got: {final_text:?}"
    );

    assert!(
        final_text.contains("\r\n\r\ndone"),
        "expected final body payload, got: {final_text:?}"
    );

    server
        .join()
        .expect("server thread panicked")
        .expect("server io failed");

    test_complete!("http1_server_expect_100_continue_full_flow");
}

#[test]
fn http1_server_rejects_unsupported_expectation() {
    init_test_logging();
    test_phase!("http1_server_rejects_unsupported_expectation");

    let timeout_duration = Duration::from_secs(5);
    let (addr_tx, addr_rx) = mpsc::channel();
    let handler_called = Arc::new(AtomicBool::new(false));
    let handler_called_server = Arc::clone(&handler_called);

    let server = thread::spawn(move || -> std::io::Result<()> {
        block_on(async {
            let listener = AsyncTcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            addr_tx.send(addr).expect("send server addr");

            let (stream, _) = listener.accept().await?;
            let called = Arc::clone(&handler_called_server);
            let server = Http1Server::new(move |_req| {
                let called = Arc::clone(&called);
                async move {
                    called.store(true, Ordering::SeqCst);
                    Response::new(200, "OK", b"unexpected".to_vec())
                }
            });

            let state = server
                .serve(stream)
                .await
                .map_err(|err| std::io::Error::other(err.to_string()))?;

            if state.requests_served != 1 {
                return Err(std::io::Error::other(format!(
                    "expected exactly one served request, got {}",
                    state.requests_served
                )));
            }

            Ok(())
        })
    });

    let addr = addr_rx
        .recv_timeout(timeout_duration)
        .expect("receive server addr");
    let mut client = std::net::TcpStream::connect(addr).expect("client connect");
    client
        .set_read_timeout(Some(timeout_duration))
        .expect("set read timeout");
    client
        .set_write_timeout(Some(timeout_duration))
        .expect("set write timeout");

    client
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: localhost\r\nExpect: fancy-feature\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .expect("write request");
    client.flush().expect("flush request");

    let mut response = Vec::new();
    client.read_to_end(&mut response).expect("read response");
    let response = String::from_utf8_lossy(&response);

    assert!(
        response.contains("HTTP/1.1 417 Expectation Failed\r\n"),
        "expected 417 response, got: {response:?}"
    );
    assert!(
        !handler_called.load(Ordering::SeqCst),
        "request handler should not run for rejected expectation"
    );

    server
        .join()
        .expect("server thread panicked")
        .expect("server io failed");

    test_complete!("http1_server_rejects_unsupported_expectation");
}
