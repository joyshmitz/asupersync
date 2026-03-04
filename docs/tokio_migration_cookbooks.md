# Domain-Specific Migration Cookbooks

**Bead**: `asupersync-2oh2u.11.2` ([T9.2])
**Program**: `asupersync-2oh2u` ([TOKIO-REPLACE])
**Author**: SapphireHill (claude-code / opus-4.6)
**Date**: 2026-03-04
**Dependencies**: `asupersync-2oh2u.10.13`, `asupersync-2oh2u.2.10`, `asupersync-2oh2u.11.1`,
  `asupersync-2oh2u.7.11`, `asupersync-2oh2u.6.13`, `asupersync-2oh2u.5.12`,
  `asupersync-2oh2u.4.11`, `asupersync-2oh2u.3.10`, `asupersync-2oh2u.7.9`,
  `asupersync-2oh2u.6.11`, `asupersync-2oh2u.5.10`
**Purpose**: Provide end-to-end migration cookbooks for each capability domain,
with concrete before/after examples, anti-patterns, failure-mode guidance, and
structured-log expectations.

---

## 1. Scope

This document covers domain-specific cookbooks for all 6 replacement tracks:

| Track | Domain | Prerequisite Docs |
|-------|--------|-------------------|
| T2 | Async I/O + Codec | tokio_io_parity_audit, e2e logging |
| T3 | fs/process/signal | tokio_fs_process_signal_migration_playbook |
| T4 | QUIC/H3 + networking | quic_h3_forensic_log_schema |
| T5 | Web/gRPC/middleware | tokio_web_grpc_migration_runbook |
| T6 | Database/messaging | tokio_db_messaging_migration_packs |
| T7 | Interop adapters | tokio_interop_support_matrix |

Prerequisites:
- `asupersync-2oh2u.10.13` (golden log corpus)
- `asupersync-2oh2u.2.10` (I/O e2e logging)
- `asupersync-2oh2u.11.1` (migration strategy framework)

---

## 2. Cookbook Structure

Each domain cookbook follows a uniform structure:

1. **Domain Overview** - what is being migrated
2. **Migration Recipes** - step-by-step procedures
3. **Before/After Examples** - concrete code transformations
4. **Anti-Patterns** - what NOT to do
5. **Failure Modes** - known risks and mitigations
6. **Log Expectations** - structured logging requirements
7. **Evidence Links** - test files, docs, golden fixtures

---

## 3. Track T2: Async I/O + Codec Cookbook

### 3.1 Domain Overview

Replace tokio I/O primitives (AsyncRead, AsyncWrite, codecs) with asupersync
equivalents while preserving backpressure semantics and zero-copy patterns.

### 3.2 Key Recipes

| Recipe | From | To | Notes |
|--------|------|-----|-------|
| R2-01 | tokio::io::AsyncRead | asupersync::io::AsyncRead | Trait-compatible |
| R2-02 | tokio::io::AsyncWrite | asupersync::io::AsyncWrite | Trait-compatible |
| R2-03 | tokio_util::codec | asupersync::codec | Encoder/Decoder traits |
| R2-04 | tokio::io::copy | asupersync::io::copy | Zero-copy when possible |
| R2-05 | tokio::net::TcpStream | asupersync::net::TcpStream | Direct replacement |

### 3.3 Before/After

```rust
// Before: tokio
use tokio::io::{AsyncReadExt, AsyncWriteExt};
let mut stream = tokio::net::TcpStream::connect("127.0.0.1:8080").await?;
stream.write_all(b"hello").await?;

// After: asupersync
use asupersync::io::{AsyncReadExt, AsyncWriteExt};
let mut stream = asupersync::net::TcpStream::connect("127.0.0.1:8080").await?;
stream.write_all(b"hello").await?;
```

### 3.4 Anti-Patterns

- AP-T2-01: Using std::io blocking calls inside async context
- AP-T2-02: Ignoring codec frame boundaries in streaming protocols
- AP-T2-03: Unbounded read buffers without backpressure

### 3.5 Failure Modes

| Failure | Symptom | Mitigation |
|---------|---------|------------|
| FM-T2-01 | Half-open TCP after migration | Enable keepalive; use read timeout |
| FM-T2-02 | Codec state corruption on cancel | Use cancel-safe codec wrapper |
| FM-T2-03 | Zero-copy path regresses to copy | Profile with tracing; check vectored I/O support |

### 3.6 Edge Cases

- Partial reads returning 0 bytes (EOF vs WouldBlock)
- Codec decode returning `None` on incomplete frame without error
- Write returning `Ok(0)` when kernel buffer is full
- Simultaneous read+write cancellation on duplex streams

### 3.7 Rollback Decision Points

| Checkpoint | Rollback Criterion | Action |
|-----------|-------------------|--------|
| After R2-01/02 migration | Latency p99 > 2x baseline | Revert to tokio traits |
| After R2-03 codec migration | Frame corruption rate > 0 | Revert codec, file bug |
| After R2-05 TcpStream swap | Connection failure rate > 1% | Revert to tokio::net |

### 3.8 Log Expectations

All I/O e2e tests must emit logs with: schema_version, scenario_id, correlation_id,
phase, outcome, detail, replay_pointer. See golden corpus `t2_io_e2e_success.json`.

---

## 4. Track T3: fs/process/signal Cookbook

### 4.1 Domain Overview

Replace tokio fs, process, and signal primitives with asupersync equivalents,
preserving POSIX signal safety and process lifecycle management.

### 4.2 Key Recipes

| Recipe | From | To | Notes |
|--------|------|-----|-------|
| R3-01 | tokio::fs::read | asupersync::fs::read | Async file I/O |
| R3-02 | tokio::fs::write | asupersync::fs::write | Atomic write option |
| R3-03 | tokio::process::Command | asupersync::process::Command | Structured exit |
| R3-04 | tokio::signal::ctrl_c | asupersync::signal::ctrl_c | Signal handler |
| R3-05 | tokio::fs::metadata | asupersync::fs::metadata | TOCTOU-aware |

### 4.3 Before/After

```rust
// Before: tokio
let contents = tokio::fs::read_to_string("config.toml").await?;

// After: asupersync
let contents = asupersync::fs::read_to_string("config.toml").await?;
```

### 4.4 Anti-Patterns

- AP-T3-01: Holding file locks across await points
- AP-T3-02: Ignoring process exit codes in pipeline
- AP-T3-03: Signal handler that allocates (not async-signal-safe)

### 4.5 Evidence

- Playbook: `docs/tokio_fs_process_signal_migration_playbook.md`
- E2E tests: `tests/tokio_fs_process_signal_e2e.rs`
- Unit tests: `tests/tokio_fs_process_signal_unit_test_matrix.rs`

---

## 5. Track T4: QUIC/H3 + Networking Cookbook

### 5.1 Domain Overview

Replace QUIC transport and HTTP/3 protocol handling with asupersync native
implementations, preserving RFC 9000/9114 compliance.

### 5.2 Key Recipes

| Recipe | From | To | Notes |
|--------|------|-----|-------|
| R4-01 | quinn::Endpoint | asupersync::net::quic::Endpoint | QUIC transport |
| R4-02 | h3::client | asupersync::http::h3::client | H3 client |
| R4-03 | h3::server | asupersync::http::h3::server | H3 server |
| R4-04 | quinn::Connection | asupersync::net::quic::Connection | Stream mux |
| R4-05 | QPACK encoder | asupersync::http::h3::qpack | Header compression |

### 5.3 Anti-Patterns

- AP-T4-01: Ignoring QUIC connection migration events
- AP-T4-02: Not handling 0-RTT replay attacks
- AP-T4-03: Hardcoded congestion control parameters

### 5.4 Evidence

- E2E tests: `tests/tokio_quic_h3_e2e_scenario_manifest.rs`
- Forensic log schema: `artifacts/quic_h3_forensic_log_schema_v1.json`

---

## 6. Track T5: Web/gRPC/Middleware Cookbook

### 6.1 Domain Overview

Replace axum/tonic/tower stack with asupersync web, gRPC, and service
layer equivalents. See dedicated runbook at
`docs/tokio_web_grpc_migration_runbook.md`.

### 6.2 Key Recipes

| Recipe | From | To | Notes |
|--------|------|-----|-------|
| R5-01 | axum::Router | asupersync::web::Router | HTTP routing |
| R5-02 | axum::extract::Json | asupersync::web::Json | Request extraction |
| R5-03 | tonic::transport::Server | asupersync::grpc::Server | gRPC server |
| R5-04 | tower::Layer | asupersync::service::Layer | Middleware |
| R5-05 | tonic-web | asupersync::grpc::web | gRPC-web bridge |

### 6.3 Anti-Patterns

- AP-T5-01: Direct tokio::spawn in request handlers (use regions)
- AP-T5-02: Missing correlation ID propagation through middleware chain
- AP-T5-03: Unbounded request body without max_body_size

### 6.4 Evidence

- Runbook: `docs/tokio_web_grpc_migration_runbook.md`
- Parity map: `docs/tokio_web_grpc_parity_map.md`
- E2E tests: `tests/web_grpc_e2e_service_scripts.rs`
- Unit tests: `tests/web_grpc_exhaustive_unit.rs`

---

## 7. Track T6: Database/Messaging Cookbook

### 7.1 Domain Overview

Replace database pooling (deadpool/bb8), PostgreSQL drivers, Redis clients,
and message broker adapters with asupersync equivalents.

### 7.2 Key Recipes

| Recipe | From | To | Notes |
|--------|------|-----|-------|
| R6-01 | deadpool::Pool | asupersync::service::pool::Pool | Connection pool |
| R6-02 | sqlx::PgPool | asupersync compatible pool | Postgres adapter |
| R6-03 | redis::Client | asupersync compatible client | Redis adapter |
| R6-04 | rdkafka::producer | asupersync compatible producer | Kafka adapter |
| R6-05 | nats::Client | asupersync compatible client | NATS adapter |

### 7.3 Anti-Patterns

- AP-T6-01: Leaking pooled connections (missing release on error paths)
- AP-T6-02: Unbounded message queue without backpressure
- AP-T6-03: Blocking database calls in async context

### 7.4 Evidence

- Migration packs: `docs/tokio_t6_migration_packs.md`
- Contract: `docs/tokio_db_messaging_migration_packs_contract.md`
- E2E tests: `tests/e2e_t6_data_path.rs`
- Unit tests: `tests/t6_database_messaging_unit_matrix.rs`

---

## 8. Track T7: Interop Adapters Cookbook

### 8.1 Domain Overview

Bridge tokio-locked ecosystem crates (hyper, reqwest, axum, tonic) via the
asupersync-tokio-compat adapter layer for incremental migration.

### 8.2 Key Recipes

| Recipe | From | To | Notes |
|--------|------|-----|-------|
| R7-01 | tokio::runtime | compat::TokioRuntime | Runtime bridge |
| R7-02 | tokio::spawn | compat::spawn_tokio | Task bridging |
| R7-03 | tokio::time::sleep | compat::sleep | Timer bridge |
| R7-04 | tokio::io traits | compat::io | I/O trait bridge |
| R7-05 | hyper::body | compat::body | HTTP body bridge |

### 8.3 Anti-Patterns

- AP-T7-01: Running both runtimes with separate thread pools (use bridge)
- AP-T7-02: Mixing tokio::spawn and asupersync::spawn without coordination
- AP-T7-03: Not propagating cancellation across runtime boundary

### 8.4 Evidence

- Support matrix: `docs/tokio_interop_support_matrix.md`
- Adapter arch: `docs/tokio_adapter_boundary_architecture.md`
- E2E tests: `tests/tokio_interop_e2e_scenarios.rs`
- Compat crate: `asupersync-tokio-compat/`

---

## 9. Cross-Cutting Concerns

### 9.1 Structured Logging

All cookbook recipes MUST produce structured logs conforming to the golden
corpus schema. See `tests/fixtures/logging_golden_corpus/manifest.json`.

### 9.2 Correlation ID Propagation

Every migration recipe must propagate correlation IDs from edge to database:
`edge -> middleware -> handler -> service -> adapter -> database/message broker`

### 9.3 Rollback Decision Points

Each recipe includes explicit rollback criteria:
- Latency regression > 2x baseline
- Error rate > 1%
- Test failure in T8.12 e2e logging gates

---

## 10. User-Friction Assumptions

| Assumption | Threshold | Validation Method |
|-----------|-----------|-------------------|
| Migration time per endpoint | < 30 min | T9.10 lab measurement |
| Zero downtime migration | Required | Canary deployment |
| Learning curve | < 2 hours for experienced Tokio dev | T9.10 user study |
| Compilation time regression | < 10% | CI benchmark |

---

## 11. CI Commands

```
rch exec -- cargo test --test tokio_migration_cookbook_enforcement -- --nocapture
```

---

## 12. Downstream Binding

This document is a prerequisite for:
- `asupersync-2oh2u.11.10` (T9.10: user-journey migration labs)
