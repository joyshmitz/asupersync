# WASM TypeScript Package Topology Contract

Contract ID: `wasm-typescript-package-topology-v1`  
Bead: `asupersync-umelq.9.1`

## Purpose

Define deterministic TypeScript package boundaries for Browser Edition so users
can adopt a layered API without hidden semantic drift.

## Canonical Inputs

- Policy: `.github/wasm_typescript_package_policy.json`
- Gate script: `scripts/check_wasm_typescript_package_policy.py`
- Onboarding runner: `scripts/run_browser_onboarding_checks.py`

## Package Topology

Required package set:

1. `@asupersync/browser-core`
2. `@asupersync/browser`
3. `@asupersync/react`
4. `@asupersync/next`

Layer contract:

1. `@asupersync/browser-core` owns low-level runtime and type surface contracts.
2. `@asupersync/browser` owns high-level SDK semantics and diagnostics surface.
3. `@asupersync/react` and `@asupersync/next` are adapter layers over
   `@asupersync/browser`.
4. Public exports must be tree-shake safe and must not expose
   `./internal/*` or `./native/*` subpaths.

## Rust Crate Layout and Artifact Provenance

The TypeScript package topology is layered over a separate Rust crate layout.

| Surface | Role | Artifact rule |
|---------|------|---------------|
| `asupersync` | Portable runtime core and canonical ABI contract owner | Rust `rlib`; no direct `wasm-bindgen` exports |
| `asupersync-browser-core` | Browser WASM producer crate | sole `cdylib`/`rlib` bindings crate; wraps the ABI dispatcher in `src/types/wasm_abi.rs` |
| `packages/browser-core/` | Published JS/WASM package root for `@asupersync/browser-core` | assembled from staged bindgen output plus package metadata |
| `packages/browser/`, `packages/react/`, `packages/next/` | Higher-level JS/TS packages | consume `@asupersync/browser-core`; no additional Rust producer crate |

Artifact provenance rules:

1. `asupersync-browser-core` is the only crate that emits the concrete browser
   WASM/JS boundary.
2. Bindgen output is staged under `pkg/browser-core/<profile>/` and is
   ephemeral build output, not the committed package source of truth.
3. `packages/browser-core/` is the package-assembly destination that receives
   artifacts from `pkg/browser-core/<profile>/`.
4. The root `asupersync` crate remains the source of truth for ABI symbols,
   compatibility policy, and dispatcher semantics.

## Next.js Boundary Strategy and Fallback Contract (WASM-10 / `asupersync-umelq.11.3`)

Source-of-truth runtime mapping lives in `src/types/wasm_abi.rs`:

- `NextjsRenderEnvironment::boundary_mode()`
- `NextjsRenderEnvironment::runtime_fallback()`
- `NextjsRenderEnvironment::runtime_fallback_reason()`

Boundary strategy:

1. `client` boundary:
   - environments: `client_ssr`, `client_hydrated`
   - direct runtime execution is allowed only in `client_hydrated`
2. `server` boundary:
   - environments: `server_component`, `node_server`
   - runtime execution is not allowed; use serialized server bridge
3. `edge` boundary:
   - environment: `edge_runtime`
   - runtime execution is not allowed; use serialized edge bridge

Deterministic fallback matrix:

| Render environment | Boundary mode | `supports_wasm_runtime` | Fallback policy | Required behavior |
|---|---|---|---|---|
| `client_hydrated` | `client` | `true` | `none_required` | execute runtime directly |
| `client_ssr` | `client` | `false` | `defer_until_hydrated` | defer runtime init until hydration completes |
| `server_component` | `server` | `false` | `use_server_bridge` | route operation through serialized server companion |
| `node_server` | `server` | `false` | `use_server_bridge` | route operation through serialized server companion |
| `edge_runtime` | `edge` | `false` | `use_edge_bridge` | route operation through serialized edge companion |

Mixed deployment guidance:

1. Keep runtime handles in client-only scope; never pass `WasmHandleRef` through server actions.
2. Treat server/edge requests as bridge requests and return serialized outcomes only.
3. Keep cancellation ownership in the originating client scope; bridge calls must return explicit cancel-compatible status instead of hidden retries.
4. If fallback path is selected, emit structured diagnostics with:
   - `boundary_mode`
   - `render_environment`
   - `runtime_fallback`
   - `repro_command`

Compatibility caveats:

1. `edge_runtime` does not imply `node_apis`; avoid Node-only adapters in edge mode.
2. `client_ssr` has browser hooks surface but no runtime initialization authority.
3. Runtime calls in non-hydrated/non-client boundaries must fail closed to fallback policy; no ambient execution escape hatches.

## Type Surface Ownership

Required symbols and package owners:

1. `Outcome` -> `@asupersync/browser-core`
2. `Budget` -> `@asupersync/browser-core`
3. `CancellationToken` -> `@asupersync/browser`
4. `RegionHandle` -> `@asupersync/browser`

Any symbol owner outside the declared package topology fails policy.

## Resolution Matrix and E2E Command Contract

The policy encodes deterministic install-and-run command pairs for:

1. Vanilla TypeScript (ESM + CJS)
2. React (ESM + CJS)
3. Next.js (ESM + CJS)

Each scenario defines:

1. `entrypoint`
2. `module_mode`
3. `bundler`
4. `adapter_path`
5. `runtime_profile`
6. `install_command`
7. `run_command`

Coverage gates:

1. Required frameworks: `vanilla-ts`, `react`, `next`
2. Required module modes: `esm`, `cjs`
3. Required bundlers: `vite`, `webpack`, `next-turbopack`

## Structured Logging Contract

Onboarding and policy logs must include:

1. `scenario_id`
2. `step_id`
3. `package_entrypoint`
4. `adapter_path`
5. `runtime_profile`
6. `diagnostic_category`
7. `outcome`
8. `artifact_log_path`
9. `repro_command`

`run_browser_onboarding_checks.py` emits these fields per step so onboarding
failures are diagnosable by package boundary and adapter lane.

## Gate Outputs

- Summary JSON: `artifacts/wasm_typescript_package_summary.json`
- NDJSON log: `artifacts/wasm_typescript_package_log.ndjson`

## Repro Commands

Self-test:

```bash
python3 scripts/check_wasm_typescript_package_policy.py --self-test
```

Full policy gate:

```bash
python3 scripts/check_wasm_typescript_package_policy.py \
  --policy .github/wasm_typescript_package_policy.json
```

Framework-scoped checks (used by onboarding runner):

```bash
python3 scripts/check_wasm_typescript_package_policy.py \
  --policy .github/wasm_typescript_package_policy.json \
  --only-scenario TS-PKG-VANILLA-ESM \
  --only-scenario TS-PKG-VANILLA-CJS

python3 scripts/check_wasm_typescript_package_policy.py \
  --policy .github/wasm_typescript_package_policy.json \
  --only-scenario TS-PKG-REACT-ESM \
  --only-scenario TS-PKG-REACT-CJS

python3 scripts/check_wasm_typescript_package_policy.py \
  --policy .github/wasm_typescript_package_policy.json \
  --only-scenario TS-PKG-NEXT-ESM \
  --only-scenario TS-PKG-NEXT-CJS
```
