# WASM Bundler/Runtime Compatibility Matrix and Packaging Pipeline (WASM-9.4)

**Bead**: `asupersync-umelq.9.4`
**Parent**: WASM-08 TypeScript SDK, Packaging, and DX Guarantees
**Date**: 2026-03-02
**Author**: SapphireHill

---

## 1. Purpose

This document defines the concrete compatibility matrix between Asupersync Browser Edition WASM artifacts and common JavaScript bundlers/runtimes. It specifies expected behavior, known constraints, packaging pipeline stages, and deterministic validation procedures for each bundler lane.

---

## 2. Package Artifacts

Asupersync Browser Edition produces these artifacts per build profile:

| Artifact | Format | Purpose |
|----------|--------|---------|
| `asupersync_bg.wasm` | WebAssembly binary | Core runtime module |
| `asupersync.js` | ES module glue | Bindgen-generated JS bridge |
| `asupersync.d.ts` | TypeScript declarations | Type surface for consumers |
| `package.json` | npm package manifest | Dependency/entry metadata |

Entry points in `package.json`:

```json
{
  "main": "./asupersync.js",
  "module": "./asupersync.js",
  "types": "./asupersync.d.ts",
  "sideEffects": false,
  "exports": {
    ".": {
      "import": "./asupersync.js",
      "types": "./asupersync.d.ts"
    },
    "./wasm": "./asupersync_bg.wasm"
  }
}
```

---

## 3. Bundler Compatibility Matrix

### 3.1 Vite (5.x+)

| Property | Value |
|----------|-------|
| Module format | ESM (native) |
| WASM loading | `?init` import or `vite-plugin-wasm` |
| Tree shaking | Supported (`sideEffects: false`) |
| Top-level await | Supported (Vite 5+ default) |
| Dev server | HMR-compatible; WASM reload on change |
| Production build | Rollup-based; WASM inlined or split per config |
| Known constraints | WASM must be loaded async; synchronous init is not supported |

**Vite configuration requirements:**

```typescript
// vite.config.ts
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";

export default defineConfig({
  plugins: [wasm(), topLevelAwait()],
  optimizeDeps: {
    exclude: ["@asupersync/browser-core"]
  }
});
```

**Validation status**: Tier 1 (primary target)

### 3.2 Webpack (5.x+)

| Property | Value |
|----------|-------|
| Module format | ESM or CJS (configurable) |
| WASM loading | `asyncWebAssembly` experiment or `wasm-loader` |
| Tree shaking | Supported with `usedExports: true` |
| Top-level await | Requires `experiments.topLevelAwait: true` |
| Dev server | webpack-dev-server; manual reload for WASM changes |
| Production build | Chunk splitting; WASM as async chunk |
| Known constraints | CJS interop requires explicit `experiments.asyncWebAssembly` |

**Webpack configuration requirements:**

```javascript
// webpack.config.js
module.exports = {
  experiments: {
    asyncWebAssembly: true,
    topLevelAwait: true
  },
  module: {
    rules: [
      {
        test: /\.wasm$/,
        type: "webassembly/async"
      }
    ]
  }
};
```

**Validation status**: Tier 1 (primary target)

### 3.3 Turbopack (Next.js 14+)

| Property | Value |
|----------|-------|
| Module format | ESM |
| WASM loading | Built-in WASM support |
| Tree shaking | Supported |
| Top-level await | Supported |
| Dev server | Incremental; fast rebuild for WASM changes |
| Production build | Integrated with Next.js build pipeline |
| Known constraints | Configuration surface differs from Webpack; check `next.config.js` |

**Next.js configuration requirements:**

```javascript
// next.config.js
const nextConfig = {
  webpack: (config, { isServer }) => {
    config.experiments = {
      ...config.experiments,
      asyncWebAssembly: true,
      topLevelAwait: true
    };
    if (!isServer) {
      config.output.webassemblyModuleFilename = "static/wasm/[modulehash].wasm";
    }
    return config;
  }
};
```

**Validation status**: Tier 1 (primary target, via Next.js integration)

### 3.4 esbuild (0.20+)

| Property | Value |
|----------|-------|
| Module format | ESM or CJS |
| WASM loading | Manual `WebAssembly.instantiate` or plugin |
| Tree shaking | Supported |
| Top-level await | Supported in ESM output |
| Dev server | Not built-in; use separate server |
| Production build | Fast single-pass; WASM as external or loader |
| Known constraints | No native WASM experiment flag; requires explicit loader or plugin |

**esbuild configuration requirements:**

```javascript
// esbuild.config.js
import esbuild from "esbuild";
import { wasmLoader } from "esbuild-plugin-wasm";

await esbuild.build({
  entryPoints: ["src/index.ts"],
  bundle: true,
  format: "esm",
  outdir: "dist",
  plugins: [wasmLoader()],
  target: "es2022"
});
```

**Validation status**: Tier 2 (secondary target; manual WASM loading)

---

## 4. Runtime Compatibility Matrix

### 4.1 Browser Runtimes

| Runtime | WASM Support | Top-level Await | Notes |
|---------|-------------|-----------------|-------|
| Chrome 119+ | Full | Yes | Primary target |
| Firefox 120+ | Full | Yes | Primary target |
| Safari 17+ | Full | Yes | Test with WebKit quirks |
| Edge 119+ | Full | Yes | Chromium-based; same as Chrome |

### 4.2 Server-Side Runtimes

| Runtime | WASM Support | Notes |
|---------|-------------|-------|
| Node.js 20+ | `WebAssembly.instantiate` | SSR/prerender only; no runtime execution |
| Deno 1.40+ | Native WASM | Not primary target |
| Bun 1.0+ | Native WASM | Not primary target |

Server-side runtimes are bridge-only contexts per the Next.js boundary strategy (see `docs/wasm_typescript_package_topology.md`). WASM runtime execution is restricted to client-hydrated boundaries.

---

## 5. Module Format Compatibility

| Format | Bundlers | Notes |
|--------|----------|-------|
| ESM (`import`/`export`) | All (Vite, Webpack, Turbopack, esbuild) | Primary format; required for tree shaking |
| CJS (`require`/`module.exports`) | Webpack, esbuild | Legacy support; requires bundler CJS-ESM interop |
| IIFE | None (not produced) | Not supported; use ESM with bundler |
| UMD | None (not produced) | Not supported; use ESM with bundler |

### 5.1 ESM Requirements

- `"type": "module"` in package.json (or `.mjs` extension).
- Bundler must support `import()` for async WASM loading.
- Top-level await required for synchronous-looking WASM init.

### 5.2 CJS Interop

- Webpack: `experiments.asyncWebAssembly` enables CJS WASM loading.
- esbuild: requires explicit WASM loader plugin.
- Vite/Turbopack: CJS not natively supported; consumers must use ESM.

---

## 6. Packaging Pipeline

### 6.1 Build Stages

```
Stage 1: Profile Selection
  ↓ Select wasm-browser-{dev,prod,deterministic,minimal}
Stage 2: Rust Compilation
  ↓ cargo build --target wasm32-unknown-unknown --features <profile>
Stage 3: Bindgen Generation
  ↓ wasm-bindgen --target web --out-dir pkg/
Stage 4: Optimization (prod only)
  ↓ wasm-opt -Oz pkg/asupersync_bg.wasm -o pkg/asupersync_bg.wasm
Stage 5: Type Generation
  ↓ TypeScript declarations from wasm-bindgen + manual augmentation
Stage 6: Package Assembly
  ↓ Copy artifacts to package directories per topology
Stage 7: Validation
  ↓ Run compatibility matrix checks
Stage 8: Publishing
  ↓ npm publish per package (@asupersync/browser-core, etc.)
```

### 6.2 Profile-to-Package Mapping

| Profile | Optimization | Package channel |
|---------|-------------|-----------------|
| `wasm-browser-dev` | Debug; no wasm-opt | `nightly` |
| `wasm-browser-prod` | Release + wasm-opt -Oz | `canary` / `stable` |
| `wasm-browser-deterministic` | Release; deterministic trace | `nightly` (replay validation) |
| `wasm-browser-minimal` | Release; minimal surface | Contract-only checks |

### 6.3 Validation Commands

```bash
# Profile compilation gates (via rch)
rch exec -- cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm-browser-dev

rch exec -- cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm-browser-prod

rch exec -- cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm-browser-deterministic

rch exec -- cargo check --target wasm32-unknown-unknown \
  --no-default-features --features wasm-browser-minimal

# Package policy gate
python3 scripts/check_wasm_typescript_package_policy.py \
  --policy .github/wasm_typescript_package_policy.json

# Bundler compatibility checks
cargo test --test wasm_bundler_compatibility -- --nocapture
```

---

## 7. Packaging Invariants

1. **Single profile per build**: exactly one `wasm-browser-*` profile is active per artifact set; multi-profile builds are compile-error rejected.
2. **Tree-shake safe**: all packages declare `"sideEffects": false`; no hidden global state in module scope.
3. **Async WASM init**: WASM module loading is always async; no synchronous `WebAssembly.instantiateStreaming` fallback.
4. **ABI version embedded**: `WASM_ABI_MAJOR_VERSION` and `WASM_ABI_MINOR_VERSION` are accessible at runtime for version negotiation.
5. **Fingerprint guard**: `WASM_ABI_SIGNATURE_FINGERPRINT_V1` is checked at build time; drift without policy update is a gate failure.
6. **No native leakage**: native-only modules (`io-uring`, `tls`, `sqlite`, `postgres`, `mysql`, `kafka`) are compile-error rejected on wasm32.
7. **Deterministic output**: given the same source and profile, the build pipeline produces byte-identical artifacts (modulo wasm-opt non-determinism, which is tracked).

---

## 8. Known Issues and Workarounds

| Issue | Bundler | Workaround |
|-------|---------|------------|
| WASM streaming compilation requires CORS headers | All | Serve `.wasm` with `Content-Type: application/wasm` and CORS headers |
| Large WASM binary (>4MB) causes slow initial load | All | Use `wasm-opt -Oz` (prod profile); consider lazy loading |
| Webpack CJS mode requires explicit experiment flag | Webpack | Set `experiments.asyncWebAssembly: true` |
| esbuild lacks native WASM experiment | esbuild | Use `esbuild-plugin-wasm` or manual instantiation |
| Safari has occasional WASM compilation timeout | Safari | Use streaming compilation; avoid synchronous instantiation |
| Turbopack config differs from Webpack | Next.js | Use `next.config.js` webpack callback; Turbopack support is automatic |

---

## 9. CI Matrix

The compatibility matrix is validated by `tests/wasm_bundler_compatibility.rs` which checks:

1. Package topology document references correct bundlers and module modes.
2. Packaging invariants are documented and testable.
3. Profile-to-channel mapping is consistent.
4. Bundler configuration requirements are specified for all Tier 1 targets.
5. Known constraints are documented for each bundler.

The CI `check` job includes a dedicated certification step:

- `WASM bundler compatibility certification`
- Captured evidence artifacts:
  - `artifacts/wasm_bundler_compatibility_summary.json`
  - `artifacts/wasm_bundler_compatibility_test.log`

Local deterministic reproduction command:

```bash
rch exec -- cargo test -p asupersync --test wasm_bundler_compatibility -- --nocapture
```

Full CI gate:

```bash
cargo test --test wasm_bundler_compatibility -- --nocapture
```

---

## 10. Cross-References

- Package topology: `docs/wasm_typescript_package_topology.md`
- Release channels: `docs/wasm_release_channel_strategy.md`
- Quickstart/migration: `docs/wasm_quickstart_migration.md`
- ABI contract: `docs/wasm_abi_contract.md`
- ABI compatibility policy: `docs/wasm_abi_compatibility_policy.md`
- Dependency audit: `docs/wasm_dependency_audit_policy.md`
- Feature profiles: `Cargo.toml` (`[features]` section)
- Compile-time guardrails: `src/lib.rs` (wasm32 compile_error gates)
