# Next Turbopack Consumer Fixture

Fixture for bead `asupersync-3qv04.6.3`.

Purpose:
- validate a real Next.js consumer build (Turbopack) against packaged Browser Edition outputs.
- exercise package import resolution from `@asupersync/next`.

This fixture is executed through:
- `scripts/validate_next_turbopack_consumer.sh`

The validation script copies this fixture into a temporary workspace and installs local package copies to keep runs deterministic and side-effect free.
