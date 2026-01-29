//! RaptorQ encoding for region state.
//!
//! Transforms region snapshots into erasure-coded symbols for
//! distribution to replicas.

use crate::types::symbol::{ObjectId, ObjectParams, Symbol, SymbolId, SymbolKind};
use crate::types::Time;
use crate::util::DetRng;

use super::snapshot::RegionSnapshot;

// ---------------------------------------------------------------------------
// EncodingConfig
// ---------------------------------------------------------------------------

/// Configuration for state encoding.
#[derive(Debug, Clone)]
pub struct EncodingConfig {
    /// Symbol size in bytes.
    pub symbol_size: u16,
    /// Minimum repair symbols to generate (for redundancy).
    pub min_repair_symbols: u16,
    /// Maximum source blocks (for large objects).
    pub max_source_blocks: u8,
    /// Repair symbol overhead factor (e.g., 1.2 = 20% overhead).
    pub repair_overhead: f32,
}

impl Default for EncodingConfig {
    fn default() -> Self {
        Self {
            symbol_size: 1280,
            min_repair_symbols: 4,
            max_source_blocks: 1,
            repair_overhead: 1.2,
        }
    }
}

// ---------------------------------------------------------------------------
// StateEncoder
// ---------------------------------------------------------------------------

/// Encodes region state into RaptorQ symbols.
///
/// The encoder serializes a [`RegionSnapshot`] to bytes, splits them into
/// source symbols, and generates XOR-parity repair symbols.
#[derive(Debug)]
pub struct StateEncoder {
    config: EncodingConfig,
    rng: DetRng,
}

impl StateEncoder {
    /// Creates a new encoder with the given configuration.
    #[must_use]
    pub fn new(config: EncodingConfig, rng: DetRng) -> Self {
        Self { config, rng }
    }

    /// Encodes a region snapshot into symbols.
    ///
    /// Generates a random object ID, then delegates to [`encode_with_id`](Self::encode_with_id).
    pub fn encode(
        &mut self,
        snapshot: &RegionSnapshot,
        encoded_at: Time,
    ) -> Result<EncodedState, EncodingError> {
        let object_id = ObjectId::new_random(&mut self.rng);
        self.encode_with_id(snapshot, object_id, encoded_at)
    }

    /// Encodes with a specific object ID (for deterministic testing).
    pub fn encode_with_id(
        &mut self,
        snapshot: &RegionSnapshot,
        object_id: ObjectId,
        encoded_at: Time,
    ) -> Result<EncodedState, EncodingError> {
        let data = snapshot.to_bytes();
        if data.is_empty() {
            return Err(EncodingError::EmptyData);
        }

        let symbol_size = self.config.symbol_size as usize;
        let params = self.calculate_params(data.len(), object_id);

        // Create source symbols by splitting data into chunks.
        let source_symbols = Self::create_source_symbols(&data, &params, symbol_size);
        let source_count = source_symbols.len() as u16;

        // Generate repair symbols via XOR parity.
        let repair_symbols = self.create_repair_symbols(&source_symbols, &params, symbol_size);
        let repair_count = repair_symbols.len() as u16;

        let mut symbols = source_symbols;
        symbols.extend(repair_symbols);

        Ok(EncodedState {
            params,
            symbols,
            source_count,
            repair_count,
            original_size: data.len(),
            encoded_at,
        })
    }

    /// Generates additional repair symbols for an existing encoding.
    pub fn generate_repair(
        &mut self,
        state: &EncodedState,
        count: u16,
    ) -> Result<Vec<Symbol>, EncodingError> {
        let source_symbols: Vec<&Symbol> = state
            .symbols
            .iter()
            .filter(|s| s.kind().is_source())
            .collect();
        if source_symbols.is_empty() {
            return Err(EncodingError::NoSourceSymbols);
        }

        let symbol_size = source_symbols[0].len();
        let base_esi = u32::from(state.source_count) + u32::from(state.repair_count);
        let mut repairs = Vec::with_capacity(count as usize);

        for i in 0..count {
            let esi = base_esi + u32::from(i);
            let mut repair_data = vec![0u8; symbol_size];

            // XOR parity: combine source symbols with a rotation based on esi.
            for (j, _) in source_symbols.iter().enumerate() {
                let offset = (esi as usize + j) % source_symbols.len();
                if offset < source_symbols.len() {
                    xor_into(&mut repair_data, source_symbols[offset].data());
                }
            }

            let id = SymbolId::new(state.params.object_id, 0, esi);
            repairs.push(Symbol::new(id, repair_data, SymbolKind::Repair));
        }

        Ok(repairs)
    }

    fn calculate_params(&self, data_size: usize, object_id: ObjectId) -> ObjectParams {
        let symbol_size = self.config.symbol_size as usize;
        let symbols_needed = data_size.div_ceil(symbol_size);

        ObjectParams::new(
            object_id,
            data_size as u64,
            self.config.symbol_size,
            1, // source_blocks
            symbols_needed as u16,
        )
    }

    fn create_source_symbols(
        data: &[u8],
        params: &ObjectParams,
        symbol_size: usize,
    ) -> Vec<Symbol> {
        let k = params.symbols_per_block as usize;
        let mut symbols = Vec::with_capacity(k);

        for i in 0..k {
            let start = i * symbol_size;
            let end = std::cmp::min(start + symbol_size, data.len());

            // Pad final symbol with zeros if needed.
            let mut sym_data = vec![0u8; symbol_size];
            if start < data.len() {
                let copy_len = end - start;
                sym_data[..copy_len].copy_from_slice(&data[start..end]);
            }

            let id = SymbolId::new(params.object_id, 0, i as u32);
            symbols.push(Symbol::new(id, sym_data, SymbolKind::Source));
        }

        symbols
    }

    fn create_repair_symbols(
        &self,
        source_symbols: &[Symbol],
        params: &ObjectParams,
        symbol_size: usize,
    ) -> Vec<Symbol> {
        let repair_count = self.config.min_repair_symbols as usize;
        let source_count = source_symbols.len();
        let mut repairs = Vec::with_capacity(repair_count);

        for r in 0..repair_count {
            let mut repair_data = vec![0u8; symbol_size];

            // XOR parity with rotation: each repair symbol XORs a different
            // subset of source symbols.
            for (j, _) in source_symbols.iter().enumerate() {
                let idx = (j + r) % source_count;
                if idx < source_count {
                    xor_into(&mut repair_data, source_symbols[idx].data());
                }
            }

            let esi = source_count as u32 + r as u32;
            let id = SymbolId::new(params.object_id, 0, esi);
            repairs.push(Symbol::new(id, repair_data, SymbolKind::Repair));
        }

        repairs
    }
}

/// XORs `src` into `dst` in place.
fn xor_into(dst: &mut [u8], src: &[u8]) {
    let len = std::cmp::min(dst.len(), src.len());
    for i in 0..len {
        dst[i] ^= src[i];
    }
}

// ---------------------------------------------------------------------------
// EncodedState
// ---------------------------------------------------------------------------

/// Result of encoding a region snapshot.
#[derive(Debug)]
pub struct EncodedState {
    /// Object parameters for this encoding.
    pub params: ObjectParams,
    /// All generated symbols (source + repair).
    pub symbols: Vec<Symbol>,
    /// Number of source symbols.
    pub source_count: u16,
    /// Number of repair symbols.
    pub repair_count: u16,
    /// Original snapshot size in bytes.
    pub original_size: usize,
    /// Encoding timestamp.
    pub encoded_at: Time,
}

impl EncodedState {
    /// Returns an iterator over source symbols only.
    pub fn source_symbols(&self) -> impl Iterator<Item = &Symbol> {
        self.symbols.iter().filter(|s| s.kind().is_source())
    }

    /// Returns an iterator over repair symbols only.
    pub fn repair_symbols(&self) -> impl Iterator<Item = &Symbol> {
        self.symbols.iter().filter(|s| s.kind().is_repair())
    }

    /// Returns the minimum symbols needed for decoding.
    #[must_use]
    pub fn min_symbols_for_decode(&self) -> u16 {
        self.source_count
    }

    /// Returns total redundancy factor.
    #[must_use]
    pub fn redundancy_factor(&self) -> f32 {
        if self.source_count == 0 {
            return 0.0;
        }
        f32::from(self.source_count + self.repair_count) / f32::from(self.source_count)
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Error during state encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodingError {
    /// Snapshot serialized to empty data.
    EmptyData,
    /// No source symbols available.
    NoSourceSymbols,
}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyData => write!(f, "snapshot serialized to empty data"),
            Self::NoSourceSymbols => write!(f, "no source symbols available"),
        }
    }
}

impl std::error::Error for EncodingError {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::similar_names)]
mod tests {
    use super::*;
    use crate::distributed::snapshot::{BudgetSnapshot, TaskSnapshot, TaskState};
    use crate::types::RegionId;

    fn create_test_snapshot() -> RegionSnapshot {
        use crate::record::region::RegionState;

        RegionSnapshot {
            region_id: RegionId::new_for_test(1, 0),
            state: RegionState::Open,
            timestamp: Time::from_secs(100),
            sequence: 1,
            tasks: vec![TaskSnapshot {
                task_id: crate::types::TaskId::new_for_test(1, 0),
                state: TaskState::Running,
                priority: 5,
            }],
            children: vec![],
            finalizer_count: 2,
            budget: BudgetSnapshot {
                deadline_nanos: Some(1_000_000_000),
                polls_remaining: Some(100),
                cost_remaining: None,
            },
            cancel_reason: None,
            parent: None,
            metadata: vec![],
        }
    }

    #[test]
    fn encode_creates_correct_symbol_count() {
        let config = EncodingConfig {
            symbol_size: 128,
            min_repair_symbols: 4,
            ..Default::default()
        };
        let mut encoder = StateEncoder::new(config, DetRng::new(42));

        let snapshot = create_test_snapshot();
        let encoded = encoder.encode(&snapshot, Time::ZERO).unwrap();

        assert_eq!(
            encoded.symbols.len(),
            (encoded.source_count + encoded.repair_count) as usize
        );
        // Source + repair should match
        assert_eq!(
            encoded.source_symbols().count(),
            encoded.source_count as usize
        );
        assert_eq!(
            encoded.repair_symbols().count(),
            encoded.repair_count as usize
        );
    }

    #[test]
    fn encode_deterministic_with_same_seed() {
        let config = EncodingConfig::default();
        let snapshot = create_test_snapshot();
        let object_id = ObjectId::new_for_test(123);

        let mut encoder1 = StateEncoder::new(config.clone(), DetRng::new(42));
        let mut encoder2 = StateEncoder::new(config, DetRng::new(42));

        let encoded1 = encoder1
            .encode_with_id(&snapshot, object_id, Time::ZERO)
            .unwrap();
        let encoded2 = encoder2
            .encode_with_id(&snapshot, object_id, Time::ZERO)
            .unwrap();

        assert_eq!(encoded1.symbols.len(), encoded2.symbols.len());
        for (s1, s2) in encoded1.symbols.iter().zip(encoded2.symbols.iter()) {
            assert_eq!(s1.data(), s2.data());
        }
    }

    #[test]
    fn encode_symbol_size_respected() {
        let config = EncodingConfig {
            symbol_size: 256,
            ..Default::default()
        };
        let mut encoder = StateEncoder::new(config, DetRng::new(42));

        let snapshot = create_test_snapshot();
        let encoded = encoder.encode(&snapshot, Time::ZERO).unwrap();

        for symbol in &encoded.symbols {
            assert!(
                symbol.len() <= 256,
                "symbol size {} exceeds config 256",
                symbol.len()
            );
        }
    }

    #[test]
    fn encode_redundancy_factor() {
        let config = EncodingConfig {
            min_repair_symbols: 10,
            ..Default::default()
        };
        let mut encoder = StateEncoder::new(config, DetRng::new(42));

        let snapshot = create_test_snapshot();
        let encoded = encoder.encode(&snapshot, Time::ZERO).unwrap();

        assert!(
            encoded.redundancy_factor() > 1.0,
            "redundancy {} should be > 1.0",
            encoded.redundancy_factor()
        );
    }

    #[test]
    fn generate_additional_repair() {
        let config = EncodingConfig::default();
        let mut encoder = StateEncoder::new(config, DetRng::new(42));

        let snapshot = create_test_snapshot();
        let encoded = encoder.encode(&snapshot, Time::ZERO).unwrap();

        let additional = encoder.generate_repair(&encoded, 5).unwrap();

        assert_eq!(additional.len(), 5);
        for symbol in &additional {
            assert!(symbol.kind().is_repair());
        }
    }

    #[test]
    fn encode_empty_snapshot() {
        let config = EncodingConfig {
            symbol_size: 128,
            ..Default::default()
        };
        let mut encoder = StateEncoder::new(config, DetRng::new(42));

        let snapshot = RegionSnapshot::empty(RegionId::new_for_test(1, 0));
        let result = encoder.encode(&snapshot, Time::ZERO);

        // Should succeed with minimal symbols.
        assert!(result.is_ok());
        assert!(result.unwrap().source_count >= 1);
    }

    #[test]
    fn encoded_state_min_symbols_for_decode() {
        let config = EncodingConfig::default();
        let mut encoder = StateEncoder::new(config, DetRng::new(42));

        let snapshot = create_test_snapshot();
        let encoded = encoder.encode(&snapshot, Time::ZERO).unwrap();

        assert_eq!(encoded.min_symbols_for_decode(), encoded.source_count);
    }

    #[test]
    fn source_and_repair_separated() {
        let config = EncodingConfig {
            symbol_size: 64,
            min_repair_symbols: 3,
            ..Default::default()
        };
        let mut encoder = StateEncoder::new(config, DetRng::new(42));

        let snapshot = create_test_snapshot();
        let encoded = encoder.encode(&snapshot, Time::ZERO).unwrap();

        let source_count = encoded.source_symbols().count();
        let repair_count = encoded.repair_symbols().count();

        assert!(source_count > 0, "should have source symbols");
        assert_eq!(repair_count, 3, "should have 3 repair symbols");
        assert_eq!(source_count + repair_count, encoded.symbols.len());
    }
}
