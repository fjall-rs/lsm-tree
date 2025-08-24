use std::hash::{BuildHasher, Hasher};

/// Calculates a 64-bit hash from a byte slice.
pub trait Hash64 {
    /// Gets the readable hash function name (e.g. "metrohash")
    fn name(&self) -> &'static str;

    /// Hashes a byte slice to a 64-bit digest
    fn hash64(&self, bytes: &[u8]) -> u64;
}

pub struct Fnv;
impl Hash64 for Fnv {
    fn name(&self) -> &'static str {
        "FNV"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        let mut hasher = fnv::FnvHasher::default();
        hasher.write(bytes);
        hasher.finish()
    }
}

pub struct Xxh64;
impl Hash64 for Xxh64 {
    fn name(&self) -> &'static str {
        "XXH64"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        let mut hasher = xxhash_rust::xxh64::Xxh64::default();
        hasher.write(bytes);
        hasher.finish()
    }
}

pub struct Xxh3;
impl Hash64 for Xxh3 {
    fn name(&self) -> &'static str {
        "XXH3"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(bytes)
    }
}

pub struct Xxh3_B;
impl Hash64 for Xxh3_B {
    fn name(&self) -> &'static str {
        "XXH3_B"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        twox_hash::XxHash3_64::oneshot(bytes)
    }
}

pub struct CityHash;
impl Hash64 for CityHash {
    fn name(&self) -> &'static str {
        "CityHash"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        cityhasher::hash(bytes)
    }
}

pub struct MetroHash;
impl Hash64 for MetroHash {
    fn name(&self) -> &'static str {
        "MetroHash"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        let mut hasher = metrohash::MetroHash64::default();
        hasher.write(bytes);
        hasher.finish()
    }
}

pub struct WyHash;
impl Hash64 for WyHash {
    fn name(&self) -> &'static str {
        "WyHash"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        wyhash::wyhash(bytes, 0)
    }
}

pub struct RapidHash;
impl Hash64 for RapidHash {
    fn name(&self) -> &'static str {
        "RapidHash"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        let mut hasher = rapidhash::fast::RapidHasher::default();
        hasher.write(bytes);
        hasher.finish()
    }
}

pub struct SeaHash;
impl Hash64 for SeaHash {
    fn name(&self) -> &'static str {
        "SeaHash"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        seahash::hash(bytes)
    }
}

pub struct RustcHash;
impl Hash64 for RustcHash {
    fn name(&self) -> &'static str {
        "RustcHash"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        rustc_hash::FxBuildHasher::default().hash_one(bytes)
    }
}

pub struct FxHash;
impl Hash64 for FxHash {
    fn name(&self) -> &'static str {
        "FxHash"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        fxhash::hash64(bytes)
    }
}

pub struct GxHash;
impl Hash64 for GxHash {
    fn name(&self) -> &'static str {
        "GxHash"
    }

    fn hash64(&self, bytes: &[u8]) -> u64 {
        gxhash::gxhash64(bytes, 123)
    }
}
