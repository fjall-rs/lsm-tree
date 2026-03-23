// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Block-level encryption at rest.
//!
//! This module defines the [`EncryptionProvider`] trait for pluggable
//! block-level encryption and, behind the `encryption` feature, ships a
//! ready-to-use [`Aes256GcmProvider`] implementation.
//!
//! ## Pipeline order
//!
//! - **Write:** raw data → compress → **encrypt** → checksum → disk
//! - **Read:** disk → verify checksum → **decrypt** → decompress → raw data
//!
//! Checksums protect the encrypted (on-disk) bytes so that corruption is
//! detected cheaply before any decryption attempt.

/// Block encryption provider.
///
/// Implementors handle key management, nonce generation, and algorithm
/// selection. The trait is object-safe so it can be stored as
/// `Arc<dyn EncryptionProvider>`.
///
/// # Contract
///
/// - [`encrypt`](EncryptionProvider::encrypt) must be deterministic in output
///   *format* (but not value — nonces should be random or unique).
/// - [`decrypt`](EncryptionProvider::decrypt) must accept the exact byte
///   sequence returned by `encrypt` and recover the original plaintext.
/// - Both methods must be safe to call concurrently from multiple threads.
pub trait EncryptionProvider:
    Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe
{
    /// Encrypt `plaintext`, returning an opaque ciphertext blob.
    ///
    /// The returned bytes may include a nonce/IV prefix and an
    /// authentication tag — the layout is provider-defined.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Encrypt`] if the encryption operation fails.
    fn encrypt(&self, plaintext: &[u8]) -> crate::Result<Vec<u8>>;

    /// Maximum number of bytes that encryption adds to a plaintext payload.
    ///
    /// Used by block I/O to account for encryption overhead in size
    /// validation. For AES-256-GCM this is 28 (12-byte nonce + 16-byte tag).
    ///
    /// Returns `u32` because block sizes are `u32`-bounded on disk.
    fn max_overhead(&self) -> u32;

    /// Decrypt `ciphertext` previously produced by [`encrypt`](EncryptionProvider::encrypt).
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Decrypt`] if the ciphertext is invalid,
    /// tampered, or encrypted with a different key.
    fn decrypt(&self, ciphertext: &[u8]) -> crate::Result<Vec<u8>>;

    /// Encrypt an owned plaintext buffer, reusing its allocation when possible.
    ///
    /// The default implementation delegates to [`encrypt`](EncryptionProvider::encrypt).
    /// Providers may override this to avoid an extra allocation by prepending
    /// the nonce and appending the tag in-place.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Encrypt`] if the encryption operation fails.
    fn encrypt_vec(&self, plaintext: Vec<u8>) -> crate::Result<Vec<u8>> {
        self.encrypt(&plaintext)
    }

    /// Decrypt an owned ciphertext buffer, reusing its allocation when possible.
    ///
    /// The default implementation delegates to [`decrypt`](EncryptionProvider::decrypt).
    /// Providers may override this to decrypt in-place, stripping the nonce
    /// prefix and tag suffix without a second allocation.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Decrypt`] if the ciphertext is invalid,
    /// tampered, or encrypted with a different key.
    fn decrypt_vec(&self, ciphertext: Vec<u8>) -> crate::Result<Vec<u8>> {
        self.decrypt(&ciphertext)
    }
}

// ---------------------------------------------------------------------------
// AES-256-GCM implementation (feature-gated)
// ---------------------------------------------------------------------------

/// AES-256-GCM encryption provider.
///
/// Each [`encrypt`](EncryptionProvider::encrypt) call generates a random
/// 12-byte nonce and prepends it to the ciphertext:
///
/// ```text
/// [nonce; 12 bytes][ciphertext + GCM tag; N + 16 bytes]
/// ```
///
/// Overhead per block: **28 bytes** (12 nonce + 16 auth tag).
///
/// # Key management
///
/// The caller is responsible for providing and rotating the 256-bit key.
/// This provider does not persist or derive keys.
#[cfg(feature = "encryption")]
pub struct Aes256GcmProvider {
    cipher: aes_gcm::Aes256Gcm,
}

#[cfg(feature = "encryption")]
impl Aes256GcmProvider {
    /// Nonce size for AES-256-GCM (96 bits).
    const NONCE_LEN: usize = 12;

    /// GCM authentication tag size (128 bits).
    const TAG_LEN: usize = 16;

    /// Total per-block overhead: nonce + tag.
    pub const OVERHEAD: usize = Self::NONCE_LEN + Self::TAG_LEN;

    /// Create a new provider from a 256-bit (32-byte) key.
    ///
    /// The key length is enforced at compile time by the `[u8; 32]` type.
    /// For runtime-checked construction from a slice, use [`from_slice`](Self::from_slice).
    #[must_use]
    pub fn new(key: &[u8; 32]) -> Self {
        use aes_gcm::KeyInit;

        Self {
            cipher: aes_gcm::Aes256Gcm::new(key.into()),
        }
    }

    /// Create a provider from a key slice, returning an error if the
    /// length is not 32 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Encrypt`] if `key` is not exactly 32 bytes.
    pub fn from_slice(key: &[u8]) -> crate::Result<Self> {
        let key: &[u8; 32] = key
            .try_into()
            .map_err(|_| crate::Error::Encrypt("AES-256-GCM key must be exactly 32 bytes"))?;
        Ok(Self::new(key))
    }
}

/// Create a new [`ChaCha20Rng`](rand_chacha::ChaCha20Rng) seeded from the OS RNG.
///
/// Returns the RNG directly (not `Result`) because callers are
/// `thread_local!` init and fork-reseed, neither of which can propagate
/// errors. This function will panic if OS entropy is unavailable.
#[cfg(feature = "encryption")]
fn new_chacha_rng() -> rand_chacha::ChaCha20Rng {
    // Use rand_core re-exported by aes_gcm to avoid version-skew with a
    // direct rand_core dependency.
    use aes_gcm::aead::rand_core::{OsRng, SeedableRng};

    #[expect(
        clippy::expect_used,
        reason = "intentionally panics if OsRng is unavailable"
    )]
    rand_chacha::ChaCha20Rng::from_rng(OsRng)
        .expect("OS RNG should be available for initial CSPRNG seed")
}

/// Thread-local CSPRNG wrapper with fork-aware PID tracking.
///
/// On each access, compares the stored PID with `std::process::id()`.
/// If they differ (i.e. the process was forked), the RNG is reseeded
/// from `OsRng` to avoid nonce reuse across processes.
#[cfg(feature = "encryption")]
struct ForkAwareRng {
    pid: std::cell::Cell<u32>,
    rng: std::cell::RefCell<rand_chacha::ChaCha20Rng>,
}

#[cfg(feature = "encryption")]
impl ForkAwareRng {
    fn new() -> Self {
        Self {
            pid: std::cell::Cell::new(std::process::id()),
            rng: std::cell::RefCell::new(new_chacha_rng()),
        }
    }

    fn with_rng<R>(&self, f: impl FnOnce(&mut rand_chacha::ChaCha20Rng) -> R) -> R {
        let mut rng_ref = self.rng.borrow_mut();
        let current_pid = std::process::id();
        if self.pid.get() != current_pid {
            // Process was forked; reseed RNG to avoid nonce reuse across PIDs.
            self.pid.set(current_pid);
            *rng_ref = new_chacha_rng();
        }

        // The RefMut guard is held while f() runs. This is safe because
        // f() only generates a 12-byte nonce (no reentrant RNG access).
        // Deref-coercion: &mut RefMut<ChaCha20Rng> → &mut ChaCha20Rng
        // (explicit &mut *rng_ref is denied by clippy::explicit_auto_deref).
        f(&mut rng_ref)
    }
}

#[cfg(feature = "encryption")]
thread_local! {
    // Module-scope so all monomorphizations of `thread_local_rng`
    // share a single thread-local instance.
    static THREAD_RNG: ForkAwareRng = ForkAwareRng::new();
}

/// Access a thread-local CSPRNG seeded from the OS RNG in a fork-aware way.
///
/// Using a thread-local [`ChaCha20Rng`](rand_chacha::ChaCha20Rng) avoids a
/// `getrandom` syscall on every nonce generation, which saves 1-10 µs per
/// block under contention. The RNG is cryptographically secure and seeded
/// from `OsRng` on first access per thread, and is lazily reseeded on the
/// next use if the process ID changes (e.g., after a `fork()`) to reduce
/// the risk of nonce reuse across processes.
#[cfg(feature = "encryption")]
fn thread_local_rng<R>(f: impl FnOnce(&mut rand_chacha::ChaCha20Rng) -> R) -> R {
    THREAD_RNG.with(|state| state.with_rng(f))
}

#[cfg(feature = "encryption")]
impl EncryptionProvider for Aes256GcmProvider {
    fn max_overhead(&self) -> u32 {
        // OVERHEAD = NONCE_LEN + TAG_LEN = 28, always fits u32.
        #[expect(clippy::cast_possible_truncation, reason = "OVERHEAD is 28")]
        {
            Self::OVERHEAD as u32
        }
    }

    fn encrypt(&self, plaintext: &[u8]) -> crate::Result<Vec<u8>> {
        use aes_gcm::AeadCore;
        use aes_gcm::AeadInPlace;

        let nonce = thread_local_rng(|rng| aes_gcm::Aes256Gcm::generate_nonce(rng));

        let mut buf = Vec::with_capacity(Self::NONCE_LEN + plaintext.len() + Self::TAG_LEN);
        buf.extend_from_slice(&nonce);
        buf.extend_from_slice(plaintext);

        // encrypt_in_place_detached operates on buf[NONCE_LEN..] (the plaintext portion).
        // Indexing is safe: buf was allocated as nonce + plaintext.
        //
        // TODO: pass block context (table_id, offset, block_type) as AAD to
        // bind ciphertext authenticity to its position and prevent block
        // substitution attacks. Requires extending EncryptionProvider API.
        #[expect(
            clippy::indexing_slicing,
            reason = "buf length = NONCE_LEN + plaintext.len()"
        )]
        let tag = self
            .cipher
            .encrypt_in_place_detached(&nonce, b"", &mut buf[Self::NONCE_LEN..])
            .map_err(|_| crate::Error::Encrypt("AES-256-GCM encryption failed"))?;

        buf.extend_from_slice(&tag);

        Ok(buf)
    }

    fn decrypt(&self, ciphertext: &[u8]) -> crate::Result<Vec<u8>> {
        use aes_gcm::aead::generic_array::GenericArray;
        use aes_gcm::AeadInPlace;

        let min_len = Self::NONCE_LEN + Self::TAG_LEN;
        if ciphertext.len() < min_len {
            return Err(crate::Error::Decrypt(
                "ciphertext too short for AES-256-GCM (need nonce + tag)",
            ));
        }

        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let nonce = GenericArray::from_slice(&ciphertext[..Self::NONCE_LEN]);

        // Safe: ciphertext.len() >= NONCE_LEN + TAG_LEN checked above
        let tag_start = ciphertext.len() - Self::TAG_LEN;

        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let tag = GenericArray::from_slice(&ciphertext[tag_start..]);

        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let mut buf = ciphertext[Self::NONCE_LEN..tag_start].to_vec();

        self.cipher
            .decrypt_in_place_detached(nonce, b"", &mut buf, tag)
            .map_err(|_| {
                crate::Error::Decrypt("AES-256-GCM decryption failed (bad key or tampered data)")
            })?;

        Ok(buf)
    }

    fn encrypt_vec(&self, mut buf: Vec<u8>) -> crate::Result<Vec<u8>> {
        use aes_gcm::AeadCore;
        use aes_gcm::AeadInPlace;

        let nonce = thread_local_rng(|rng| aes_gcm::Aes256Gcm::generate_nonce(rng));

        // Reserve space for nonce prefix + tag suffix in one allocation,
        // then shift plaintext right and write the nonce into the gap.
        let plaintext_len = buf.len();
        buf.reserve(Self::NONCE_LEN + Self::TAG_LEN);
        buf.resize(plaintext_len + Self::NONCE_LEN, 0);
        buf.copy_within(..plaintext_len, Self::NONCE_LEN);
        #[expect(
            clippy::indexing_slicing,
            reason = "buf was just resized to include NONCE_LEN"
        )]
        buf[..Self::NONCE_LEN].copy_from_slice(&nonce);

        #[expect(
            clippy::indexing_slicing,
            reason = "buf length ≥ NONCE_LEN after resize + copy_within"
        )]
        let tag = self
            .cipher
            .encrypt_in_place_detached(&nonce, b"", &mut buf[Self::NONCE_LEN..])
            .map_err(|_| crate::Error::Encrypt("AES-256-GCM encryption failed"))?;

        buf.extend_from_slice(&tag);

        Ok(buf)
    }

    fn decrypt_vec(&self, mut buf: Vec<u8>) -> crate::Result<Vec<u8>> {
        use aes_gcm::aead::generic_array::GenericArray;
        use aes_gcm::AeadInPlace;

        // Error::Decrypt takes &'static str — can't include runtime lengths
        // without changing the upstream error type to accept String/Cow.
        let min_len = Self::NONCE_LEN + Self::TAG_LEN;
        if buf.len() < min_len {
            return Err(crate::Error::Decrypt(
                "ciphertext too short for AES-256-GCM (need nonce + tag)",
            ));
        }

        // Copy nonce and tag to the stack before mutating the buffer.
        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let nonce = *GenericArray::from_slice(&buf[..Self::NONCE_LEN]);

        let tag_start = buf.len() - Self::TAG_LEN;
        #[expect(clippy::indexing_slicing, reason = "length checked above")]
        let tag = *GenericArray::from_slice(&buf[tag_start..]);

        // Strip nonce prefix and tag suffix via copy_within + truncate
        // (single memmove, avoids Drain iterator adapter overhead).
        buf.copy_within(Self::NONCE_LEN..tag_start, 0);
        buf.truncate(tag_start - Self::NONCE_LEN);

        self.cipher
            .decrypt_in_place_detached(&nonce, b"", &mut buf, &tag)
            .map_err(|_| {
                crate::Error::Decrypt("AES-256-GCM decryption failed (bad key or tampered data)")
            })?;

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encryption_provider_trait_is_object_safe() {
        // Compile-time check: the trait can be used as a trait object.
        fn _assert_object_safe(_: &dyn EncryptionProvider) {}
    }

    /// Minimal provider that only implements required methods,
    /// exercising the default encrypt_vec/decrypt_vec implementations.
    struct XorProvider;

    impl std::panic::UnwindSafe for XorProvider {}
    impl std::panic::RefUnwindSafe for XorProvider {}

    impl EncryptionProvider for XorProvider {
        fn encrypt(&self, plaintext: &[u8]) -> crate::Result<Vec<u8>> {
            Ok(plaintext.iter().map(|b| b ^ 0xAA).collect())
        }

        fn max_overhead(&self) -> u32 {
            0
        }

        fn decrypt(&self, ciphertext: &[u8]) -> crate::Result<Vec<u8>> {
            Ok(ciphertext.iter().map(|b| b ^ 0xAA).collect())
        }
    }

    #[test]
    fn default_encrypt_vec_delegates_to_encrypt() -> crate::Result<()> {
        let provider = XorProvider;
        let plaintext = b"test default encrypt_vec";

        let via_encrypt = provider.encrypt(plaintext)?;
        let via_encrypt_vec = provider.encrypt_vec(plaintext.to_vec())?;
        assert_eq!(via_encrypt, via_encrypt_vec);

        let decrypted = provider.decrypt(&via_encrypt_vec)?;
        assert_eq!(decrypted, plaintext);
        Ok(())
    }

    #[test]
    fn default_decrypt_vec_delegates_to_decrypt() -> crate::Result<()> {
        let provider = XorProvider;
        let plaintext = b"test default decrypt_vec";

        let ciphertext = provider.encrypt(plaintext)?;

        let via_decrypt = provider.decrypt(&ciphertext)?;
        let via_decrypt_vec = provider.decrypt_vec(ciphertext.clone())?;
        assert_eq!(via_decrypt, via_decrypt_vec);
        assert_eq!(via_decrypt_vec, plaintext);
        Ok(())
    }

    #[cfg(feature = "encryption")]
    mod aes256gcm {
        use super::*;

        fn test_key() -> [u8; 32] {
            [0x42; 32]
        }

        #[test]
        fn roundtrip_basic() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let plaintext = b"hello world, this is a block of data!";

            let ciphertext = provider.encrypt(plaintext)?;
            assert_ne!(&ciphertext[..], plaintext.as_slice());
            assert_eq!(
                ciphertext.len(),
                Aes256GcmProvider::NONCE_LEN + plaintext.len() + Aes256GcmProvider::TAG_LEN,
            );

            let decrypted = provider.decrypt(&ciphertext)?;
            assert_eq!(decrypted, plaintext);
            Ok(())
        }

        #[test]
        fn roundtrip_empty() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let plaintext = b"";

            let ciphertext = provider.encrypt(plaintext)?;
            let decrypted = provider.decrypt(&ciphertext)?;
            assert_eq!(decrypted, plaintext);
            Ok(())
        }

        #[test]
        fn different_nonces_produce_different_ciphertexts() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let plaintext = b"deterministic input";

            let ct1 = provider.encrypt(plaintext)?;
            let ct2 = provider.encrypt(plaintext)?;
            assert_ne!(
                ct1, ct2,
                "random nonces should produce different ciphertexts"
            );

            // Both decrypt to the same plaintext
            assert_eq!(provider.decrypt(&ct1)?, provider.decrypt(&ct2)?,);
            Ok(())
        }

        #[test]
        fn wrong_key_fails_decrypt() -> crate::Result<()> {
            let provider1 = Aes256GcmProvider::new(&[0x01; 32]);
            let provider2 = Aes256GcmProvider::new(&[0x02; 32]);

            let ciphertext = provider1.encrypt(b"secret")?;
            let result = provider2.decrypt(&ciphertext);
            assert!(result.is_err());
            Ok(())
        }

        #[test]
        fn tampered_ciphertext_fails_decrypt() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let mut ciphertext = provider.encrypt(b"data")?;

            // Flip a byte in the ciphertext body
            let mid = Aes256GcmProvider::NONCE_LEN + 1;
            if mid < ciphertext.len() {
                #[expect(clippy::indexing_slicing)]
                {
                    ciphertext[mid] ^= 0xFF;
                }
            }

            let result = provider.decrypt(&ciphertext);
            assert!(result.is_err());
            Ok(())
        }

        #[test]
        fn truncated_ciphertext_fails_decrypt() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let result = provider.decrypt(&[0u8; 10]); // less than nonce + tag
            assert!(result.is_err());
            Ok(())
        }

        #[test]
        fn from_slice_rejects_wrong_length() {
            assert!(Aes256GcmProvider::from_slice(&[0u8; 16]).is_err());
            assert!(Aes256GcmProvider::from_slice(&[0u8; 31]).is_err());
            assert!(Aes256GcmProvider::from_slice(&[0u8; 33]).is_err());
            assert!(Aes256GcmProvider::from_slice(&[0u8; 32]).is_ok());
        }

        #[test]
        fn roundtrip_large_payload() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let plaintext = vec![0xAB_u8; 64 * 1024]; // 64 KiB

            let ciphertext = provider.encrypt(&plaintext)?;
            let decrypted = provider.decrypt(&ciphertext)?;
            assert_eq!(decrypted, plaintext);
            Ok(())
        }

        /// Verify the thread-local CSPRNG produces unique nonces across many
        /// encrypt calls — no nonce reuse even under rapid sequential use.
        #[test]
        fn thread_local_rng_produces_unique_nonces() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let plaintext = b"nonce uniqueness test";

            let mut nonces = std::collections::HashSet::new();
            for _ in 0..1000 {
                let ct = provider.encrypt(plaintext)?;

                #[expect(clippy::indexing_slicing, reason = "ct always >= NONCE_LEN")]
                #[expect(clippy::expect_used, reason = "test assertion")]
                let nonce: [u8; Aes256GcmProvider::NONCE_LEN] = ct[..Aes256GcmProvider::NONCE_LEN]
                    .try_into()
                    .expect("nonce has expected length");

                assert!(
                    nonces.insert(nonce),
                    "nonce collision detected — CSPRNG produced duplicate nonce"
                );
            }
            Ok(())
        }

        /// Verify ForkAwareRng reseeds when it detects a PID change.
        ///
        /// Asserts on deterministic state (PID restoration) rather than
        /// probabilistic RNG output to avoid flaky CI.
        #[test]
        fn fork_aware_rng_reseeds_on_pid_change() {
            use aes_gcm::aead::rand_core::RngCore;

            let rng = ForkAwareRng::new();

            // Generate a value with the current PID (ensures RNG is initialized).
            let _ = rng.with_rng(|r| r.next_u64());

            // Simulate fork by setting a fake PID that differs from the real one.
            let current_pid = std::process::id();
            let fake_pid = current_pid ^ 1;
            rng.pid.set(fake_pid);
            assert_eq!(rng.pid.get(), fake_pid, "PID should be set to fake value");

            // Next call sees real PID != fake PID → reseeds from OsRng and
            // restores the stored PID to the real process ID.
            let _ = rng.with_rng(|r| r.next_u64());

            // Deterministic assertion: PID was restored after reseed.
            assert_eq!(
                rng.pid.get(),
                std::process::id(),
                "PID should be restored to real process ID after reseed"
            );
        }

        #[test]
        fn encrypt_vec_roundtrip() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let plaintext = b"block data for encrypt_vec test";

            let ciphertext = provider.encrypt_vec(plaintext.to_vec())?;
            assert_eq!(
                ciphertext.len(),
                Aes256GcmProvider::NONCE_LEN + plaintext.len() + Aes256GcmProvider::TAG_LEN,
            );

            // encrypt_vec output must be decryptable by decrypt
            let decrypted = provider.decrypt(&ciphertext)?;
            assert_eq!(decrypted, plaintext);
            Ok(())
        }

        #[test]
        fn decrypt_vec_roundtrip() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let plaintext = b"block data for decrypt_vec test";

            // encrypt output must be decryptable by decrypt_vec
            let ciphertext = provider.encrypt(plaintext)?;
            let decrypted = provider.decrypt_vec(ciphertext)?;
            assert_eq!(decrypted, plaintext);
            Ok(())
        }

        #[test]
        fn encrypt_vec_decrypt_vec_roundtrip() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let plaintext = vec![0xCD_u8; 16 * 1024]; // 16 KiB

            let ciphertext = provider.encrypt_vec(plaintext.clone())?;
            let decrypted = provider.decrypt_vec(ciphertext)?;
            assert_eq!(decrypted, plaintext);
            Ok(())
        }

        #[test]
        fn encrypt_vec_empty() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());

            let ciphertext = provider.encrypt_vec(vec![])?;
            let decrypted = provider.decrypt_vec(ciphertext)?;
            assert!(decrypted.is_empty());
            Ok(())
        }

        #[test]
        fn decrypt_vec_truncated_fails() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let result = provider.decrypt_vec(vec![0u8; 10]);
            assert!(result.is_err());
            Ok(())
        }

        #[test]
        fn decrypt_vec_tampered_fails() -> crate::Result<()> {
            let provider = Aes256GcmProvider::new(&test_key());
            let mut ciphertext = provider.encrypt_vec(b"data".to_vec())?;

            let mid = Aes256GcmProvider::NONCE_LEN + 1;
            if mid < ciphertext.len() {
                #[expect(clippy::indexing_slicing)]
                {
                    ciphertext[mid] ^= 0xFF;
                }
            }

            let result = provider.decrypt_vec(ciphertext);
            assert!(result.is_err());
            Ok(())
        }
    }
}
