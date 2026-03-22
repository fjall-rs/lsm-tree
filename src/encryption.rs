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
        use aes_gcm::aead::OsRng;
        use aes_gcm::AeadCore;
        use aes_gcm::AeadInPlace;

        let nonce = aes_gcm::Aes256Gcm::generate_nonce(&mut OsRng);

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encryption_provider_trait_is_object_safe() {
        // Compile-time check: the trait can be used as a trait object.
        fn _assert_object_safe(_: &dyn EncryptionProvider) {}
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
    }
}
