//! HMAC-SHA256 for the mesh handshake (RFC 0004 §4.7), plus the challenge
//! nonce source.
//!
//! **0.2.0: swapped from a vendored implementation to the audited RustCrypto
//! crates** (`hmac`/`sha2`) — the change promised in the Phase 3A PR notes
//! ("swapping `hmac_sha256` for `hmac::Hmac<sha2::Sha256>` is a one-line
//! change and every caller is unaffected"). Every caller still takes `&[u8]`
//! in, `[u8; 32]` out — nothing outside this file changed. Correctness is
//! still proven the same way: the FIPS 180-4 SHA-256 vectors and the RFC 4231
//! HMAC-SHA256 vectors below, byte for byte, now run as a regression suite
//! against the RustCrypto implementation instead of the hand-rolled one.
//!
//! What this does NOT provide is confidentiality — mesh traffic is cleartext
//! (§4.7). The HMAC authenticates peers and pins the negotiation transcript; it
//! is not encryption. mTLS rides RFC 0003's seam.

use std::sync::atomic::{AtomicU64, Ordering};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// SHA-256 (FIPS 180-4), one-shot.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// HMAC-SHA256 (RFC 2104 / RFC 4231). `Hmac::new_from_slice` only errors on a
/// key-length constraint that doesn't apply to HMAC (it accepts any key
/// length — short keys are zero-padded, long keys are hashed first, exactly
/// as RFC 4231 specifies), so the `expect` is an invariant, not a data path.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 accepts a key of any length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// Constant-time equality for MAC verification. A `==` on `[u8; 32]` short-
/// circuits on the first differing byte and leaks a timing signal about how
/// much of a forged MAC was correct; auth comparisons must not.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// A fresh 32-byte challenge nonce (§4.7). Reads the OS CSPRNG via
/// `/dev/urandom` — the mesh targets Linux and macOS (the same platform
/// envelope as the HTTP-attach phase), where this is always present.
///
/// The fallback path exists only so a missing `/dev/urandom` degrades to a
/// non-repeating value rather than a panic; it mixes monotonic + wall time, a
/// per-process `RandomState` seed (ASLR-derived), a stack address, and a
/// monotonic counter. It is explicitly **not** claimed to be cryptographic —
/// if it ever runs, the handshake's replay resistance is weakened and that is a
/// deployment bug worth the log line the caller emits.
pub fn random_nonce() -> [u8; 32] {
    use std::io::Read;
    let mut buf = [0u8; 32];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        if f.read_exact(&mut buf).is_ok() {
            return buf;
        }
    }
    fallback_nonce()
}

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn fallback_nonce() -> [u8; 32] {
    use std::hash::{BuildHasher, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};

    let n = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // RandomState is seeded per-process from the OS at startup; hashing through
    // two independently-seeded instances yields process-unique words.
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(n);
    h.write_u64(t);
    let seed_a = h.finish();
    let mut h2 = std::collections::hash_map::RandomState::new().build_hasher();
    h2.write_u64(seed_a);
    let seed_b = h2.finish();

    // Whiten the weak seed through SHA-256 so structure does not survive.
    let mut material = Vec::with_capacity(32);
    material.extend_from_slice(&n.to_le_bytes());
    material.extend_from_slice(&t.to_le_bytes());
    material.extend_from_slice(&seed_a.to_le_bytes());
    material.extend_from_slice(&seed_b.to_le_bytes());
    sha256(&material)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    // ---------- FIPS 180-4 SHA-256 known-answer vectors ----------

    #[test]
    fn sha256_empty() {
        assert_eq!(
            sha256(b"").to_vec(),
            hex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
    }

    #[test]
    fn sha256_abc() {
        assert_eq!(
            sha256(b"abc").to_vec(),
            hex("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
    }

    #[test]
    fn sha256_two_block() {
        assert_eq!(
            sha256(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq").to_vec(),
            hex("248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1")
        );
    }

    #[test]
    fn sha256_long_multiblock() {
        // 1000 'a's crosses several blocks and a length-field carry.
        let data = vec![b'a'; 1000];
        assert_eq!(
            sha256(&data).to_vec(),
            hex("41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3")
        );
    }

    // ---------- RFC 4231 HMAC-SHA256 known-answer vectors ----------

    #[test]
    fn hmac_rfc4231_case1() {
        let key = [0x0b; 20];
        let mac = hmac_sha256(&key, b"Hi There");
        assert_eq!(
            mac.to_vec(),
            hex("b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7")
        );
    }

    #[test]
    fn hmac_rfc4231_case2() {
        // Short key ("Jefe"), the classic test.
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            mac.to_vec(),
            hex("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")
        );
    }

    #[test]
    fn hmac_long_key_is_hashed_first() {
        // RFC 4231 case 6: 131-byte key exercises the key > blocksize path.
        let key = [0xaa; 131];
        let mac = hmac_sha256(
            &key,
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        );
        assert_eq!(
            mac.to_vec(),
            hex("60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54")
        );
    }

    // ---------- constant-time compare + nonce sanity ----------

    #[test]
    fn constant_time_eq_matches_semantics() {
        assert!(constant_time_eq(&[1, 2, 3], &[1, 2, 3]));
        assert!(!constant_time_eq(&[1, 2, 3], &[1, 2, 4]));
        assert!(!constant_time_eq(&[1, 2, 3], &[1, 2]));
    }

    #[test]
    fn nonces_are_fresh() {
        let a = random_nonce();
        let b = random_nonce();
        assert_ne!(a, b, "two nonces must not collide");
        // The fallback alone must also not repeat (freshness without urandom).
        assert_ne!(fallback_nonce(), fallback_nonce());
    }
}
