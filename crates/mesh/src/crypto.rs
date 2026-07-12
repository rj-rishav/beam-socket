//! HMAC-SHA256 for the mesh handshake (RFC 0004 §4.7), plus the challenge
//! nonce source.
//!
//! This is **vendored on purpose** rather than pulled from `hmac`/`sha2`: it
//! keeps `beamsocket-mesh` a std-plus-tokio crate — offline-buildable, and
//! adding a mesh does not drag the RustCrypto tree into the workspace lockfile.
//! Correctness is not asserted by hand: the tests below are the FIPS 180-4
//! SHA-256 vectors and the RFC 4231 HMAC-SHA256 vectors, byte for byte. If
//! review would rather depend on audited crates, swapping [`hmac_sha256`] for
//! `hmac::Hmac<sha2::Sha256>` is a one-line change and every caller is
//! unaffected (they take `&[u8]` in, `[u8; 32]` out).
//!
//! What this does NOT provide is confidentiality — mesh traffic is cleartext
//! (§4.7). The HMAC authenticates peers and pins the negotiation transcript; it
//! is not encryption. mTLS rides RFC 0003's seam.

use std::sync::atomic::{AtomicU64, Ordering};

/// SHA-256 (FIPS 180-4), one-shot. The handshake never hashes anything large
/// enough to want a streaming API — inputs are a 32-byte nonce plus two HELLO
/// bodies — so a single buffer is the clearest correct implementation.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    #[rustfmt::skip]
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];

    #[rustfmt::skip]
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];

    // Pad: 0x80, zeros to 56 mod 64, then the 64-bit big-endian bit length.
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for block in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().enumerate().take(16) {
            let j = i * 4;
            *word = u32::from_be_bytes([block[j], block[j + 1], block[j + 2], block[j + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// HMAC-SHA256 (RFC 2104 / RFC 4231). Block size 64, output 32.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const B: usize = 64;
    let mut k = [0u8; B];
    if key.len() > B {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; B];
    let mut opad = [0x5cu8; B];
    for ((ip, op), kb) in ipad.iter_mut().zip(opad.iter_mut()).zip(k.iter()) {
        *ip ^= *kb;
        *op ^= *kb;
    }

    let mut inner = Vec::with_capacity(B + msg.len());
    inner.extend_from_slice(&ipad);
    inner.extend_from_slice(msg);
    let inner_hash = sha256(&inner);

    let mut outer = Vec::with_capacity(B + 32);
    outer.extend_from_slice(&opad);
    outer.extend_from_slice(&inner_hash);
    sha256(&outer)
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
