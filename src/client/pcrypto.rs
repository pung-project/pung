// This file contains all of the cryptographic operations perform by the client
// including generation of labels, encryption and decryption of messages, etc.

use byteorder::{BigEndian, WriteBytesExt};

use capnp::Error;

use crypto::aead::{AeadDecryptor, AeadEncryptor};
use crypto::chacha20poly1305::ChaCha20Poly1305;
use crypto::digest::Digest;
use crypto::hkdf;
use crypto::hmac;
use crypto::mac::Mac;
use crypto::sha2::Sha256;

use db;

use std::io::Cursor;
use std::iter::repeat;
use std::mem;

pub const MESSAGE_SIZE: usize = db::CIPHER_SIZE;

/// Converts one or several unsigned integers `(u8, u16, u32, u64)` into a `Vec<u8>`
macro_rules! create_nonce {
    ( $( $x:ident ),* ) => {
        {
            let mut cursor = Cursor::new(Vec::new());
            $(
                cursor.write_uint::<BigEndian>($x, mem::size_of_val(&$x)).unwrap();
            )*
            cursor.into_inner()
        }
    };
}

/// Cryptographic keys
pub struct PungKeys {
    /// Key 1 used for label generation
    pub k_l: Vec<u8>,

    /// Key 2 used for label generation (used by PO2C optimization)
    pub k_l2: Vec<u8>,

    /// Key used for message encryption
    pub k_e: Vec<u8>,
}

/// Derives a pair of keys from a given secret. This function ensures the secret's randomness
/// is uniformly distributed prior to generating the keys.
pub fn derive_keys(secret: &[u8]) -> PungKeys {
    let digest = Sha256::new();
    let len = digest.output_bytes();

    // Buffer for pseudorandom key that will be used to generate the two keys
    let mut prk: Vec<u8> = repeat(0).take(len).collect();

    // This step is needed because the secret's randomness migt not be distributed uniformly or
    // an attacker may have partial information (e.g., knows the j bit is 1).
    hkdf::hkdf_extract(digest, &[0; 0], secret, &mut prk[..]);

    // Serves as a contiguous buffer for all 3 keys
    let mut okm: Vec<u8> = repeat(0).take(len * 3).collect();

    // Fills in the buffer with cryptographic key material
    hkdf::hkdf_expand(Sha256::new(), &prk[..], &[0; 0], &mut okm[..]);

    // Splits the buffer into the three keys.
    let mut k_l = okm.split_off(len);
    let k_l2 = k_l.split_off(len);

    assert_eq!(k_l.len(), len);
    assert_eq!(k_l2.len(), len);
    assert_eq!(okm.len(), len);

    // Key lenghts are 256-bits each.
    PungKeys {
        k_l: k_l,
        k_l2: k_l2,
        k_e: okm,
    }
}

/// Generates a Pung label from a round and a uid using a PRF keyed with
/// the label key.
pub fn gen_label(key: &[u8], round: u64, uid: u64, msg_num: u64, iter: u64) -> Vec<u8> {
    // Create PRF instance
    let mut prf = hmac::Hmac::new(Sha256::new(), &key[..]);

    let input: Vec<u8> = create_nonce!(round, uid, msg_num, iter);

    let mut output: Vec<u8> = repeat(0).take(prf.output_bytes()).collect();

    prf.input(&input[..]);

    // Comute PRF(round || uid)
    prf.raw_result(&mut output);

    output
}

/// Encrypts a message under the given round with the encryption key.
pub fn encrypt(key: &[u8], round: u64, message: &[u8]) -> (Vec<u8>, Vec<u8>) {
    assert!(message.len() <= MESSAGE_SIZE);

    let nonce: Vec<u8> = create_nonce!(round);

    // Sets up cryptosystem for the current round
    let mut ae = ChaCha20Poly1305::new(key, &nonce[..], &[0; 0]);

    // Performs the encryption
    let mut c: Vec<u8> = repeat(0).take(MESSAGE_SIZE).collect();
    let mut mac: Vec<u8> = repeat(0).take(16).collect(); // 128-bit tag

    // Pad message
    let mut padded_message: Vec<u8> = repeat(0).take(MESSAGE_SIZE).collect();
    padded_message[0..message.len()].clone_from_slice(message);

    ae.encrypt(&padded_message[..], &mut c[..], &mut mac[..]);

    (c, mac)
}

/// Decrypts and verifies the authenticity of a ciphertext and returns
/// the corresponding message or an error.
pub fn decrypt(key: &[u8], round: u64, c: &[u8], mac: &[u8]) -> Result<Vec<u8>, Error> {
    assert_eq!(c.len(), MESSAGE_SIZE);

    let nonce: Vec<u8> = create_nonce!(round);

    let mut ae = ChaCha20Poly1305::new(key, &nonce[..], &[0; 0]);

    // Performs the decryption
    let mut msg: Vec<u8> = repeat(0).take(c.len()).collect();

    if !ae.decrypt(c, &mut msg[..], mac) {
        Err(Error::failed(
            "Unable to decrypt ciphertext or verify mac".to_string(),
        ))
    } else {
        Ok(msg)
    }
}
