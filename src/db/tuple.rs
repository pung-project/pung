use abomonation::Abomonation;
use std::cmp::Ordering;
use std::io::Write;
use std::ops::BitXor;
use std::ops::BitXorAssign;

use super::{PungTuple, CIPHER_SIZE, LABEL_SIZE, TUPLE_SIZE};
use util;

impl PungTuple {
    /// Creates a Pung tuple from a binary stream ([u8]).
    pub fn new(data: &[u8]) -> PungTuple {
        assert!(data.len() == TUPLE_SIZE);

        PungTuple {
            data: {
                let mut x = [0; TUPLE_SIZE];
                x.clone_from_slice(data);
                x
            },
        }
    }

    pub fn default() -> PungTuple {
        PungTuple {
            data: [0; TUPLE_SIZE],
        }
    }

    /// Serializes a Pung tuple to a binary stream (Vec<u8>).
    pub fn to_binary(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(TUPLE_SIZE);
        res.extend_from_slice(&self.data);
        res
    }

    /// Less-than compares a Pung tuple and some label.
    // XXX: This is slightly faster than self.label < label, but uses unsafe casts and assumes
    // (without checking) that label is a valid label (32 bytes).
    #[inline]
    pub fn lt(&self, label: &[u8]) -> bool {
        unsafe {
            (&*(self.label() as *const [u8] as *const [u64; 4]))
                < (&*(label as *const [u8] as *const [u64; 4]))
        }
    }

    /// Greater-than compares a Pung tuple and some label.
    //XXX: This is slightly faster than self.label > label, but uses unsafe casts and assumes
    // (without checking) that label is a valid label (32 bytes).
    #[inline]
    pub fn gt(&self, label: &[u8]) -> bool {
        unsafe {
            (&*(self.label() as *const [u8] as *const [u64; 4]))
                > (&*(label as *const [u8] as *const [u64; 4]))
        }
    }

    #[inline]
    pub fn label(&self) -> &[u8] {
        &self.data[..LABEL_SIZE]
    }

    /// Returns a slice to the cipher-only portion of a Pung tuple.
    #[inline]
    pub fn cipher(&self) -> &[u8] {
        &self.data[LABEL_SIZE..LABEL_SIZE + CIPHER_SIZE]
    }

    /// Returns a slice to the mac portion of a Pung tuple.
    #[inline]
    pub fn mac(&self) -> &[u8] {
        &self.data[LABEL_SIZE + CIPHER_SIZE..]
    }
}

impl<'a> BitXor for &'a PungTuple {
    type Output = PungTuple;

    fn bitxor(self, other: &PungTuple) -> PungTuple {
        assert_eq!(self.data.len(), other.data.len());

        let mut xored_tuple = self.clone();

        for i in 0..self.data.len() {
            xored_tuple.data[i] ^= other.data[i];
        }

        xored_tuple
    }
}

impl BitXorAssign for PungTuple {
    fn bitxor_assign(&mut self, other: PungTuple) {
        assert_eq!(self.data.len(), other.data.len());

        let len = self.data.len();

        for i in 0..len {
            self.data[i] ^= other.data[i];
        }
    }
}

impl PartialEq for PungTuple {
    #[inline]
    fn eq(&self, other: &PungTuple) -> bool {
        unsafe {
            (&*(self.label() as *const [u8] as *const [u64; 4]))
                .eq(&*(other.label() as *const [u8] as *const [u64; 4]))
        }
    }
}

impl Eq for PungTuple {}

impl Ord for PungTuple {
    #[inline]
    fn cmp(&self, other: &PungTuple) -> Ordering {
        util::label_cmp(self.label(), other.label())
    }
}

impl PartialOrd for PungTuple {
    #[inline]
    fn partial_cmp(&self, other: &PungTuple) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Clone for PungTuple {
    #[inline]
    fn clone(&self) -> PungTuple {
        PungTuple { data: self.data }
    }
}

impl Abomonation for PungTuple {
    unsafe fn embalm(&mut self) {}

    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        bytes.write_all(&self.data).unwrap();
    }

    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let temp = bytes;

        bytes = if TUPLE_SIZE <= temp.len() {
            let (mine, rest) = temp.split_at_mut(TUPLE_SIZE);
            self.data = *(mine.as_ptr() as *const [u8; TUPLE_SIZE]);
            rest
        } else {
            return None;
        };

        Some(bytes)
    }
}
