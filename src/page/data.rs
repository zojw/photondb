use std::cmp::Ordering;

use super::codec::{BufReader, BufWriter, DecodeFrom, EncodeTo};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Key<'a> {
    pub(crate) raw: &'a [u8],
    pub(crate) lsn: u64,
}

impl<'a> Key<'a> {
    pub(crate) fn new(raw: &'a [u8], lsn: u64) -> Self {
        Self { raw, lsn }
    }
}

impl Ord for Key<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.raw.cmp(other.raw) {
            Ordering::Equal => other.lsn.cmp(&self.lsn),
            o => o,
        }
    }
}

impl PartialOrd for Key<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl EncodeTo for &[u8] {
    fn encode_size(&self) -> usize {
        BufWriter::length_prefixed_slice_size(self)
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        w.put_length_prefixed_slice(self);
    }
}

impl DecodeFrom for &[u8] {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        r.get_length_prefixed_slice()
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum Value<'a> {
    Put(&'a [u8]),
    Delete,
}

impl EncodeTo for Value<'_> {
    fn encode_size(&self) -> usize {
        match self {
            Self::Put(v) => 1 + v.len(),
            Self::Delete => 1,
        }
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        todo!()
    }
}

impl DecodeFrom for Value<'_> {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        todo!()
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Index {
    pub(crate) id: u64,
    pub(crate) epoch: u64,
}

impl Index {
    pub(crate) fn new(id: u64) -> Self {
        Self { id, epoch: 0 }
    }
}

impl EncodeTo for Index {
    fn encode_size(&self) -> usize {
        todo!()
    }

    unsafe fn encode_to(&self, w: &mut BufWriter) {
        todo!()
    }
}

impl DecodeFrom for Index {
    unsafe fn decode_from(r: &mut BufReader) -> Self {
        todo!()
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct Range<'a> {
    pub(crate) left: &'a [u8],
    pub(crate) right: Option<&'a [u8]>,
}