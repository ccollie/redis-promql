use std::cmp::{Ordering, PartialEq};
use std::convert::TryInto;
use std::io::{self, Read};

const CHUNK_COMPACT_CAPACITY_THRESHOLD: usize = 32;

#[derive(Clone)]
pub struct BStream {
    stream: Vec<u8>,
    count: usize,
}

impl BStream {
    fn new() -> Self {
        Self {
            stream: vec![0; 2],
            count: 0,
        }
    }

    fn reset(&mut self, stream: Vec<u8>) {
        self.stream = stream;
        self.count = 0;
    }

    fn bytes(&self) -> &[u8] {
        &self.stream
    }

    fn write_byte(&mut self, b: u8) {
        self.stream.push(b);
        self.count += 8;
    }

    fn write_bits(&mut self, v: u64, n: usize) {
        let mut v = v;
        let mut n = n;
        while n > 0 {
            let mut b = self.stream.pop().unwrap_or(0);
            let bits_to_write = n.min(8 - (self.count % 8));
            b |= ((v as u8) & ((1 << bits_to_write) - 1)) << (self.count % 8);
            self.stream.push(b);
            v >>= bits_to_write;
            self.count += bits_to_write;
            n -= bits_to_write;
        }
    }

    fn read_bits(&mut self, n: usize) -> u64 {
        let mut v = 0;
        for i in 0..n {
            let byte_idx = self.count / 8;
            let bit_idx = self.count % 8;
            if byte_idx < self.stream.len() {
                v |= ((self.stream[byte_idx] >> bit_idx) & 1) << i;
            }
            self.count += 1;
        }
        v as u64
    }
}

pub struct XORChunk {
    b: BStream,
}

impl PartialEq for ValueType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ValueType::ValNone, ValueType::ValNone) => true,
            (ValueType::ValFloat, ValueType::ValFloat) => true,
            _ => false,
        }
    }
}

impl XORChunk {
    fn new() -> Self {
        Self { b: BStream::new() }
    }

    fn reset(&mut self, stream: Vec<u8>) {
        self.b.reset(stream);
    }

    fn encoding(&self) -> Encoding {
        Encoding::XOR
    }

    fn bytes(&self) -> &[u8] {
        self.b.bytes()
    }

    fn num_samples(&self) -> usize {
        u16::from_be_bytes(self.b.bytes()[0..2].try_into().unwrap()) as usize
    }

    fn compact(&mut self) {
        if self.b.stream.capacity() > self.b.stream.len() + CHUNK_COMPACT_CAPACITY_THRESHOLD {
            self.b.stream.shrink_to_fit();
        }
    }

    fn appender(&self) -> Result<XORAppender, io::Error> {
        let mut it = self.iterator(None);
        while it.next() != ValueType::ValNone {}
        if it.err.is_some() {
            return Err(io::Error::new(io::ErrorKind::Other, "iterator error"));
        }
        let mut a = XORAppender {
            b: self.b.clone(),
            t: it.t,
            v: it.val,
            t_delta: it.t_delta,
            leading: it.leading,
            trailing: it.trailing,
        };
        if it.num_total == 0 {
            a.leading = 0xff;
        }
        Ok(a)
    }

    fn iterator(&self, it: Option<XORIterator>) -> XORIterator {
        if let Some(mut xor_iter) = it {
            xor_iter.reset(self.b.bytes().to_vec());
            xor_iter
        } else {
            XORIterator {
                br: BStreamReader::new(self.b.bytes()[2..].to_vec()),
                num_total: u16::from_be_bytes(self.b.bytes()[0..2].try_into().unwrap()),
                num_read: 0,
                t: i64::MIN,
                val: 0.0,
                leading: 0,
                trailing: 0,
                t_delta: 0,
                err: None,
            }
        }
    }
}

enum Encoding {
    XOR,
}

struct XORAppender {
    b: BStream,
    t: i64,
    v: f64,
    t_delta: u64,
    leading: u8,
    trailing: u8,
}

impl XORAppender {
    fn append(&mut self, t: i64, v: f64) {
        let num = u16::from_be_bytes(self.b.bytes()[0..2].try_into().unwrap());
        let mut t_delta: u64 = 0;
        match num {
            0 => {
                self.b.write_bits(t as u64, 64);
                self.b.write_bits(v.to_bits(), 64);
            }
            1 => {
                t_delta = (t - self.t) as u64;
                self.b.write_bits(t_delta, 64);
                self.write_v_delta(v);
            }
            _ => {
                t_delta = (t - self.t) as u64;
                let dod = (t_delta as i64 - self.t_delta as i64);
                match dod.cmp(&0) {
                    Ordering::Equal => self.b.write_bits(0, 1),
                    Ordering::Less => {
                        if bit_range(dod, 14) {
                            self.b.write_bits(2, 2);
                            self.b.write_bits(dod as u64, 14);
                        } else if bit_range(dod, 17) {
                            self.b.write_bits(6, 3);
                            self.b.write_bits(dod as u64, 17);
                        } else if bit_range(dod, 20) {
                            self.b.write_bits(14, 4);
                            self.b.write_bits(dod as u64, 20);
                        } else {
                            self.b.write_bits(15, 4);
                            self.b.write_bits(dod as u64, 64);
                        }
                    }
                    Ordering::Greater => {
                        if bit_range(dod, 14) {
                            self.b.write_bits(2, 2);
                            self.b.write_bits(dod as u64, 14);
                        } else if bit_range(dod, 17) {
                            self.b.write_bits(6, 3);
                            self.b.write_bits(dod as u64, 17);
                        } else if bit_range(dod, 20) {
                            self.b.write_bits(14, 4);
                            self.b.write_bits(dod as u64, 20);
                        } else {
                            self.b.write_bits(15, 4);
                            self.b.write_bits(dod as u64, 64);
                        }
                    }
                }
                self.write_v_delta(v);
            }
        }
        self.t = t;
        self.v = v;
        self.t_delta = t_delta;
        self.b.stream[0..2].copy_from_slice(&(num + 1).to_be_bytes());
    }

    fn write_v_delta(&mut self, v: f64) {
        xor_write(&mut self.b, v, self.v, &mut self.leading, &mut self.trailing);
    }
}

fn bit_range(x: i64, nbits: u8) -> bool {
    let min = -(1 << (nbits - 1)) + 1;
    let max = 1 << (nbits - 1);
    x >= min && x <= max
}

fn xor_write(b: &mut BStream, new_value: f64, current_value: f64, leading: &mut u8, trailing: &mut u8) {
    let delta = new_value.to_bits() ^ current_value.to_bits();
    if delta == 0 {
        b.write_bits(0, 1);
        return;
    }
    b.write_bits(1, 1);
    let new_leading = delta.leading_zeros() as u8;
    let new_trailing = delta.trailing_zeros() as u8;
    if *leading != 0xff && new_leading >= *leading && new_trailing >= *trailing {
        b.write_bits(0, 1);
        b.write_bits(delta >> *trailing, 64 - *leading as usize - *trailing as usize);
        return;
    }
    *leading = new_leading;
    *trailing = new_trailing;
    b.write_bits(1, 1);
    b.write_bits(new_leading as u64, 5);
    let sigbits = 64 - new_leading as usize - new_trailing as usize;
    b.write_bits(sigbits as u64, 6);
    b.write_bits(delta >> new_trailing, sigbits);
}

struct XORIterator {
    br: BStreamReader,
    num_total: u16,
    num_read: u16,
    t: i64,
    val: f64,
    leading: u8,
    trailing: u8,
    t_delta: u64,
    err: Option<io::Error>,
}

impl XORIterator {
    fn reset(&mut self, b: Vec<u8>) {
        self.br = BStreamReader::new(b);
        self.num_total = 0;
        self.num_read = 0;
        self.t = 0;
        self.val = 0.0;
        self.leading = 0;
        self.trailing = 0;
        self.t_delta = 0;
        self.err = None;
    }

    fn next(&mut self) -> ValueType {
        if self.err.is_some() || self.num_read == self.num_total {
            return ValueType::ValNone;
        }
        if self.num_read == 0 {
            self.t = self.br.read_bits(64) as i64;
            self.val = f64::from_bits(self.br.read_bits(64));
            self.num_read += 1;
            return ValueType::ValFloat;
        }
        if self.num_read == 1 {
            self.t_delta = self.br.read_bits(64);
            self.t += self.t_delta as i64;
            return self.read_value();
        }
        let mut d = 0;
        for _ in 0..4 {
            d <<= 1;
            match self.br.read_bit() {
                Ok(0) => break,
                Ok(1) => d |= 1,
                Err(e) => {
                    self.err = Some(e);
                    return ValueType::ValNone;
                }
            }
        }
        let (sz, dod) = match d {
            0 => (0, 0),
            2 => (14, self.br.read_bits(14) as i64),
            6 => (17, self.br.read_bits(17) as i64),
            14 => (20, self.br.read_bits(20) as i64),
            15 => (64, self.br.read_bits(64) as i64),
            _ => {
                self.err = Some(io::Error::new(io::ErrorKind::Other, "invalid delta-of-delta encoding"));
                return ValueType::ValNone;
            }
        };
        self.t_delta = (self.t_delta as i64 + dod) as u64;
        self.t += self.t_delta as i64;
        self.read_value()
    }

    fn read_value(&mut self) -> ValueType {
        if let Err(e) = xor_read(&mut self.br, &mut self.val, &mut self.leading, &mut self.trailing) {
            self.err = Some(e);
            return ValueType::ValNone;
        }
        self.num_read += 1;
        ValueType::ValFloat
    }
}

fn xor_read(br: &mut BStreamReader, value: &mut f64, leading: &mut u8, trailing: &mut u8) -> Result<(), io::Error> {
    let bit = br.read_bit()?;
    if bit == 0 {
        return Ok(());
    }
    let bit = br.read_bit()?;
    let (new_leading, mbits) = if bit == 0 {
        (*leading, 64 - *leading as usize - *trailing as usize)
    } else {
        (*leading, br.read_bits(5) as usize)
    };
    let mbits = if mbits == 0 { 64 } else { mbits };
    let new_trailing = 64 - new_leading as usize - mbits;
    *leading = new_leading;
    *trailing = new_trailing as u8;
    let bits = br.read_bits(mbits);
    let vbits = value.to_bits() ^ (bits << new_trailing);
    *value = f64::from_bits(vbits);
    Ok(())
}

pub struct BStreamReader {
    stream: Vec<u8>,
    count: usize,
}

impl BStreamReader {
    fn new(stream: Vec<u8>) -> Self {
        Self { stream, count: 0 }
    }

    fn read_bit(&mut self) -> Result<u8, io::Error> {
        let byte_idx = self.count / 8;
        let bit_idx = self.count % 8;
        if byte_idx >= self.stream.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of stream"));
        }
        self.count += 1;
        Ok((self.stream[byte_idx] >> bit_idx) & 1)
    }

    fn read_bits(&mut self, n: usize) -> u64 {
        let mut v: u64 = 0;
        for i in 0..n {
            let byte_idx = self.count / 8;
            let bit_idx = self.count % 8;
            if byte_idx < self.stream.len() {
                v = v | ((self.stream[byte_idx] >> bit_idx) & 1) << i;
            }
            self.count += 1;
        }
        v
    }
}

enum ValueType {
    ValNone,
    ValFloat,
}

fn main() {
    // Example usage
    let mut chunk = XORChunk::new();
    let mut appender = chunk.appender().unwrap();
    appender.append(1, 1.0);
    appender.append(2, 2.0);
    let mut iter = chunk.iterator(None);
    while iter.next() != ValueType::ValNone {
        let (t, v) = iter.t;
        println!("Timestamp: {}, Value: {}", t, v);
    }
}