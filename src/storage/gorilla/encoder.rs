use crate::common::types::Sample;
use bitstream_io::{BigEndian, BitWrite, BitWriter};
use rusty_chunkenc::uvarint::write_uvarint;
use rusty_chunkenc::varbit_ts::write_varbit_ts;
use rusty_chunkenc::varbit_xor::write_varbit_xor;
use rusty_chunkenc::varint::write_varint;
use smallvec::SmallVec;
use std::io::Write;

struct SaveState {
    pos: usize,
    byte: u8,
}

pub struct XORChunkEncoder {
    writer: BitWriter<Vec<u8>, BigEndian>,
    num_samples: usize,
    saved_state: Option<SaveState>,
    pub timestamp: i64,
    pub value: f64,
    pub leading_bits_count: u8,
    pub trailing_bits_count: u8,
    pub timestamp_delta: i64,
}

impl XORChunkEncoder {
    pub fn new() -> XORChunkEncoder {
        let writer = BitWriter::endian(vec![], BigEndian);

        XORChunkEncoder {
            writer,
            num_samples: 0,
            saved_state: None,
            timestamp: 0,
            value: 0.0,
            leading_bits_count: 0,
            trailing_bits_count: 0,
            timestamp_delta: 0,
        }
    }

    pub fn add_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        match self.num_samples {
            0 => self.write_first_sample(sample),
            1 => self.write_second_sample(sample),
            _ => self.write_n_sample(sample),
        }
    }

    fn write_first_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let mut bytes: Vec<u8> = Vec::with_capacity(32);
        write_varint(sample.timestamp, &mut bytes)?;
        // Classic Float64 for the value
        bytes.write_all(&sample.value.to_be_bytes())?;

        self.writer = BitWriter::endian(bytes, BigEndian);
        self.num_samples += 1;
        Ok(())
    }

    fn write_second_sample(&mut self, sample: &Sample) -> std::io::Result<()> {

        let timestamp_delta = sample.timestamp - self.timestamp;
        if timestamp_delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }

        // I didn't find a more beautiful way to write the uvarint in the bitstream directly,
        // we use SmallVec so at least it's allocated on the stack and not the heap.
        let mut uvarint_bytes = SmallVec::<u8, 9>::new();
        write_uvarint(timestamp_delta as u64, &mut uvarint_bytes)?;
        self.writer.write_bytes(&uvarint_bytes)?;

        let (leading, trailing) = write_varbit_xor(
            sample.value,
            self.value,
            0xff,
            0,
            &mut self.writer
        )?;

        self.timestamp = sample.timestamp;
        self.value = sample.value;
        self.leading_bits_count = leading;
        self.trailing_bits_count = trailing;
        self.timestamp_delta = timestamp_delta;

        self.num_samples += 1;

        Ok(())
    }

    fn write_n_sample(&mut self, sample: &Sample) -> std::io::Result<()> {
        let timestamp = sample.timestamp;
        let value = sample.value;

        let timestamp_delta = timestamp - self.timestamp;
        if timestamp_delta < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "samples aren't sorted by timestamp ascending",
            ));
        }
        let timestamp_delta_of_delta = timestamp_delta - self.timestamp_delta;

        write_varbit_ts(timestamp_delta_of_delta, &mut self.writer)?;

        let (leading_bits_count, trailing_bits_count) = write_varbit_xor(
            value,
            self.value,
            self.leading_bits_count,
            self.trailing_bits_count,
            &mut self.writer,
        )?;

        self.timestamp = timestamp;
        self.value = value;
        self.leading_bits_count = leading_bits_count;
        self.trailing_bits_count = trailing_bits_count;
        self.timestamp_delta = timestamp_delta;

        self.num_samples += 1;

        Ok(())
    }

    fn save_state(&mut self) {
        let was_aligned = self.writer.byte_aligned();
        self.writer.byte_align();

        self.save_pos = self.writer.position();
        self.save_byte = self.writer.byte();
    }

    pub fn iter(&self) -> std::slice::Iter<u8> {
        let was_aligned = self.writer.byte_aligned();

        self.buf.iter()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Sample;
    use rand::{Rng, SeedableRng};

    fn generate_random_test_data(seed: u64) -> Vec<Vec<Sample>> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        let mut test_cases = Vec::with_capacity(128);
        for _ in 0..128 {
            let mut timestamp: i64 = rng.gen_range(1234567890..1357908642);
            let vec_size = rng.gen_range(1..129);
            let mut vec = Vec::with_capacity(vec_size);

            let mut value: f64 = if rng.gen_bool(0.5) {
                rng.gen_range(-100000000.0..1000000.0)
            } else {
                rng.gen_range(-10000.0..10000.0)
            };
            vec.push(Sample { timestamp, value });

            for _ in 1..vec_size {
                timestamp += rng.gen_range(1..30);
                if rng.gen_bool(0.33) {
                    value += 1.0;
                } else if rng.gen_bool(0.33) {
                    value = rng.gen();
                }
                vec.push(Sample { timestamp, value });
            }
            test_cases.push(vec);
        }
        test_cases
    }
}
