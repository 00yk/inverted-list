// function naming convention according to ziglang, use camelCase
//
use std::{collections::VecDeque, iter::FromIterator};
pub fn vbyteEncodeNumber(mut n: u32) -> Vec<u8> {
    let mut bytes: VecDeque<u8> = VecDeque::new();
    loop {
        bytes.push_front(((n % 128) as u8).to_le_bytes()[0]);
        if n < 128 {
            break;
        }
        n /= 128;
    }
    let n = bytes.len();
    bytes[n - 1] += 128;

    // bytes
    Vec::from_iter(bytes)
}
pub fn vbyteEncode(numbers: Vec<u32>) -> Vec<u8> {
    let mut bytes_stream = vec![];
    for num in numbers {
        let bytes = vbyteEncodeNumber(num);
        bytes_stream.extend(bytes);
    }
    bytes_stream
}
pub fn vbyteDecode(bytes_stream: Vec<u8>) -> Vec<u32> {
    let mut numbers: Vec<u32> = vec![];
    let mut n: u32 = 0;
    for i in 0..bytes_stream.len() {
        if bytes_stream[i] < 128 {
            n = 128 * n + bytes_stream[i] as u32;
        } else {
            n = 128 * n + (bytes_stream[i] - 128) as u32;
            numbers.push(n);
            n = 0;
        }
    }
    numbers
}
