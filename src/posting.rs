use std::{fs::File, io::BufRead};
use std::io::{BufReader, Read, Write, BufWriter};
use std::io::Cursor;
use std::collections::VecDeque;
use serde::{Serialize, Deserialize};
use byteorder::{LittleEndian, ReadBytesExt};
#[derive(Serialize, Deserialize, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Posting {
    // word: String,
    pub term_ID: u32,
    pub doc_ID: u32,
    pub freq: u32  // use percentage or num count?
}

/// batched write postings to intermediate format
/// used for both intermediate postings and merged postings
#[cfg(not(feature = "binary-posting"))]
pub fn offload_vector_of_postings(posting_vec: &Vec<Posting>, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = File::create(filename)?;
    let mut buf: Vec<String> = Vec::new();
    for p in posting_vec {
        buf.push(format!("{} {} {}", p.term_ID, p.doc_ID, p.freq));
        if buf.len() >= 204800  {
            let joined_s = buf.join("\n");
            f.write_all(joined_s.as_bytes())?;
            buf.clear();
        }

        // writeln!(&mut f, "{} {} {}", p.term_ID, p.doc_ID, p.freq)?;
    }
    let joined_s = buf.join("\n");
    f.write_all(&joined_s.as_bytes())?;
    Ok(())
}

#[cfg(feature = "binary-posting")]
pub fn offload_vector_of_postings(posting_vec: &Vec<Posting>, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = File::create(filename)?;
    let mut buf: Vec<u8> = Vec::new();
    for p in posting_vec {
        buf.extend_from_slice(&p.term_ID.to_le_bytes());
        buf.extend_from_slice(&p.doc_ID.to_le_bytes());
        buf.extend_from_slice(&p.freq.to_le_bytes());
        if buf.len() >= 204800  {
            f.write_all(&buf)?;
            buf.clear();
        }
    }
    f.write_all(&buf)?;
    Ok(())
}

/// abstraction for partial read functionality used in external k-way merge
#[cfg(not(feature = "binary-posting"))]
pub struct CachedFile {
    buf: std::io::Lines<BufReader<File>>
}
#[cfg(not(feature = "binary-posting"))]
impl CachedFile {
    pub fn new(filename: &str) -> Self {
        let f = File::open(filename).unwrap();
        let r = BufReader::new(f);
        CachedFile {
            buf: r.lines(),
        }
    }
    pub fn forward(&mut self, num: u32) -> Option<VecDeque<Posting>> {
        let mut res = VecDeque::new();
        let mut cnt = 0;
        let mut ok = false;
        while let Some(line) = self.buf.next() {
            ok = true;
            if let Ok(content) = line {
                let v = content.split_whitespace().map(|e| e.parse::<u32>().unwrap()).collect::<Vec<u32>>();
                res.push_back(Posting { term_ID: v[0], doc_ID: v[1], freq: v[2] });
                cnt += 1;
                if cnt == num {
                    break;
                }
            }
        }
        if !ok {
            return None;
        }
        Some(res)
    }

}

#[cfg(feature = "binary-posting")]
pub struct CachedFile {
    buf: BufReader<File>
}
#[cfg(feature = "binary-posting")]
impl CachedFile {
    pub fn new(filename: &str) -> Self {
        let f = File::open(filename).unwrap();
        let r = BufReader::new(f);
        CachedFile {
            buf: r,
        }
    }
    pub fn forward(&mut self, num: u32) -> Option<VecDeque<Posting>> {
        let mut res = VecDeque::new();
        let mut cnt = 0;
        let num_bytes_to_read = 4 * 3 * num as u64;
        // println!("num_bytes_to_read {}", num_bytes_to_read);

        let mut ok = false;
        if let Some(content) = read_n(&mut self.buf, num_bytes_to_read) {
            ok = true;
            let mut rdr = Cursor::new(content);
            for i in 0..num {
                let term_ID;
                if let Ok(n) = rdr.read_u32::<LittleEndian>() {
                    term_ID = n;
                }
                else {
                    break;
                }
                let doc_ID: u32 = rdr.read_u32::<LittleEndian>().unwrap();
                let freq: u32 = rdr.read_u32::<LittleEndian>().unwrap();
                res.push_back(Posting { term_ID: term_ID, doc_ID: doc_ID, freq: freq });
            }
        }
        if !ok {
            return None;
        }
        Some(res)
    }
}

pub fn read_n<R>(reader: R, bytes_to_read: u64) -> Option<Vec<u8>>
where
    R: Read,
{
    let mut buf = vec![];
    let mut chunk = reader.take(bytes_to_read);
    let n = chunk.read_to_end(&mut buf).expect("Didn't read enough");

    if n == 0 {
        return None;
    }
    Some(buf)
}
// currently not used, use CachedFile for partial read
#[cfg(feature = "binary-posting")]
pub fn read_vector_of_postings(filename: &str) -> Vec<Posting>{
    let f = File::open(filename).unwrap();
    let mut r = BufReader::new(f);
    let mut res: Vec<Posting> = Vec::new();
    const num_bytes_to_read: u64 = 12;
    while let Some(content) = read_n(&mut r, num_bytes_to_read) {
        let mut rdr = Cursor::new(content);
            let term_ID: u32 = rdr.read_u32::<LittleEndian>().unwrap();
            let doc_ID: u32 = rdr.read_u32::<LittleEndian>().unwrap();
            let freq: u32 = rdr.read_u32::<LittleEndian>().unwrap();
            res.push(Posting { term_ID: term_ID, doc_ID: doc_ID, freq: freq });
    }
    res
}
#[cfg(not(feature = "binary-posting"))]
pub fn read_vector_of_postings(filename: &str) -> Vec<Posting>{
    let f = File::open(filename).unwrap();
    let r = BufReader::new(f);
    let mut res: Vec<Posting> = Vec::new();
    let forwarder = r.lines();
    for line in forwarder {
        if let Ok(content) = line {
            let v = content.split_whitespace().map(|e| e.parse::<u32>().unwrap()).collect::<Vec<u32>>();
            res.push(Posting { term_ID: v[0], doc_ID: v[1], freq: v[2] });
        }
    }
    res
}
pub fn offload_tmp_file(posting_vec: &mut Vec<Posting>) {
    // in-mem sort posting vec
    posting_vec.sort();
    let mut now: String = chrono::offset::Utc::now().to_string();
    now.push_str(".intermediate");

    offload_vector_of_postings(&posting_vec, &now).unwrap();
}
