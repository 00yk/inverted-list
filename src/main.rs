use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::{fs::File, io::BufRead};
use std::io::{BufReader, Read, Write, BufWriter};
use std::mem::size_of_val;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

use glob::glob;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Posting {
    // word: String,
    term_ID: u32,
    doc_ID: u32,
    freq: u32  // use percentage or num count?
}

/// batched write postings to intermediate format
#[cfg(not(feature = "binary-posting"))]
fn offload_vector_of_postings(posting_vec: &Vec<Posting>, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
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
fn offload_vector_of_postings(posting_vec: &Vec<Posting>, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
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
struct CachedFile {
    buf: std::io::Lines<BufReader<File>>
}
#[cfg(not(feature = "binary-posting"))]
impl CachedFile {
    fn new(filename: &str) -> Self {
        let f = File::open(filename).unwrap();
        let r = BufReader::new(f);
        CachedFile {
            buf: r.lines(),
        }
    }
    fn forward(&mut self, num: u32) -> Option<VecDeque<Posting>> {
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
struct CachedFile {
    buf: BufReader<File>
}
#[cfg(feature = "binary-posting")]
impl CachedFile {
    fn new(filename: &str) -> Self {
        let f = File::open(filename).unwrap();
        let r = BufReader::new(f);
        CachedFile {
            buf: r,
        }
    }
    fn forward(&mut self, num: u32) -> Option<VecDeque<Posting>> {
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
fn read_vector_of_postings(filename: &str) -> Vec<Posting>{
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
fn offload_tmp_file(posting_vec: &mut Vec<Posting>) {
    // in-mem sort posting vec
    posting_vec.sort();
    let mut now: String = chrono::offset::Utc::now().to_string();
    now.push_str(".intermediate");

    offload_vector_of_postings(&posting_vec, &now).unwrap();
}

#[derive(Serialize, Deserialize)]
struct LexiconValue {
    pos: u32,
    len: u32,
}

// later change compare function to only consider posting
#[derive(Ord, Eq, PartialOrd, PartialEq, Debug)]
struct HeapNode {
    p: Posting,
    index: u8 // which queue it belongs to, k-way merge no more that 255-way
}

/// external k way merge
fn k_way_merge(files: Vec<String>) {
    // 1-way-merge
    if files.len() == 0 {
        panic!("to-be merged files not found.");
    }
    if files.len() == 1 {
        panic!("only a to-be merged file.");
    }
    // build reader
    let mut cached_files: Vec<CachedFile> = Vec::new();
    for f in files {
        cached_files.push(CachedFile::new(&f));
    }
    let mut heap: BinaryHeap<Reverse<HeapNode>> = BinaryHeap::new(); // min heap
    // push heads to init priority queue
    let mut buf: Vec<VecDeque<Posting>> = Vec::new();
    let batch_size = 5;
    for c in cached_files.iter_mut() {
        if let Some(postings_deque) = c.forward(batch_size) {
            buf.push(postings_deque);
        }
        else {
            unreachable!();
        }
    }
    for (i, postings_deque) in buf.iter_mut().enumerate() {
        if let Some(p) = postings_deque.pop_front() {
            let heapnode = HeapNode { p: p, index: i as u8 };
            heap.push(Reverse(heapnode));
        }
        else {
            unreachable!();
        }
    }
    let mut output_buf: Vec<Posting> = Vec::new();
    while !heap.is_empty() {
        if let Some(ele) = heap.pop() {
            // println!("-------------Iteration-----------------------------");
            let p = ele.0.p; // unwrap Reverse struct
            let i = ele.0.index;
            output_buf.push(p);
            // push another element from the same queue
            if let Some(p) = buf[i as usize].pop_front() {
                let heapnode = HeapNode { p: p, index: i };
                heap.push(Reverse(heapnode));
            }
            else {
                // forward corresponding cachedfile to get a new queue
                if let Some(posting_deque) = cached_files[i as usize].forward(batch_size) {
                    buf[i as usize] = posting_deque;
                    // then push the front element to heap
                    if let Some(p) = buf[i as usize].pop_front() {
                        let heapnode = HeapNode { p: p, index: i };
                        heap.push(Reverse(heapnode));
                    }
                    else {
                        unreachable!();
                    }
                    // println!("#### newly forwarded deque: {:?}", buf[i as usize]);
                }
                else {
                    // cachedfile is over, then leave empty queue where it is
                }
            }
            // println!("output_buf: {:?}", output_buf);
            // println!("heap: {:?}", heap);
            // println!("queues: {:?}", buf);
        }
    }
    // println!("*************over");
    // println!("length of vector of posting: {}", output_buf.len());
    // println!("output_buf: {:?}", output_buf);
    // println!("heap: {:?}", heap);
    // println!("queues: {:?}", buf);
    offload_vector_of_postings(&output_buf, "merged_postings.tmp").unwrap();

}
/// helper function for ease of use
fn dumping_to_file<T: Serialize>(object: &T, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = File::create(filename)?;
    dumping(object, &mut f)
}

/// used in pair with deserialize_to_mem
#[cfg(feature = "binary-format")]
fn dumping<T: Serialize>(object: &T, f: &mut File) -> Result<(), Box<dyn std::error::Error>> {
    // let ser = bincode::serialize(object)?; // bincode
    let ser = rmp_serde::to_vec(object)?; // messagepack
    let mut encoder = lz4::EncoderBuilder::new().level(1).build(f)?;
    encoder.write_all(&ser)?;
    let (_, result) = encoder.finish();
    result?;
    Ok(())


}

#[cfg(not(feature = "binary-format"))]
fn dumping<T: Serialize>(object: &T, f: &mut File) -> Result<(), Box<dyn std::error::Error>> {
    let ser = serde_json::to_vec(object)?; // json
    f.write_all(&ser)?;
    Ok(())


}
// #[cfg(feature = "binary-format")]
// fn dumping_batch<T: Serialize>(object: &T, f: &mut BufWriter) -> Result<(), Box<dyn std::error::Error>> {
//     // let ser = bincode::serialize(object)?; // bincode
//     let ser = rmp_serde::to_vec(object)?; // messagepack
//     // let ser = serde_json::to_vec(object)?; // json
//     // let mut f = File::create(filename)?;
//     // file.write_all(&ser)?;
//     // Ok(())
//     let mut encoder = lz4::EncoderBuilder::new().level(1).build(f)?;
//     encoder.write(&ser)?;
//     let (_, result) = encoder.finish();
//     result?;
//     Ok(())


// }

// #[cfg(not(feature = "binary-format"))]
// fn dumping_batch<T: Serialize>(object: &T, f: &mut BufWriter<W>) -> Result<(), Box<dyn std::error::Error>> {
//     // let ser = bincode::serialize(object)?; // bincode
//     // let ser = rmp_serde::to_vec(object)?; // messagepack
//     let ser = serde_json::to_vec(object)?; // json
//     // let mut f = File::create(filename)?;
//     f.write(&ser)?;
//     Ok(())
//     // let mut encoder = lz4::EncoderBuilder::new().level(1).build(f)?;
//     // encoder.write_all(&ser)?;
//     // let (_, result) = encoder.finish();
//     // result?;
//     // Ok(())


// }
/// used in pair with dumping_to_file
#[cfg(feature = "binary-format")]
fn deserialize_to_mem<T: serde::de::DeserializeOwned>(filename: &str) -> Result<T, Box<dyn std::error::Error>> {
    let f = File::open(filename)?;
    let reader = BufReader::new(f);
    let mut lz4_reader = lz4::Decoder::new(reader)?;
    Ok(rmp_serde::from_read(lz4_reader)?) // messagepack
    // Ok(bincode::deserialize_from(reader)?) // bincode
}

#[cfg(not(feature = "binary-format"))]
fn deserialize_to_mem<T: serde::de::DeserializeOwned>(filename: &str) -> Result<T, Box<dyn std::error::Error>> {
    let f = File::open(filename)?;
    let reader = BufReader::new(f);
    Ok(serde_json::from_reader(reader)?) // json
}

/// build inverted index and lexicon together
/// need to read in the term_ID_to_term mapping, and merged_postings file
/// maybe bottleneck: dumping cur_inverted_list should perhaps use a BufWriter to
/// perform batched writing.
fn build_inverted_index_and_lexicon(){
    // read in term_to_term_ID mapping
    let term_ID_to_term: BTreeMap<u32, String> = deserialize_to_mem("term_ID_to_term.tmp").unwrap();

    // let postings: Vec<Posting> = deserialize_to_mem("merged_postings.tmp").unwrap();
    let postings: Vec<Posting> = read_vector_of_postings("merged_postings.tmp");
    let mut lexicon: BTreeMap<String, LexiconValue> = BTreeMap::new(); // term to start index in inverted index
    let mut cur_inverted_list: Vec<(u32, u32)> = Vec::new();
    let mut num_inverted_list = 0;
    let mut f = File::create("inverted_list.tmp").unwrap();
    for p in postings {
        let word = term_ID_to_term.get(&p.term_ID).unwrap();
        if let Some(value) = lexicon.get(word) {
            // if already in the middle of building a inverted list for p.word
            // then keep pushing current posting doc_ID to the inverted list
            cur_inverted_list.push((p.doc_ID, p.freq));
            // update total inverted list len in lexicon for this term
            lexicon.entry(word.clone()).and_modify(|e| { e.len += 1 });
        }
        else {
            // println!("Dumping {}: {:?}", word, cur_inverted_list);
            // offload_inverted_list(&cur_inverted_list);
            dumping(&cur_inverted_list, &mut f).unwrap();
            // if this a new inverted list
            // insert this term into lexicon first with
            lexicon.insert(word.clone(), LexiconValue { pos: num_inverted_list, len: 0 });
            // update total number of inverted list
            num_inverted_list += 1;
            // create a inverted list
            cur_inverted_list = Vec::new();
            // push the first posting onto the inverted list
            cur_inverted_list.push((p.doc_ID, p.freq));
        }
    }
    dumping_to_file(&lexicon, "lexicon.tmp").unwrap();
}
fn offload_dict(doc_ID: u32, dict: BTreeMap<String, u32>, vec: &mut Vec<Posting>, term_to_term_ID: &BTreeMap<String, u32>) {
     // dumping [word]freq to Vec<Posting>
     for (word, freq) in dict {
         let term_ID = *term_to_term_ID.get(&word).unwrap();
         vec.push(Posting {doc_ID: doc_ID, term_ID: term_ID, freq: freq});
     }

}
/// Parse .trec file, save page_table, term_to_term_ID mapping, term_ID_to_term mapping,
/// intermediate postings in several files(sorted before saving)
/// maybe bottleneck: offload_dict, consumes the dict[word, count] and saves in posting_vec
/// posting_vec are later dumped to file at an interval of processing NUM_LINES
fn parse() {
    let file = File::open("msmarco-docs.trec").unwrap();
    // let file = File::open("middle.trec").unwrap();
    let file = BufReader::new(file);
    // parser from scratch, because of ampersand character
    let mut next_url = false;
    let mut flow = 0;
    let mut cnt = 0;
    let mut doc_count = 0;
    let mut posting_vec: Vec<Posting> = Vec::new();
    let mut word_count: BTreeMap<String, u32> = BTreeMap::new();
    let mut page_table: BTreeMap<u32, String> = BTreeMap::new();
    let mut num_dumped_files = 1;
    let mut term_ID_to_term: BTreeMap<u32, String> = BTreeMap::new();
    let mut term_to_term_ID: BTreeMap<String, u32> = BTreeMap::new();
    let mut term_count = 0;
    for line in file.lines() {
        cnt += 1;
        if cnt % 100000 == 0 {
            println!("lineno: {}", cnt);
        }
        let s = line.unwrap();
        if s == "" { // blank line, if not continue here, will panic at later unwrap
            continue;
        }
        if next_url == true {
            //println!("url line");
            next_url = false;

            doc_count += 1;
            page_table.insert(doc_count, s);

            //assert_eq!(flow, 3);
            flow = 4;
            continue;
        }
        if s == "<DOC>" {
            //println!("encounter doc start");
            //assert_eq!(flow, 0);
            flow = 1;
        }
        else if s == "<TEXT>" {
            //println!("encounter text start");
            next_url = true;
            //assert_eq!(flow, 2);
            flow = 3;
        }
        else if s == "</DOC>" {
            //println!("encounter doc end");
            //assert_eq!(flow, 5);
            flow = 0;

            // dump dict for each docID
            offload_dict(doc_count, word_count, &mut posting_vec, &term_to_term_ID);
            word_count = BTreeMap::new();

            // 18750000
            if cnt > 18750000 * num_dumped_files { // roughly 1.21GB for default json serialization
                num_dumped_files += 1;
                // dump this file
                offload_tmp_file(&mut posting_vec);
                posting_vec = Vec::new(); // reinit posting vec
            }
        }
        else if s == "</TEXT>" {
            //assert_eq!(flow, 4);
            flow = 5;
            //println!("encounter text end");
        }
        // else if s.len() > 6 && s.chars().nth(0).unwrap() == '<' {//&& &s[0..7] == "<DOCNO>" {
        //     if s.chars().nth(1).unwrap() == 'D' &&
        //      s.chars().nth(2).unwrap() == 'O' &&
        //      s.chars().nth(3).unwrap() == 'C' &&
        //      s.chars().nth(4).unwrap() == 'N' &&
        //      s.chars().nth(5).unwrap() == 'O' &&
        //      s.chars().nth(6).unwrap() == '>' {
                 // test which impl is faster
        else if let Some(pos) = s.find("<DOCNO>") {
            if pos == 0 {
                //println!("other tag");
                //assert_eq!(flow, 1);
                flow = 2;
            }
        }
        else {
            // actual text
            // count each term for this doc
            let words = s.split_whitespace();
            for word in words {
                // posting_vec.push(Posting {doc_ID: doc_count, word: word.to_string()});
                word_count.entry(word.to_string()).and_modify(|freq| { *freq += 1 }).or_insert(1);
                if let None = term_to_term_ID.get(word) {
                    term_to_term_ID.insert(word.to_string(), term_count);
                    term_ID_to_term.insert(term_count, word.to_string());
                    term_count += 1;
                }
            }
        }
    }
    // dump remainingi posting vec
    if posting_vec.len() != 0 {
        offload_tmp_file(&mut posting_vec);
    }
    // dump page table;
    dumping_to_file(&term_ID_to_term, "page_table.tmp").unwrap();

    // dump term_ID_to_term mapping and term_to_term_ID mapping
    dumping_to_file(&term_ID_to_term, "term_ID_to_term.tmp").unwrap();

    dumping_to_file(&term_to_term_ID, "term_to_term_ID.tmp").unwrap();
}
fn read_n<R>(reader: R, bytes_to_read: u64) -> Option<Vec<u8>>
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
fn main() {
    // test forward
    // let mut posting_vec = vec![];
    // posting_vec.push(Posting{term_ID: 1, doc_ID:  2, freq:3});
    // posting_vec.push(Posting{term_ID: 1, doc_ID:  2, freq:3});
    // posting_vec.push(Posting{term_ID: 1, doc_ID:  2, freq:3});
    // posting_vec.push(Posting{term_ID: 1, doc_ID:  2, freq:3});
    // posting_vec.push(Posting{term_ID: 1, doc_ID:  2, freq:3});
    // posting_vec.push(Posting{term_ID: 1, doc_ID:  2, freq:3});
    // offload_vector_of_postings(&posting_vec, "1.tmp").unwrap();
    // let mut cache = CachedFile::new("1.tmp");
    // while let Some(v) = cache.forward(4) {
    //     println!("{:?}", v);
    // }
    // let mut buffer = vec![0u8;10];
    // let ret = r.read(&mut buffer);
    // while let Some(content) = read_n(&mut r, 10) {
    //     println!("{:?}", content);
    // }
    // while let Ok(n) = r.read(&mut buffer) {
    //     if n == 0 {
    //         println!("read 0 bytes");
    //         break;
    //     }
    //     println!("{:?}", n);
    //     println!("{:?}", buffer);
    //     buffer.clear();
    // }

    // let mut cache = CachedFile::new("merged_postings.tmp");
    // while let Some(a) = cache.forward(10) {
    //     println!("{:?}\n", a);
    // }
    // end testing
    parse();
    // let mut fvec = Vec::new();
    // for entry in glob("./*.intermediate").expect("Failed to glob pattern") {
    //     match entry {
    //         Ok(path) => {
    //             // println!("{:?}", path.display());
    //             if let Some(filename) = path.to_str() {
    //                 fvec.push(filename.to_string());
    //             }
    //         }
    //         Err(e) => println!("{:?}", e),
    //     }
    // }
    // k_way_merge(fvec);
    // build_inverted_index_and_lexicon();

}
