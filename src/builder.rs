use std::convert::TryInto;
use std::intrinsics::transmute;
use std::{fs::File, io::BufRead};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use crate::serde_utils::*;
use crate::posting::*;
use crate::vbyte::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct LexiconValue {
    pub pos: u32,
    pub len: u32,
    pub offset: u64
}
/// build inverted index and lexicon together
/// need to read in the term_ID_to_term mapping, and merged_postings file
/// maybe bottleneck: dumping cur_inverted_list should perhaps use a BufWriter to
/// perform batched writing.
pub fn build_inverted_index_and_lexicon(){
    // read in term_to_term_ID mapping
    let term_ID_to_term: BTreeMap<u32, String> = deserialize_to_mem("term_ID_to_term.tmp").unwrap();

    let mut cachedfile = CachedFile::new("merged_postings.tmp");
    let mut lexicon: BTreeMap<String, LexiconValue> = BTreeMap::new(); // term to start index in inverted index
    let mut cur_inverted_list: Vec<(u32, u32)> = Vec::new();
    let mut num_inverted_list = 0;
    let f = File::create("inverted_index.tmp").unwrap();
    let mut wr = BufWriter::new(f);
    let mut cur_offset = 0;
    let mut first = true;
    let mut prev_term_ID = 0;
    while let Some(postings) = cachedfile.forward(10) {
        for p in postings {
            let word = term_ID_to_term.get(&p.term_ID).unwrap();
            if let Some(v) = lexicon.get_mut(word) {
                // if already in the middle of building a inverted list for p.word
                // then keep pushing current posting doc_ID to the inverted list
                cur_inverted_list.push((p.doc_ID, p.freq));
                // update total inverted list len in lexicon for this term
                v.len += 1;
            }
            else {
                // dumping_batch(&cur_inverted_list, &mut wr).unwrap();

                if !first {
                    let bytes_representation = convert_inverted_list_to_bytes(prev_term_ID, cur_inverted_list);
                    let offset = bytes_representation.len();
                    cur_offset += offset as u64;
                    wr.write(&bytes_representation).unwrap();
                }
                first = false;
                // if this a new inverted list
                // insert this term into lexicon first with
                lexicon.insert(word.clone(), LexiconValue { pos: num_inverted_list, len: 1, offset: cur_offset});
                // update total number of inverted list
                num_inverted_list += 1;
                // create a inverted list
                cur_inverted_list = Vec::new();
                // push the first posting onto the inverted list
                cur_inverted_list.push((p.doc_ID, p.freq));
                prev_term_ID = p.term_ID;
            }
        }
    }
    dumping_to_file(&lexicon, "lexicon.tmp").unwrap();
}
#[cfg(not(feature = "vbyte-compression"))]
pub fn convert_inverted_list_to_bytes(term_ID: u32, inverted_list: Vec<(u32, u32)>) -> Vec<u8> {
    // inverted list consists of (doc_ID, frequency) pairs
    let mut bytes_representation = vec![];

    let mut metadata: Vec<u8> = vec![];

    let term_ID_bytes = term_ID.to_ne_bytes();
    let length_bytes = inverted_list.len().to_ne_bytes();
    metadata.extend(term_ID_bytes);
    metadata.extend(length_bytes);
    bytes_representation.extend(metadata);
    for ele in inverted_list {
        let e1 = ele.0.to_ne_bytes();
        let e2 = ele.1.to_ne_bytes();
        bytes_representation.extend(e1);
        bytes_representation.extend(e2);

    }

    bytes_representation
}
// #[cfg(not(feature = "vbyte-compression"))]
// pub fn convert_bytes_to_inverted_list(bytes_: Vec<u8>)
//                                       -> (u32, Vec<(u32, u32)>) {
//     let term_ID_bytes: [u8; 4] = (&bytes_[0..4]).try_into().unwrap();
//     let term_ID = u32::from_ne_bytes(term_ID_bytes);

//     let length = usize::from_ne_bytes(
//         (&bytes_[4..12]).try_into().unwrap()
//     );
//     let mut inverted_list = vec![];
//     for i in 0..length {
//         let start_pos = 12 + 8 * i;
//         let doc_ID = u32::from_ne_bytes(
//             (&bytes_[start_pos..start_pos + 4]).try_into().unwrap()
//         );
//         let frequency = u32::from_ne_bytes(
//             (&bytes_[start_pos + 4..start_pos + 8]).try_into().unwrap()
//         );
//         inverted_list.push(
//             (doc_ID, frequency)
//         );
//     }


//     (term_ID, inverted_list)
// }

#[cfg(not(feature = "vbyte-compression"))]
pub fn read_inverted_list_from_offset(r: &mut File, offset: u64) -> (u32, Vec<(u32, u32)>) {
    r.seek(SeekFrom::Start(offset)).unwrap();
    let mut metadata = vec![0u8; 12];
    r.read_exact(&mut metadata).expect("read exact 12 bytes metadata failed! Make sure index file matches the lexicon and page_table.");
    let term_ID_bytes = (&metadata[0..4]).try_into().unwrap();
    let term_ID = u32::from_ne_bytes(term_ID_bytes);
    let length = usize::from_ne_bytes(
        (&metadata[4..12]).try_into().unwrap()
    );
    let mut inverted_list = vec![];
    // let li = read_n(r, length as u64 * 8).unwrap();
    let mut li = vec![0u8; length * 8];
    r.read_exact(&mut li).expect("read exact length as u64 * 8 bytes li failed!");

    for i in 0..length {
        let start_pos = i * 8;
        let doc_ID = u32::from_ne_bytes((&li[start_pos..start_pos + 4]).try_into().unwrap());
        let freq = u32::from_ne_bytes((&li[start_pos + 4..start_pos + 8]).try_into().unwrap());
        inverted_list.push((doc_ID, freq));
    }


    (term_ID, inverted_list)
}
///////////////////////////

#[cfg(feature = "vbyte-compression")]
pub fn convert_inverted_list_to_bytes(term_ID: u32, mut inverted_list: Vec<(u32, u32)>) -> Vec<u8> {
    // inverted list consists of (doc_ID, frequency) pairs
    let mut bytes_representation = vec![];

    let mut metadata: Vec<u8> = vec![];

    let term_ID_bytes = term_ID.to_ne_bytes();
    metadata.extend(term_ID_bytes);
                        // encode all inverted_list (doc_ID, freq) in normal 4 bytes for u32 scheme
                        // for ele in inverted_list {
                        //     let e1 = ele.0.to_ne_bytes();
                        //     let e2 = ele.1.to_ne_bytes();
                        //     bytes_representation.extend(e1);
                        //     bytes_representation.extend(e2);

                        // }
                        // end
    // use vbyte compression
    // unsafe transmute Vec<(u32, u32)> to Vec<u32>
    let input: Vec<u32> = unsafe {
        inverted_list.set_len(inverted_list.len() * 2);
        std::mem::transmute(inverted_list)
    };
    let bytes = vbyteEncode(input);
    // length can only be known after vbyteEncode
    let vbyte_content_length_bytes = bytes.len().to_ne_bytes();
    metadata.extend(vbyte_content_length_bytes);
    bytes_representation.extend(metadata);
    bytes_representation.extend(bytes);
    // end

    bytes_representation
}

#[cfg(feature = "vbyte-compression")]
pub fn read_inverted_list_from_offset(r: &mut File, offset: u64) -> (u32, Vec<(u32, u32)>) {
    r.seek(SeekFrom::Start(offset)).unwrap();
    let mut metadata = vec![0u8; 12];
    r.read_exact(&mut metadata).expect("read exact 12 bytes metadata failed! Make sure index file matches the lexicon and page_table.");
    let term_ID_bytes = (&metadata[0..4]).try_into().unwrap();
    let term_ID = u32::from_ne_bytes(term_ID_bytes);
    let vbyte_content_length = usize::from_ne_bytes(
        (&metadata[4..12]).try_into().unwrap()
    );
    // let mut inverted_list = vec![];
    // let li = read_n(r, length as u64 * 8).unwrap();
    let mut li = vec![0u8; vbyte_content_length]; // FIXME: cannot be calculated through length * 8, because we use vbyte compression now!
    r.read_exact(&mut li).expect("read exact length as u64 * 8 bytes li failed!");

    // encode all inverted_list (doc_ID, freq) in normal 4 bytes for u32 scheme
    // for i in 0..length {
    //     let start_pos = i * 8;
    //     let doc_ID = u32::from_ne_bytes((&li[start_pos..start_pos + 4]).try_into().unwrap());
    //     let freq = u32::from_ne_bytes((&li[start_pos + 4..start_pos + 8]).try_into().unwrap());
    //     inverted_list.push((doc_ID, freq));
    // }
    // end
    //
    // use vbyte compression

    let mut raw_inverted_list = vbyteDecode(li);


    // unsafe transmute Vec<u32> to Vec<(u32, u32)>
    let inverted_list: Vec<(u32, u32)> = unsafe {
        raw_inverted_list.set_len(raw_inverted_list.len() / 2);
        std::mem::transmute(raw_inverted_list)
    };


    // end

    (term_ID, inverted_list)
}
