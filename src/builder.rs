use std::{fs::File, io::BufRead};
use std::io::{BufReader, Read, Write, BufWriter};
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use crate::serde_utils::*;
use crate::posting::*;

#[derive(Serialize, Deserialize)]
pub struct LexiconValue {
    pub pos: u32,
    pub len: u32,
}
/// build inverted index and lexicon together
/// need to read in the term_ID_to_term mapping, and merged_postings file
/// maybe bottleneck: dumping cur_inverted_list should perhaps use a BufWriter to
/// perform batched writing.
pub fn build_inverted_index_and_lexicon(){
    // read in term_to_term_ID mapping
    let term_ID_to_term: BTreeMap<u32, String> = deserialize_to_mem("term_ID_to_term.tmp").unwrap();

    // let postings: Vec<Posting> = deserialize_to_mem("merged_postings.tmp").unwrap();
    let mut cachedfile = CachedFile::new("merged_postings.tmp");
    // let postings: VecDeque<Posting> = cachedfile.forward(10).unwrap();
    // let postings: Vec<Posting> = read_vector_of_postings("merged_postings.tmp");
    let mut lexicon: BTreeMap<String, LexiconValue> = BTreeMap::new(); // term to start index in inverted index
    let mut cur_inverted_list: Vec<(u32, u32)> = Vec::new();
    let mut num_inverted_list = 0;
    let f = File::create("inverted_index.tmp").unwrap();
    let mut wr = BufWriter::new(f);
    while let Some(postings) = cachedfile.forward(10) {
        for p in postings {
            let word = term_ID_to_term.get(&p.term_ID).unwrap();
            if let Some(_) = lexicon.get(word) {
                // if already in the middle of building a inverted list for p.word
                // then keep pushing current posting doc_ID to the inverted list
                cur_inverted_list.push((p.doc_ID, p.freq));
                // update total inverted list len in lexicon for this term
                lexicon.entry(word.clone()).and_modify(|e| { e.len += 1 });
            }
            else {
                // println!("Dumping {}: {:?}", word, cur_inverted_list);
                // dumping(&cur_inverted_list, &mut f).unwrap();
                dumping_batch(&cur_inverted_list, &mut wr).unwrap();
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
    }
    dumping_to_file(&lexicon, "lexicon.tmp").unwrap();
}
