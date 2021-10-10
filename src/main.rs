use std::{fs::File, io::BufRead};
use std::io::{BufReader, Write};
use std::mem::size_of_val;
use std::collections::BTreeMap;

use quick_xml::Reader;

use serde::{Serialize, Deserialize};

fn indent(size: usize) -> String {
    const INDENT: &'static str = "    ";
    (0..size).map(|_| INDENT).fold(String::with_capacity(size*INDENT.len()), |r, s| r + s)
}
#[derive(Serialize, Deserialize, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Posting {
    word: String,
    doc_ID: u32,
    freq: u32  // use percentage or num count?
}
fn dump_to_file(posting_vec: &mut Vec<Posting>) {
    // println!("{}", size_of_val(&*posting_vec));
    // in-mem sort posting vec
    posting_vec.sort();
    let mut now: String = chrono::offset::Utc::now().to_string();
    now.push_str(".tmp");
    let serialized = serde_json::to_vec_pretty(posting_vec).unwrap();
    // let serialized = bincode::serialize(posting_vec).unwrap();
    let mut f = File::create(now).expect("Unable to create file");
    f.write_all(&serialized).unwrap();
}
#[derive(Serialize, Deserialize)]
struct LexiconValue {
    pos: u32,
    len: u32,
}
fn merge_sort_postings() {

}

fn build_inverted_index_and_lexicon(){
    let f = File::open("merged_postings.tmp").unwrap();
    let reader = BufReader::new(f);
    // this posting will be consumed in the for loop
    let postings: Vec<Posting> = serde_json::from_reader(reader).unwrap(); // assume can be read entirely to DRAM
    let mut lexicon: BTreeMap<String, LexiconValue> = BTreeMap::new(); // term to start index in inverted index
    let mut cur_inverted_list: Vec<u32> = Vec::new();
    let mut num_inverted_list = 0;
    let mut f = File::create("inverted_list.tmp").unwrap();
    for p in postings {
        if let Some(value) = lexicon.get(&p.word) {
            // if already in the middle of building a inverted list for p.word
            // then keep pushing current posting doc_ID to the inverted list
            cur_inverted_list.push(p.doc_ID);
            // update total inverted list len in lexicon for this term
            lexicon.entry(p.word).and_modify(|e| { e.len += 1 });
        }
        else {
            println!("Dumping {}: {:?}", p.word, cur_inverted_list);
            // dump_inverted_list(&cur_inverted_list);
            let serialized = serde_json::to_vec(&cur_inverted_list).unwrap();
            f.write_all(&serialized).unwrap();
            // if this a new inverted list
            // insert this term into lexicon first with
            lexicon.insert(p.word, LexiconValue { pos: num_inverted_list, len: 0 });
            // update total number of inverted list
            num_inverted_list += 1;
            // create a inverted list
            cur_inverted_list = Vec::new();
            // push the first posting onto the inverted list
            cur_inverted_list.push(p.doc_ID);
        }
    }
    let mut f = File::create("lexicon.tmp").unwrap();
    let ser = serde_json::to_vec(&lexicon).unwrap();
    f.write_all(&ser).unwrap();
}
fn dump_dict(doc_ID: u32, dict: BTreeMap<String, u32>, vec: &mut Vec<Posting>) {
     // dumping [word]freq to Vec<Posting>
     for (word, freq) in dict {
         vec.push(Posting {doc_ID: doc_ID, word: word, freq: freq});
     }

}
fn parse() {
    // let file = File::open("msmarco-docs.trec").unwrap();
    let file = File::open("small.trec").unwrap();
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

            // dump dict for each docID
            dump_dict(doc_count, word_count, &mut posting_vec);
            word_count = BTreeMap::new();

            doc_count += 1;
            page_table.insert(doc_count, s);

            //assert_eq!(flow, 3);
            flow = 4;
            if cnt > 3000000 * num_dumped_files { // roughly 1.21GB for default json serialization
                num_dumped_files += 1;
                // dump this file
                dump_to_file(&mut posting_vec);
                posting_vec = Vec::new(); // reinit posting vec
            }
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
            // count each term for this doc
            let words = s.split_whitespace();
            for word in words {
                // posting_vec.push(Posting {doc_ID: doc_count, word: word.to_string()});
                word_count.entry(word.to_string()).and_modify(|freq| { *freq += 1 }).or_insert(1);
            }
        }
    }
    dump_to_file(&mut posting_vec);
    // dump page table;
    let serialized = serde_json::to_vec_pretty(&page_table).unwrap();
    let mut f = File::create("page_table.tmp").expect("Unable to create file");
    f.write_all(&serialized).unwrap();
}
fn main() {
    parse();
    merge_sort_postings();
    build_inverted_index_and_lexicon();
}
