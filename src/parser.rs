use std::{fs::File, io::BufRead};
use std::io::{BufReader, Read, Write, BufWriter};
use std::collections::BTreeMap;
use crate::serde_utils::*;
use crate::posting::*;
use mongodb::IndexModel;
use mongodb::options::IndexOptions;
use mongodb::{
    bson::doc,
    sync::Client,
};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Page {
    // #[serde(rename = "_id")]
    pub docID: u32,
    pub URL: String,
    pub content: Vec<String>
}

pub fn offload_dict(doc_ID: u32, dict: BTreeMap<String, u32>, vec: &mut Vec<Posting>, term_to_term_ID: &BTreeMap<String, u32>) {
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
pub fn parse(filename: &str) {
    let file = File::open(filename).unwrap();
    let file = BufReader::new(file);
    // parser from scratch, because of ampersand character
    let mut next_url = false;
    let mut flow = 0;
    let mut cnt = 0;
    let mut doc_count = 0;
    let mut posting_vec: Vec<Posting> = Vec::new();
    let mut word_count: BTreeMap<String, u32> = BTreeMap::new();
    let mut page_table: BTreeMap<u32, (String, u32)> = BTreeMap::new();
    let mut num_dumped_files = 1;
    let mut term_ID_to_term: BTreeMap<u32, String> = BTreeMap::new();
    let mut term_to_term_ID: BTreeMap<String, u32> = BTreeMap::new();
    let mut term_count = 0;
    const save_per_lines: u32 = 18750000;
    let mut total_word_num: u64 = 0;
    let mut content = vec![];

    // mongodb connection
    let client = Client::with_uri_str("mongodb://localhost:27017").expect("MongoDB connection failed");
    let database = client.database("wse");
    let collection = database.collection::<Page>("pages");
    let new_index = IndexModel::builder()
        .keys(doc! { "docID": 1 })
        .options(IndexOptions::builder().unique(true).build())
        .build();
    collection.create_index(new_index, None).expect("create MongoDB index failed");

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
            page_table.insert(doc_count, (s, 0));

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
            let doc_length: u32 = word_count.values().sum();
            let (s, docl) = page_table.get_mut(&doc_count).unwrap();
            *docl = doc_length; // modify doc_length at the end of processing each doc

            total_word_num += doc_length as u64;

            // println!("doc_count: {:?}", doc_count);
            offload_dict(doc_count, word_count, &mut posting_vec, &term_to_term_ID);

            word_count = BTreeMap::new();

            let page = Page {
                docID: doc_count,
                URL: page_table.get(&doc_count).unwrap().0.clone(),
                content: content
            };
            collection.insert_one(page, None).expect("MongoDB insertion failed");
            content = vec![];

            // 18750000
            if cnt > save_per_lines * num_dumped_files { // roughly 1.21GB for default json serialization
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
            // track all document content in a Vec<String>
            content.push(s.clone());
            let words = s.split_whitespace();
            for word in words {
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
    dumping_to_file(&page_table, "page_table.tmp").unwrap();

    // dump term_ID_to_term mapping and term_to_term_ID mapping
    dumping_to_file(&term_ID_to_term, "term_ID_to_term.tmp").unwrap();

    dumping_to_file(&term_to_term_ID, "term_to_term_ID.tmp").unwrap();
    let mut f = File::create("hyperparameter.txt").unwrap();
    let average_doc_length: f32 = total_word_num as f32 / doc_count as f32;
    println!("total words number: {}", total_word_num);

    println!("total doc number: {}", doc_count);
    println!("average doc length: {}", average_doc_length);
    writeln!(f, "total doc number: {}", doc_count).unwrap();
    writeln!(f, "average doc length: {}", average_doc_length).unwrap();
}
