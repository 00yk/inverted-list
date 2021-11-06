use super::*;
use std::fs::File;
use std::io::Write;
use std::io::BufReader;

#[cfg(test)]
#[test]
fn my_test() {
    use std::io::{BufWriter, Read};

    let term_ID = 42;

    let inverted_list = vec![(1, 2); 5];

    let bytes_ = convert_inverted_list_to_bytes(term_ID, inverted_list);
    let f = File::create("wtf.tmp").unwrap();
    let mut wr = BufWriter::new(f);
    wr.write(&bytes_);
    drop(wr);
    let f = File::open("wtf.tmp").unwrap();
    let mut r = BufReader::new(f);
    let mut buf = vec![0u8; 52];
    r.read_exact(&mut buf).expect("read_exact 4 bytes error!");
}
#[cfg(test)]
#[test]
fn read_inverted_index() {
    use std::{convert::TryInto};

    let f = File::open("inverted_index.tmp").unwrap();
    let mut r = BufReader::new(f);

    let metadata = read_n(&mut r, 28).expect("read 4 bytes for term_ID");
    let term_ID = u32::from_ne_bytes((&metadata[0..4]).try_into().unwrap());
    println!("{:?}", term_ID);
    let length = usize::from_ne_bytes((&metadata[4..12]).try_into().unwrap());
    println!("{:?}", length);
    let term_ID = u32::from_ne_bytes((&metadata[12..16]).try_into().unwrap());
    println!("{:?}", term_ID);
    let length = usize::from_ne_bytes((&metadata[16..24]).try_into().unwrap());
    println!("{:?}", length);

}

#[cfg(test)]
#[test]
fn read_specific_inverted_list() {
    use std::collections::BTreeMap;
    let mut f = File::open("inverted_index.tmp").unwrap();
    let lexicon: BTreeMap<String, LexiconValue> = deserialize_to_mem("lexicon.tmp").unwrap();
    // for (k, v) in &lexicon {
    //     if v.pos == 0 {
    //         println!("{:?} {:?}", k, v);
    //     }
    // }
    let v = lexicon.get("as").unwrap();
    println!("lexicon_value {:?}", v);
    let (term_ID, inverted_list) = read_inverted_list_from_offset(&mut f, v.offset);
    println!("term_ID {:?}", &term_ID);
    println!("inverted_list {:?}", inverted_list);
}
