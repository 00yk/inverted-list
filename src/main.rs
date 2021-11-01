use glob::glob;
use inverted_list::{parse, k_way_merge, build_inverted_index_and_lexicon};

fn main() {

    parse("small.trec");
    let mut fvec = Vec::new();
    for entry in glob("./*.intermediate").expect("Failed to glob pattern") {
        match entry {
            Ok(path) => {
                // println!("{:?}", path.display());
                if let Some(filename) = path.to_str() {
                    fvec.push(filename.to_string());
                }
            }
            Err(e) => println!("{:?}", e),
        }
    }
    k_way_merge(fvec);
    build_inverted_index_and_lexicon();

}
