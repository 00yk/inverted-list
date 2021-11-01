
use std::{fs::File, io::BufRead};
use std::io::{BufReader, Read, Write, BufWriter};
use serde::{Serialize, Deserialize};
/// helper function for ease of use
pub fn dumping_to_file<T: Serialize>(object: &T, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut f = File::create(filename)?;
    dumping(object, &mut f)
}

/// used in pair with deserialize_to_mem
/// dumping to in-mem data structure to file using default serialization method
#[cfg(feature = "binary-format")]
pub fn dumping<T: Serialize>(object: &T, f: &mut File) -> Result<(), Box<dyn std::error::Error>> {
    // let ser = bincode::serialize(object)?; // bincode
    let ser = rmp_serde::to_vec(object)?; // messagepack
    let mut encoder = lz4::EncoderBuilder::new().level(1).build(f)?;
    encoder.write_all(&ser)?;
    let (_, result) = encoder.finish();
    result?;
    Ok(())
}

#[cfg(not(feature = "binary-format"))]
pub fn dumping<T: Serialize>(object: &T, f: &mut File) -> Result<(), Box<dyn std::error::Error>> {
    let ser = serde_json::to_vec(object)?; // json
    f.write_all(&ser)?;
    Ok(())
}
#[cfg(feature = "binary-format")]
pub fn dumping_batch<T: Serialize>(object: &T, wr: &mut BufWriter<File>) -> Result<(), Box<dyn std::error::Error>> {
    // let ser = bincode::serialize(object)?; // bincode
    let ser = rmp_serde::to_vec(object)?; // messagepack
    // let ser = serde_json::to_vec(object)?; // json
    let mut encoder = lz4::EncoderBuilder::new().level(1).build(wr)?;
    encoder.write(&ser)?;
    let (_, result) = encoder.finish();
    result?;
    Ok(())


}

#[cfg(not(feature = "binary-format"))]
pub fn dumping_batch<T: Serialize>(object: &T, wr: &mut BufWriter<File>) -> Result<(), Box<dyn std::error::Error>> {
    let ser = serde_json::to_vec(object)?; // json
    wr.write(&ser)?;
    Ok(())
}
/// used in pair with dumping_to_file
#[cfg(feature = "binary-format")]
pub fn deserialize_to_mem<T: serde::de::DeserializeOwned>(filename: &str) -> Result<T, Box<dyn std::error::Error>> {
    let f = File::open(filename)?;
    let reader = BufReader::new(f);
    let mut lz4_reader = lz4::Decoder::new(reader)?;
    Ok(rmp_serde::from_read(lz4_reader)?) // messagepack
    // Ok(bincode::deserialize_from(reader)?) // bincode
}

#[cfg(not(feature = "binary-format"))]
pub fn deserialize_to_mem<T: serde::de::DeserializeOwned>(filename: &str) -> Result<T, Box<dyn std::error::Error>> {
    let f = File::open(filename)?;
    let reader = BufReader::new(f);
    Ok(serde_json::from_reader(reader)?) // json
}
