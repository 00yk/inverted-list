use std::collections::VecDeque;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use crate::posting::*;

// later change compare function to only consider posting
#[derive(Ord, Eq, PartialOrd, PartialEq, Debug)]
pub struct HeapNode {
    pub p: Posting,
    pub index: u8 // which queue it belongs to, k-way merge no more that 255-way
}

/// external k way merge
pub fn k_way_merge(files: Vec<String>) {
    // 1-way-merge
    if files.len() == 0 {
        panic!("to-be merged files not found.");
    }
    if files.len() == 1 {
        std::fs::copy(files[0].clone(), "merged_postings.tmp").unwrap();
        return;
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
                }
                else {
                    // cachedfile is over, then leave empty queue where it is
                }
            }
        }
    }
    offload_vector_of_postings(&output_buf, "merged_postings.tmp").unwrap();

}
