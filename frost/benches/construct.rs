#![allow(dead_code)]
#![cfg(nightly)]
#![feature(test)]

#[cfg(all(nightly, test))]
extern crate test;

use std::hint;

const BAG_PATH: &'static str = "./tests/fixtures/compressed_lz4.bag"; // bench runs in `frost`
const COMPRESSED_LZ4: &[u8] = include_bytes!("../tests/fixtures/compressed_lz4.bag");

#[cfg(all(nightly, test))]
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use frost::{query::Query, Bag};
    use test::Bencher;

    #[bench]
    fn bench_from_bytes(b: &mut Bencher) {
        b.iter(|| {
            for _i in 0..1000 {
                let mut _bag = hint::black_box(Bag::from_bytes(COMPRESSED_LZ4).unwrap());
            }
        });
    }

    #[bench]
    fn bench_from_file(b: &mut Bencher) {
        b.iter(|| {
            for _i in 0..1000 {
                let mut _bag = hint::black_box(Bag::from(BAG_PATH).unwrap());
            }
        });
    }

    #[bench]
    fn bench_iterate_from_bytes(b: &mut Bencher) {
        let mut bag = Bag::from_bytes(COMPRESSED_LZ4).unwrap();
        let query = Query::all();

        b.iter(|| {
            for _i in 0..1000 {
                for msg_view in bag.read_messages(&query).unwrap() {
                    let _topic = hint::black_box(msg_view.topic);
                }
            }
        });
    }

    #[bench]
    fn bench_iterate_from_file(b: &mut Bencher) {
        let mut bag = Bag::from(BAG_PATH).unwrap();
        let query = Query::all();

        b.iter(|| {
            for _i in 0..1000 {
                for msg_view in bag.read_messages(&query).unwrap() {
                    let _topic = hint::black_box(msg_view.topic);
                }
            }
        });
    }
}
