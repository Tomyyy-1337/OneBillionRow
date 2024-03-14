use std::{
    fs::File, 
    path::Path,
};
use hashbrown::HashMap;
use memmap2::Mmap;
use rayon::prelude::*;

#[derive(Debug, Copy, Clone)]
struct Station {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

impl Station {
    fn new(value: f64) -> Self {
        Self {
            min: value,
            max: value,
            sum: value,
            count: 1,
        }
    }

    fn upate(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.count += 1;
    }

    fn combine(&mut self, other: Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }
}

fn main() {
    let start = std::time::Instant::now();

    let path = Path::new("measurements.txt");
    let file = File::open(path).expect("failed to open file");
    let mapped_data = unsafe { Mmap::map(&file) }.unwrap();
    let raw_data = &*mapped_data;
    let raw_data = raw_data.strip_suffix(b"\n").unwrap_or(raw_data);

    let mut data = raw_data
        .par_split(|&b| b == b'\n')
        .map(|row| {
            let mut iter = row.split(|&b| b == b';');
            let name = std::str::from_utf8(iter.next().unwrap()).unwrap();
            let value = std::str::from_utf8(iter.next().unwrap()).unwrap().parse::<f64>().unwrap();
            (name, value)
        })
        .fold( 
            || HashMap::new(),
            |mut station_map: HashMap<&str, Station>, (name, value)| {
                match station_map.get_mut(name) {
                    Some(station) => {
                        station.upate(value);
                    }
                    None => {
                        station_map.insert_unique_unchecked(name, Station::new(value));
                    }
                }
                station_map
            }
        )
        .reduce(
            || HashMap::new(),
            |mut map1: HashMap<&str, Station>, map2: HashMap<&str, Station>| {
                map2.into_iter().for_each(|(key, other)| 
                match map1.get_mut(&key) {
                    Some(station) => {
                        station.combine(other);
                    },
                    None => {
                        map1.insert_unique_unchecked(key, other);
                    }
                });
                map1
            }
        )
        .into_iter()
        .map(|(key, value)| {
            let avg = value.sum / value.count as f64;
            (key, value.min, avg, value.max)
        })
        .collect::<Vec<_>>();

    data.sort_unstable_by_key(|(name, _, _, _)| name.to_string());

    let mut output = data.iter().map(|(name, min, avg, max)| {
        format!("{}:{:.1}/{:.1}/{:.1};", name, min, avg, max)
    }).collect::<String>();
    output.pop();
    let output = format!("{{{}}}", output);   


    println!("{}", output);
    println!("Elapsed time: {:?}", start.elapsed());
}