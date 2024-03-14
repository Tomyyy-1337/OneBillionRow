use std::{
    collections::HashMap, fs::File, path::Path,
};

use memmap2::Mmap;
use rayon::prelude::*;

#[derive(Debug, Copy, Clone)]
struct Station {
    min: f32,
    max: f32,
    sum: f32,
    count: u64,
}

impl Station {
    fn upate(&mut self, value: f32) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.count += 1;
    }
}

fn main() {
    let start = std::time::Instant::now();
    
    let path = "measurements.txt";

    let path = Path::new(&path);
    let file = File::open(path).expect("failed to open file");
    let mapped_data = unsafe { Mmap::map(&file) }.expect("failed to create memory map");
    let raw_data = &*mapped_data;
    let raw_data = raw_data.strip_suffix(b"\n").unwrap_or(raw_data);

    let mut data = raw_data
        .par_split(|&b| b == b'\n')
        .map(|row| {
            let mut iter = row.split(|&b| b == b';');
            let name = std::str::from_utf8(iter.next().unwrap()).unwrap();
            let value = std::str::from_utf8(iter.next().unwrap()).unwrap().parse::<f32>().unwrap();
            (name, value)
        })
        .fold( 
            || HashMap::new(),
            |mut station_map: HashMap<String, Station>, (name, value)| {
                match station_map.get_mut(name) {
                    Some(station) => {
                        station.upate(value);
                    }
                    None => {
                        station_map.insert(name.to_string(), Station {
                            min: value,
                            max: value,
                            sum: value,
                            count: 1,
                        });
                    }
                }
                station_map
            }
        )
        .reduce(
            || HashMap::new(),
            |mut map1: HashMap<String, Station>, map2: HashMap<String, Station>| {
                for (key, value) in map2 {
                    match map1.get_mut(&key) {
                        Some(station) => {
                            station.min = station.min.min(value.min);
                            station.max = station.max.max(value.max);
                            station.sum += value.sum;
                            station.count += value.count;
                        }
                        None => {
                            map1.insert(key, value);
                        }
                    }
                }
                map1
            }
        )
        .into_iter()
        .map(|(key, value)| {
            let avg = value.sum / value.count as f32;
            (key, value.min, avg, value.max)
        })
        .collect::<Vec<_>>();

    println!("Collection time: {:?}", start.elapsed());

    data.sort_unstable_by_key(|(name, _, _, _)| name.to_string());

    let mut output = data.par_iter().map(|(name, min, avg, max)| {
        format!("{}:{:.1}/{:.1}/{:.1};", name, min, max, avg)
    }).collect::<String>();
    output.pop();
    let output = format!("{{{}}}", output);   

    println!("{}", output);

    println!("Elapsed time: {:?}", start.elapsed());

}