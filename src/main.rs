use std::{
    fs::File, path::Path
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
    let data = unsafe { Mmap::map(&file) }.unwrap();
    let data = &*data;
    let data = data.strip_suffix(b"\n").unwrap_or(data);

    let mut chunk_count: usize = std::thread::available_parallelism().unwrap().into();
    chunk_count *= 12;
    let chunk_size = data.len() / chunk_count;

    let mut data = (0..chunk_count)
        .scan(0, |start_indx, _| {
            let end = (*start_indx + chunk_size).min(data.len());
            let next_new_line = match memchr::memchr(b'\n', &data[end..]) {
                Some(v) => v,
                None => 0,
            };
            let end = end + next_new_line;
            let chunk = (*start_indx, end);
            *start_indx = end + 1;
            Some(chunk)
        })
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|(start , end)| { data[start..end]
            .split(|b| b == &b'\n')
            .map(|row| {
                let mut iter = row.split(|b| *b == b';');
                let name = iter.next().unwrap();
                let value = std::str::from_utf8(iter.next().unwrap()).unwrap().parse::<f64>().unwrap();
                (name, value)
            })
            .fold(
                HashMap::new(),
                |mut station_map: HashMap<&[u8], Station>, (name, value)| 
                {
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
        })      
        .reduce(
            || HashMap::new(),
            |mut map1: HashMap<&[u8], Station>, map2: HashMap<&[u8], Station>| {
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

    data.sort_unstable_by_key(|(name, _, _, _)| *name);

    let mut output = data.into_iter().map(|(name, min, avg, max)| {
        format!("{}={:.1}/{:.1}/{:.1};", std::str::from_utf8(name).unwrap(), min, avg, max)
    }).collect::<String>();
    output.pop();
    let output = format!("{{{}}}", output);   

    println!("{}", output);
    println!("Elapsed time: {:?}", start.elapsed());
}