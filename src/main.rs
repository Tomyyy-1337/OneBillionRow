use std::{
    fs::File, 
    path::Path
};
use hashbrown::HashMap;
use memmap2::Mmap;
use rayon::prelude::*;

struct Station {
    min: i16,
    max: i16,
    sum: i32,
    count: u32,
}

impl Station {
    fn new(value: i16) -> Self {
        Self {
            min: value,
            max: value,
            sum: value as i32,
            count: 1,
        }
    }

    fn upate(&mut self, value: i16) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value as i32;
        self.count += 1;
    }

    fn combine(&mut self, other: Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn to_string(&self) -> String {
        format!("{:.1}/{:.1}/{:.1}", self.min as f64 / 10.0, self.sum as f64 / self.count as f64 / 10.0, self.max as f64 / 10.0)
    }
}

fn fast_parse(input: &[u8]) -> i16 {
    if input[0] == b'-' {
        return -fast_parse(&input[1..]);
    }
    let decimal_point_indx = input.iter().position(|&b| b == b'.').unwrap();
    let result_integer = input[..decimal_point_indx].into_iter().fold(0, |acc, &b| acc * 10 + (b - b'0')) as i16 * 10;
    let result_decimal  = (input[decimal_point_indx+1] - b'0') as i16;  
    result_integer + result_decimal
}

fn main() {
    let start = std::time::Instant::now();

    let path = Path::new("measurements.txt");
    let file = File::open(path).expect("failed to open file");
    let data = unsafe { Mmap::map(&file) }.unwrap();
    let data = &*data;
    let data = data.strip_suffix(b"\n").unwrap_or(data);

    let mut chunk_count: usize = std::thread::available_parallelism().unwrap().into();
    chunk_count *= 20;
    let chunk_size = data.len() / chunk_count;

    let mut data: Vec<(&[u8], Station)> = (0..chunk_count)
        .scan(0, |start_indx: &mut usize, _| {
            let end = (*start_indx + chunk_size).min(data.len());
            let end = end + &data[end..].iter().position(|c| *c == b'\n').unwrap_or(0);
            let chunk = (*start_indx, end);
            *start_indx = end + 1;
            Some(chunk)
        })
        .par_bridge()
        .map(|(start , end)| data[start..end]
            .split(|b| *b == b'\n')
            .map(|row| {
                let split_pos = row.iter().position(|b| *b == b';').unwrap_or(0);
                let name = &row[..split_pos];
                let value = fast_parse(&row[split_pos+1..]);
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
        )      
        .reduce(
            || HashMap::new(),
            |map1: HashMap<&[u8], Station>, mut map2: HashMap<&[u8], Station>| {
                for (key, other) in map1.into_iter() {
                    match map2.get_mut(&key) {
                        Some(station) => {
                            station.combine(other);
                        },
                        None => {
                            map2.insert_unique_unchecked(key, other);
                        }
                    }
                }
                map2
            }
        )
        .into_iter()
        .collect();

    data.sort_unstable_by_key(|(name,_)| *name);

    let mut output = data.into_iter().map(|(name, station)| {
        format!("{}={};", std::str::from_utf8(name).unwrap(), station.to_string())
    }).collect::<String>();
    output.pop();
    let output = format!("{{{}}}", output);   

    println!("{}", output);
    println!("Elapsed time: {:?}", start.elapsed());
}