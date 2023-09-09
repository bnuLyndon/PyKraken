use pyo3::prelude::*;
use dashmap::DashMap;
use bincode::{DefaultOptions, Options};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::io::{BufReader, Read};
use std::io::ErrorKind;
use rayon::prelude::*;

macro_rules! define_dashmap {
    ($name:ident, $key:ty, $value:ty) => {
        #[pyclass]
        struct $name {
            map: DashMap<$key, $value> 
        }

        #[pymethods]
        impl $name {

            #[new]
            fn new() -> Self {
                Self { 
                    map: DashMap::new()
                }
            }

            fn insert(&mut self, key: $key, value: $value) {
                self.map.insert(key, value);
            }

            fn query(&self, key: $key) -> Option<$value> {
                self.map.get(&key).map(|v| *v)
            }

            fn batch_insert(&mut self, keys: Vec<$key>, values: Vec<$value>) {
                self.map.par_extend(keys.into_par_iter().zip(values.into_par_iter()));

            }
            
            fn batch_query(&self, keys: Vec<$key>) -> Vec<Option<$value>> {
                keys.into_par_iter()
                    .map(|key| self.query(key))
                    .collect()
            }

            fn save(&self, path: String) -> PyResult<()> {
                let file = File::create(path)?;
                let mut writer = BufWriter::new(file);
                let config = DefaultOptions::new();

                for r in self.map.iter() {
                    let key = r.key();
                    let value = r.value();
                    let bytes = config.serialize(&(key, value)).unwrap();
                    writer.write(&bytes.len().to_le_bytes())?;
                    writer.write(&bytes)?;
                }

                writer.flush()?;
                Ok(())
            }

            // A function to load a MyDashMap from a file
            fn load(&mut self, path: String) -> PyResult<()> {
                let file = File::open(path)?;
                let mut reader = BufReader::new(file);
                let config = DefaultOptions::new();

                // Loop until the end of the file
                loop {
                    // Read the length of the next key-value pair
                    let mut len_bytes = [0u8; 8];
                    match reader.read_exact(&mut len_bytes) {
                        Ok(_) => {
                            // Convert the bytes to an integer
                            let len = u64::from_le_bytes(len_bytes) as usize;

                            // Read the key-value pair bytes
                            let mut bytes = vec![0u8; len];
                            reader.read_exact(&mut bytes)?;

                            // Deserialize the key-value pair
                            let (key, value): ($key, $value) = config.deserialize(&bytes).unwrap();

                            // Insert the key-value pair into the map
                            self.map.insert(key, value);
                        }
                        Err(e) => {
                            // If the error is EOF, break the loop
                            if e.kind() == ErrorKind::UnexpectedEof {
                                break;
                            } else {
                                // Otherwise, return the error
                                return Err(e.into());
                            }
                        }
                    }
                }

                Ok(())
            }

        }

    };
}


define_dashmap!(MyDashMapSI_32, String, i32);
define_dashmap!(MyDashMapI64_32, i64, i32);

#[pymodule]
fn rust_dash_map(_py: Python, m: &PyModule) -> PyResult<()> {
    
    m.add_class::<MyDashMapSI_32>()?;
    m.add_class::<MyDashMapI64_32>()?;
    Ok(())
}
