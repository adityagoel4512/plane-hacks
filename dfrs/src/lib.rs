use pyo3::prelude::*;
use pyo3::{exceptions::PyKeyError, exceptions::PyValueError, types::PyDict, types::PyString};
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::{collections::HashMap, ops::Add};

trait ConcreteArrayTrait: std::fmt::Debug + Add + Sized {
    fn len(&self) -> usize;
}

// We cannot have a generic pyclass for obvious reasons so we template out the variants explicitly.
macro_rules! create_concrete_array {
    ($name: tt, $type: ty) => {
        #[pyclass]
        #[derive(Clone, Debug)]
        struct $name {
            items: Vec<$type>,
        }
    };
}

create_concrete_array!(ConcreteString, String);
create_concrete_array!(ConcreteFloat, f64);
create_concrete_array!(ConcreteInt, i64);

impl Add for ConcreteString {
    type Output = Self;

    fn add(self, _: Self) -> Self::Output {
        panic!("Programmer error to ever arrive here.")
    }
}

impl Add for ConcreteInt {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let items: Vec<i64> = self
            .items
            .into_iter()
            .zip(rhs.items.into_iter())
            .map(|(x, y)| x + y)
            .collect();
        Self { items }
    }
}

impl Add for ConcreteFloat {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let items: Vec<f64> = self
            .items
            .into_iter()
            .zip(rhs.items.into_iter())
            .map(|(x, y)| x + y)
            .collect();
        Self { items }
    }
}

impl ConcreteArrayTrait for ConcreteInt {
    fn len(&self) -> usize {
        self.items.len()
    }
}
impl ConcreteArrayTrait for ConcreteString {
    fn len(&self) -> usize {
        self.items.len()
    }
}
impl ConcreteArrayTrait for ConcreteFloat {
    fn len(&self) -> usize {
        self.items.len()
    }
}

// We could use trait objects if we didn't know all the types ahead of time.
// struct AltSeries {
//     item: Box<dyn ConcreteArrayTrait>
// }
// TODO: shared buffers (e.g. using Arc)?
#[pyclass]
#[derive(Clone)]
enum Series {
    Int(ConcreteInt),
    Float(ConcreteFloat),
    String(ConcreteString),
}

impl std::fmt::Debug for Series {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Series::Int(ConcreteInt { items }) => write!(f, "IntSeries({items:?})"),
            Series::Float(ConcreteFloat { items }) => write!(f, "FloatSeries({items:?})"),
            Series::String(ConcreteString { items }) => write!(f, "StringSeries({items:?})"),
        }
    }
}

#[pymethods]
impl Series {
    #[new]
    fn create(pylist: Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(items) = pylist.extract::<Vec<i64>>() {
            Ok(Series::Int(ConcreteInt { items }))
        } else if let Ok(items) = pylist.extract::<Vec<f64>>() {
            Ok(Series::Float(ConcreteFloat { items }))
        } else if let Ok(items) = pylist.extract::<Vec<String>>() {
            Ok(Series::String(ConcreteString { items }))
        } else {
            Err(PyValueError::new_err("Invalid item type"))
        }
    }

    fn __repr__(&self) -> String {
        match self {
            Series::Int(ConcreteInt { items }) => format!("IntSeries({items:?})"),
            Series::Float(ConcreteFloat { items }) => format!("FloatSeries({items:?})"),
            Series::String(ConcreteString { items }) => format!("StringSeries({items:?})"),
        }
    }

    fn __add__(slf: PyRef<'_, Self>, other: Bound<'_, PyAny>) -> PyResult<Self> {
        let slf = slf.clone();
        let other: Series = other.extract()?;
        slf.add(other)
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
enum Dtype {
    Int,
    Float,
    String,
}

impl Dtype {
    fn infer(s: &str) -> Option<Self> {
        if s.len() == 1 && s == "\"" {
            return None;
        }
        let mut cs = s.chars();
        let first = cs.next()?;
        let last = cs.next_back();
        if first == '\"' && last.is_some() && last.unwrap() == '\"' {
            return Some(Self::String);
        }

        fn acceptable_char(c: char) -> bool {
            c.is_ascii_digit() || c == '.' || c == '-'
        }
        if s.chars().all(acceptable_char)
            && acceptable_char(first)
            && last.map_or_else(|| true, acceptable_char)
        {
            if s.contains('.') {
                Some(Self::Float)
            } else {
                Some(Self::Int)
            }
        } else {
            None
        }
    }
}

fn collect_early_exit<'s, Input: 's, Return, F, E, It>(it: It, func: F) -> Result<Vec<Return>, E>
where
    It: Iterator<Item = &'s Input>,
    F: Fn(&Input) -> Result<Return, E>,
{
    let mut vec = Vec::new();
    for item in it {
        vec.push(func(item)?);
    }
    Ok(vec)
}

impl Series {
    fn infer_dtype(sl: &[String]) -> Result<Dtype, Box<dyn Error>> {
        let (dtype_success, dtype_fail): (HashSet<_>, HashSet<_>) = sl
            .iter()
            .map(|s| Dtype::infer(s.as_str()))
            .partition(|opt| opt.is_some());
        if dtype_fail.is_empty() {
            let mut success_iter = dtype_success.into_iter().map(|opt| opt.unwrap());
            if let Some(mut dtype) = success_iter.next() {
                for entry in success_iter {
                    match (entry, dtype) {
                        (Dtype::Float, Dtype::Int) => {
                            dtype = Dtype::Float;
                        }
                        (Dtype::Int, Dtype::Float) => {
                            dtype = Dtype::Float;
                        }
                        (x, y) if x == y => {
                            continue;
                        }
                        _ => {
                            return Err(format!(
                                "Incompatible mixture of dtypes inferred: {entry:?} and {dtype:?}"
                            )
                            .into())
                        }
                    };
                }
                Ok(dtype)
            } else {
                Err("Empty sequence of entries provided for inference".into())
            }
        } else {
            Err(format!("Failed to parse some elements: {dtype_fail:?}").into())
        }
    }
    fn from_untyped(sl: &[String]) -> Result<Self, Box<dyn Error>> {
        let target_dtype = Self::infer_dtype(sl)?;
        match target_dtype {
            Dtype::Int => Ok(Series::Int(ConcreteInt {
                items: collect_early_exit(sl.iter(), |item| item.parse::<i64>())?,
            })),
            Dtype::Float => Ok(Series::Float(ConcreteFloat {
                items: collect_early_exit(sl.iter(), |item| item.parse::<f64>())?,
            })),
            Dtype::String => Ok(Series::String(ConcreteString {
                items: sl
                    .iter()
                    .map(|s| s.trim_matches('\"'))
                    .map(|s| s.to_owned())
                    .collect(),
            })),
        }
    }
    fn dtype(&self) -> Dtype {
        match self {
            Self::Int(_) => Dtype::Int,
            Self::Float(_) => Dtype::Float,
            Self::String(_) => Dtype::String,
        }
    }

    fn i64(self) -> Result<ConcreteInt, Box<dyn Error>> {
        match self {
            Self::Int(concrete) => Ok(concrete),
            _ => Err("Not Int Series".into()),
        }
    }

    fn f64(self) -> Result<ConcreteFloat, Box<dyn Error>> {
        match self {
            Self::Float(concrete) => Ok(concrete),
            _ => Err("Not Float Series".into()),
        }
    }

    fn string(self) -> Result<ConcreteString, Box<dyn Error>> {
        match self {
            Self::String(concrete) => Ok(concrete),
            _ => Err("Not String Series".into()),
        }
    }

    fn promote(lhs: Self, rhs: Self) -> PyResult<(Self, Self, Dtype)> {
        match (&lhs, &rhs) {
            (Self::Int(l), Self::Float(_)) => Ok((
                Self::Float(ConcreteFloat {
                    items: l.items.iter().map(|v| *v as f64).collect(),
                }),
                rhs,
                Dtype::Float,
            )), // todo: implement Into for ConcreteArray
            (Self::Float(_), Self::Int(r)) => Ok((
                lhs,
                Self::Float(ConcreteFloat {
                    items: r.items.iter().map(|v| *v as f64).collect(),
                }),
                Dtype::Float,
            )), // todo: implement Into for ConcreteArray
            (x, y) if x.dtype() == y.dtype() => {
                let dtype = lhs.dtype();
                Ok((lhs, rhs, dtype))
            }
            _ => Err(PyValueError::new_err("PromotionError!")),
        }
    }
}

impl Add for Series {
    type Output = PyResult<Self>;

    fn add(self, rhs: Self) -> Self::Output {
        let (lhs, rhs, dtype) = Self::promote(self, rhs)?;
        match dtype {
            Dtype::Float => Ok(Self::Float(lhs.f64().unwrap() + rhs.f64().unwrap())),
            Dtype::Int => Ok(Self::Int(lhs.i64().unwrap() + rhs.i64().unwrap())),
            Dtype::String => Ok(Self::String(lhs.string().unwrap() + rhs.string().unwrap())),
        }
    }
}
impl ConcreteArrayTrait for Series {
    fn len(&self) -> usize {
        match self {
            Self::Int(concrete) => concrete.len(),
            Self::Float(concrete) => concrete.len(),
            Self::String(concrete) => concrete.len(),
        }
    }
}

#[pyclass]
struct DataFrame {
    item: HashMap<String, Series>,
}

#[pymethods]
impl DataFrame {
    #[new]
    fn create(pydict: Bound<'_, PyDict>) -> PyResult<Self> {
        let mut item: HashMap<String, Series> = HashMap::new();
        let mut length: Option<usize> = None;
        for (k, v) in pydict {
            let col_name: String = k.extract()?;
            let vec: Series = Series::create(v)?;
            if *length.get_or_insert(vec.len()) != vec.len() {
                return Err(PyValueError::new_err(
                    "Incompatible length columns provided",
                ));
            }
            item.insert(col_name, vec);
        }
        Ok(Self { item })
    }
    fn __repr__(&self) -> String {
        format!("DataFrame({:?})", self.item)
    }

    fn __str__(&self) -> String {
        format!("DataFrame({:?})", self.item)
    }

    fn __len__(&self) -> usize {
        self.item.values().next().map_or(0, |s| s.len())
    }

    fn __getitem__(&self, index: Bound<'_, PyString>) -> PyResult<Series> {
        let key: String = index.extract()?;
        // TODO: keep data in rust? Make Series PyClass compatible with print for viewing in python
        let result = self
            .item
            .get(&key)
            .ok_or(PyKeyError::new_err("Unrecognised key"))?;
        Ok(result.clone())
    }

    fn __setitem__(&mut self, index: Bound<'_, PyString>, value: Bound<'_, PyAny>) -> PyResult<()> {
        let key: String = index.extract()?;
        let value: Series = Series::create(value)?;
        let current_length = self.__len__();
        if current_length != 0 && (current_length != value.len()) {
            return Err(PyValueError::new_err(
                "Incompatible length series inserted!",
            ));
        }
        self.item.insert(key, value);
        Ok(())
    }

    #[staticmethod]
    fn from_csv(path: Bound<'_, PyString>) -> PyResult<Self> {
        let path: String = path.extract()?;
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        // TODO: move out of Box<dyn Error>
        match Self::from_csv_reader(reader) {
            Err(_) => Err(PyValueError::new_err("Failed to parse appropriately")),
            Ok(res) => Ok(res),
        }
    }
}

impl DataFrame {
    // Ideally use serde but I'm on a flight and cannot `cargo add`
    fn from_csv_reader<R: Read>(buf_reader: BufReader<R>) -> Result<Self, Box<dyn Error>> {
        let mut iter = buf_reader.lines();
        if let Some(Ok(header_elements)) = iter.next() {
            let header_elements: Vec<String> = header_elements
                .trim_matches(',')
                .split(',')
                .map(|s| s.trim().to_owned())
                .collect();
            let mut str_items: HashMap<&str, Vec<String>> = header_elements
                .iter()
                .map(|s| (s.as_str(), vec![]))
                .collect();
            for line in iter {
                let line = line?;
                let split_tokens = line.trim_matches(',').split(',');
                let mut count = 0;
                for (key, tok) in header_elements.iter().zip(split_tokens) {
                    str_items
                        .get_mut(key.as_str())
                        .unwrap()
                        .push(tok.to_owned());
                    count += 1;
                }
                if count != header_elements.len() {
                    return Err("Incompatible row length with number of columns in header".into());
                }
            }
            let mut item: HashMap<String, Series> = HashMap::new();
            for (k, v) in str_items.into_iter() {
                let series = Series::from_untyped(v.as_slice())?;
                item.insert(k.to_owned(), series);
            }

            return Ok(Self { item });
        } else {
            return Err("Empty buffer".into());
        }
    }
}

// impl IntoPy<PyObject> for Series {
//     fn into_py(self, py: Python<'_>) -> PyObject {
//         match self {
//             Self::Int(concrete) => concrete.into_py(py),
//             Self::Float(concrete) => concrete.into_py(py),
//             Self::String(concrete) => concrete.into_py(py),
//         }
//     }
// }

#[pymodule]
fn dfrs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DataFrame>()?;
    m.add_class::<Series>()?;
    Ok(())
}
