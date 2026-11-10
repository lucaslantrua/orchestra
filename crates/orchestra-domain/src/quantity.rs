//! Resource quantities with Kubernetes-style textual notation (`500m`, `32Gi`).

use std::{fmt, ops, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid quantity {0:?}")]
pub struct QuantityError(pub String);

/// Accepts quantities written as YAML/JSON numbers as well as strings.
#[derive(Deserialize)]
#[serde(untagged)]
enum Raw {
    Int(u64),
    Float(f64),
    Str(String),
}

impl Raw {
    fn into_text(self) -> String {
        match self {
            Raw::Int(n) => n.to_string(),
            Raw::Float(f) => f.to_string(),
            Raw::Str(s) => s,
        }
    }
}

macro_rules! quantity_impls {
    ($ty:ident) => {
        impl ops::Add for $ty {
            type Output = $ty;
            fn add(self, rhs: $ty) -> $ty {
                $ty(self.0.saturating_add(rhs.0))
            }
        }

        impl ops::AddAssign for $ty {
            fn add_assign(&mut self, rhs: $ty) {
                *self = *self + rhs;
            }
        }

        impl $ty {
            pub fn saturating_sub(self, rhs: $ty) -> $ty {
                $ty(self.0.saturating_sub(rhs.0))
            }
        }

        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Raw::deserialize(d)?
                    .into_text()
                    .parse()
                    .map_err(serde::de::Error::custom)
            }
        }
    };
}

/// CPU quantity, stored in millicores.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cpu(u64);

impl Cpu {
    pub const fn from_millis(millis: u64) -> Self {
        Cpu(millis)
    }

    pub const fn from_cores(cores: u64) -> Self {
        Cpu(cores * 1000)
    }

    pub const fn millis(self) -> u64 {
        self.0
    }
}

impl FromStr for Cpu {
    type Err = QuantityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || QuantityError(s.to_string());
        let s = s.trim();
        if let Some(millis) = s.strip_suffix('m') {
            return millis.parse().map(Cpu).map_err(|_| err());
        }
        let cores: f64 = s.parse().map_err(|_| err())?;
        if !(0.0..=1e12).contains(&cores) {
            return Err(err());
        }
        Ok(Cpu((cores * 1000.0).round() as u64))
    }
}

impl fmt::Display for Cpu {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 % 1000 == 0 {
            write!(f, "{}", self.0 / 1000)
        } else {
            write!(f, "{}m", self.0)
        }
    }
}

quantity_impls!(Cpu);

/// Memory (or any byte-sized) quantity, stored in bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Memory(u64);

const BINARY_SUFFIXES: [(&str, u64); 4] =
    [("Ti", 1 << 40), ("Gi", 1 << 30), ("Mi", 1 << 20), ("Ki", 1 << 10)];
const DECIMAL_SUFFIXES: [(&str, u64); 4] = [
    ("T", 1_000_000_000_000),
    ("G", 1_000_000_000),
    ("M", 1_000_000),
    ("K", 1_000),
];

impl Memory {
    pub const fn from_bytes(bytes: u64) -> Self {
        Memory(bytes)
    }

    pub const fn from_gib(gib: u64) -> Self {
        Memory(gib << 30)
    }

    pub const fn bytes(self) -> u64 {
        self.0
    }
}

impl FromStr for Memory {
    type Err = QuantityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || QuantityError(s.to_string());
        let s = s.trim();
        let (number, factor) = BINARY_SUFFIXES
            .iter()
            .chain(DECIMAL_SUFFIXES.iter())
            .find_map(|(suffix, factor)| s.strip_suffix(suffix).map(|n| (n, *factor)))
            .unwrap_or((s, 1));
        if let Ok(n) = number.parse::<u64>() {
            return n.checked_mul(factor).map(Memory).ok_or_else(err);
        }
        // Fractional values such as `1.5Gi`.
        let n: f64 = number.parse().map_err(|_| err())?;
        let bytes = n * factor as f64;
        if !bytes.is_finite() || bytes < 0.0 || bytes >= u64::MAX as f64 {
            return Err(err());
        }
        Ok(Memory(bytes.round() as u64))
    }
}

impl fmt::Display for Memory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (suffix, factor) in BINARY_SUFFIXES {
            if self.0 != 0 && self.0 % factor == 0 {
                return write!(f, "{}{}", self.0 / factor, suffix);
            }
        }
        write!(f, "{}", self.0)
    }
}

quantity_impls!(Memory);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_parsing_and_display() {
        assert_eq!("500m".parse::<Cpu>().unwrap().millis(), 500);
        assert_eq!("8".parse::<Cpu>().unwrap(), Cpu::from_cores(8));
        assert_eq!("0.25".parse::<Cpu>().unwrap().millis(), 250);
        assert_eq!(Cpu::from_millis(1500).to_string(), "1500m");
        assert_eq!(Cpu::from_cores(2).to_string(), "2");
        assert!("-1".parse::<Cpu>().is_err());
        assert!("abc".parse::<Cpu>().is_err());
    }

    #[test]
    fn memory_parsing_and_display() {
        assert_eq!("32Gi".parse::<Memory>().unwrap(), Memory::from_gib(32));
        assert_eq!("1.5Gi".parse::<Memory>().unwrap().bytes(), 3 << 29);
        assert_eq!("2K".parse::<Memory>().unwrap().bytes(), 2000);
        assert_eq!("1024".parse::<Memory>().unwrap().to_string(), "1Ki");
        assert_eq!(Memory::from_gib(80).to_string(), "80Gi");
        assert!("12Xi".parse::<Memory>().is_err());
        assert!("99999999999Ti".parse::<Memory>().is_err());
    }

    #[test]
    fn serde_accepts_numbers_and_strings() {
        assert_eq!(serde_json::from_str::<Cpu>("8").unwrap(), Cpu::from_cores(8));
        assert_eq!(serde_json::from_str::<Cpu>("0.5").unwrap().millis(), 500);
        assert_eq!(
            serde_json::from_str::<Memory>("\"80Gi\"").unwrap(),
            Memory::from_gib(80)
        );
        assert_eq!(serde_json::to_string(&Memory::from_gib(1)).unwrap(), "\"1Gi\"");
    }
}
