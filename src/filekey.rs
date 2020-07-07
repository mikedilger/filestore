// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

use std::fmt;
use std::ops::Deref;
#[cfg(feature = "serde")]
use serde::{Serialize, Deserialize};
#[cfg(feature = "postgres")]
use postgres::types::{ToSql, FromSql};

/// A key issued at storage, used to retrieve your file
#[derive(PartialEq, Eq, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
pub struct FileKey(pub String);

impl fmt::Display for FileKey
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error>
    {
        write!(f, "{}", &*self)
    }
}

impl Deref for FileKey {
    type Target = str;

    fn deref(&self) -> &str {
        &*self.0
    }
}

// inner error for building postgres conversion errors
#[cfg(feature = "postgres")]
#[derive(Debug)]
pub struct WrongType(pub ::postgres::types::Type);

#[cfg(feature = "postgres")]
impl fmt::Display for WrongType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "cannot convert to or from a Postgres value of type `{}`",
               self.0)
    }
}

#[cfg(feature = "postgres")]
impl ::std::error::Error for WrongType {
    fn description(&self) -> &str {
        "cannot convert to or from a Postgres value"
    }
}

#[cfg(feature = "postgres")]
impl WrongType {
    pub fn new(ty: ::postgres::types::Type) -> WrongType {
        WrongType(ty)
    }
}
