// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

use std::fmt;
use std::ops::Deref;
use std::error::Error as StdError;

use postgres::types::{ToSql, FromSql, Type, IsNull};

/// A key issued at storage, used to retrieve your file
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct FileKey(pub String);

impl ToSql for FileKey {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>)
              -> Result<IsNull, Box<StdError + Sync + Send>>
        where Self: Sized
    {
        // use the inner type
        self.0.to_sql(ty,out)
    }

    accepts!(Type::Text);

    fn to_sql_checked(&self, ty: &Type, out: &mut Vec<u8>)
                      -> Result<IsNull, Box<StdError + Sync + Send>>
        where Self: Sized
    {
        // use the inner type
        self.0.to_sql_checked(ty,out)
    }
}

impl FromSql for FileKey {
    fn from_sql(ty: &Type, raw: &[u8])
                -> Result<Self, Box<StdError + Sync + Send>>
    {
        // use the inner type
        let s = try!(<String>::from_sql(ty, raw));
        Ok(FileKey(s))
    }

    accepts!(Type::Text);
}

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
