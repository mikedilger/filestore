// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

use std::fmt;
use std::ops::Deref;
use std::io::{Read,Write};

#[cfg(feature = "postgres")]
use postgres::types::{ToSql,FromSql,Type,IsNull,SessionInfo};
#[cfg(feature = "postgres")]
use postgres::error::Error as PgError;

/// A key issued at storage, used to retrieve your file
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "rustc-serialize", derive(RustcEncodable, RustcDecodable))]
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

#[cfg(feature = "rustc-serialize")]
impl ::rustc_serialize::json::ToJson for FileKey {
    fn to_json(&self) -> ::rustc_serialize::json::Json {
        let FileKey(ref s) = *self;
        ::rustc_serialize::json::Json::String(s.clone())
    }
}

#[cfg(feature = "postgres")]
impl ToSql for FileKey {
    fn to_sql<W: Write+?Sized>(&self, ty: &Type, mut out: &mut W, ctx: &SessionInfo)
            -> ::postgres::Result<IsNull>
    {
        let FileKey(ref s) = *self;
        s.to_sql(ty, out, ctx)
    }
    fn accepts(ty: &Type) -> bool {
        <String as ToSql>::accepts(ty)
    }
    to_sql_checked!();
}

#[cfg(feature = "postgres")]
impl FromSql for FileKey {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, ctx: &SessionInfo)
                         -> ::postgres::Result<FileKey> {
        let s: String = match FromSql::from_sql(ty,raw,ctx) {
            Ok(s) => s,
            Err(_) => return Err(PgError::Conversion(Box::new(WrongType::new(ty.clone())))),
       };
        Ok(FileKey(s))
    }
    fn accepts(ty: &Type) -> bool {
        <String as FromSql>::accepts(ty)
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
