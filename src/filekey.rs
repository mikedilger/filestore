// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

use std::fmt;
use std::ops::Deref;
use std::io::{Read,Write};

use postgres::types::{ToSql,FromSql,Type,IsNull,SessionInfo};
use postgres::error::Error::{WrongType};

/// A key issued at storage, used to retrieve your file
#[derive(PartialEq, Eq, Debug, Clone)]
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

    fn deref<'a>(&'a self) -> &'a str {
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
    fn to_sql_checked(&self, ty: &Type, out: &mut Write, ctx: &SessionInfo)
                      -> ::postgres::Result<IsNull>
    {
        if !<Self as ToSql>::accepts(ty) {
            return Err(WrongType(ty.clone()));
        }
        self.to_sql(ty, out, ctx)
    }
}

impl FromSql for FileKey {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, ctx: &SessionInfo)
                         -> ::postgres::Result<FileKey> {
        let s: String = match FromSql::from_sql(ty,raw,ctx) {
            Ok(s) => s,
            Err(_) => return Err(WrongType(ty.clone())),
        };
        Ok(FileKey(s))
    }
    fn accepts(ty: &Type) -> bool {
        <String as FromSql>::accepts(ty)
    }
}

#[cfg(feature = "serde")]
impl ::serde::ser::Serialize for FileKey {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
        where S: ::serde::ser::Serializer
    {
        serializer.visit_str(&*self.0)
    }
}

#[cfg(feature = "serde")]
impl ::serde::de::Deserialize for FileKey {
    fn deserialize<D>(deserializer: &mut D) -> Result<Self, D::Error>
        where D: ::serde::de::Deserializer
    {
        Ok(FileKey(try!(::serde::Deserialize::deserialize(deserializer))))
    }
}
