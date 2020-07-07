// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

use std::fmt;
use std::error::Error as StdError;
use std::io;

use log::Level;

pub struct Error {
    pub io: io::Error,
    pub message: String,
}

impl Error {
    pub fn log_level(&self) -> Level {
        match self.io.kind() {
            io::ErrorKind::NotFound => Level::Debug,
            _ => Level::Warn,
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.io.source()
    }
}

// This is for the Developer and Log files
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&*self.io.to_string())?;
        if ! self.message.is_empty() {
            write!(f, " = {}", self.message)?;
        }
        if self.source().is_some() {
            write!(f, ": {:?}", self.source().unwrap())?;
        }
        Ok(())
    }
}

// This is for the end user
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.io.kind() {
            io::ErrorKind::NotFound => {
                write!(f, "The file requested was not found.")
            }
            _ => {
                write!(f, "{}: ", self.io.to_string())?;
                self.io.fmt(f) // trust upstream?
            },
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error {
            io: err,
            message: "".to_owned()
        }
    }
}

impl<'a> From<(io::Error, &'a str)> for Error {
    fn from((err, message): (io::Error, &'a str)) -> Error {
        Error {
            io: err,
            message: message.to_owned(),
        }
    }
}
