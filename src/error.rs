// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

use std::fmt;
use std::error::Error as StdError;
use std::io;

use log::LogLevel;

#[derive(Debug)]
pub enum ErrorKind
{
    Io(io::Error),
    NotFound,
}

pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}

impl Error {
    pub fn new(kind: ErrorKind) -> Self {
        Error {
            kind: kind,
            message: "".to_owned(),
        }
    }

    pub fn log_level(&self) -> LogLevel {
        match self.kind {
            ErrorKind::Io(_) => LogLevel::Warn,
            ErrorKind::NotFound => LogLevel::Debug,
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str
    {
        match self.kind {
            ErrorKind::Io(_) => "I/O Error",
            ErrorKind::NotFound => "Not Found",
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match self.kind {
            ErrorKind::Io(ref e) => Some(e),
            _ => None
        }
    }
}

// This is for the Developer and Log files
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!( f.write_str(&*self.description()) );
        if ! self.message.is_empty() {
            try!( write!(f, " = {}", self.message) );
        }
        if self.cause().is_some() {
            try!( write!(f, ": {:?}", self.cause().unwrap()) );
        }
        Ok(())
    }
}

// This is for the end user
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            ErrorKind::Io(ref e) => {
                try!(write!(f, "{}: ", self.description()));
                e.fmt(f) // trust upstream?
            },
            ErrorKind::NotFound => {
                write!(f, "The file requested was not found.")
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::new( ErrorKind::Io(err) )
    }
}

impl<'a> From<(io::Error, &'a str)> for Error {
    fn from((e, message): (io::Error, &'a str)) -> Error {
        Error {
            kind: ErrorKind::Io(e),
            message: message.to_owned(),
        }
    }
}
