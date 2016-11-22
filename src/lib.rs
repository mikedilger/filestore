// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

//! joist_store is a crate that handles storage and retrieval of files.
//! Files are stored along with a filename, retrieved via a key issued
//! at storage.  Content is deduplicated at storage time, so only one
//! copy of each distinct file is stored, with potentially multiple
//! references to it.

// For serde_macros
#![feature(proc_macro)]

#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate log;
extern crate byteorder;
extern crate crypto;
#[cfg(feature = "rustc-serialize")]
extern crate rustc_serialize;
#[cfg(feature = "serde")]
extern crate serde;
#[cfg(feature = "serde")]
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate postgres;

pub mod error;
pub mod filekey;
mod hashable;
mod storable;

use std::fs;
use std::fs::{File,OpenOptions};
use std::io;
use std::path::{Path,PathBuf};

use byteorder::{ReadBytesExt,WriteBytesExt,BigEndian};

use error::{Error,ErrorKind};

pub use filekey::FileKey;
use hashable::Hashable;
use storable::Storable;


/// Returns PathBuf for directory that data will be stored into
fn storage_file_dir(storage_path: &Path, key: &FileKey) -> PathBuf {
    let r: &str = &**key;
    storage_path.to_path_buf().join( &r[..2] )
}

/// Returns short name of file that data will be stored into
fn storage_file_name(key: &FileKey) -> String {
    let r: &str = &*key;
    r[2..].to_owned()
}

/// Returns full PathBuf for file that data will be stored intoa
fn storage_file_path(storage_path: &Path, key: &FileKey) -> PathBuf
{
    storage_file_dir(storage_path, key).to_path_buf().join( &storage_file_name(key)[..] )
}

/// Returns short name of file that refcount will be stored into
fn storage_refcount_name(key: &FileKey) -> String {
    let r: &str = &*key;
    (r[2..]).to_owned() + ".refcount"
}

// Returns full PathBuf for file that refcount will be stored into
fn storage_refcount_path(storage_path: &Path, key: &FileKey) -> PathBuf
{
    storage_file_dir(storage_path, key).to_path_buf().join( &storage_refcount_name(key)[..] )
}

/// Store the input at the storage_path.  Hashes, uses that as a key and
/// also the filename, and manages refcounts (in case it is pre-existing)
fn store<T: Storable + Hashable>(storage_path: &Path, input: &T)
                                 -> Result<FileKey,Error>
{
    let key: FileKey = FileKey(try!(input.hash()));

    // Make storage_file_dir, if it doesn't already exist
    let storage_file_dir = storage_file_dir(storage_path, &key);
    if let Err(e) = fs::create_dir(&storage_file_dir) {
        if e.kind() != io::ErrorKind::AlreadyExists { return Err( From::from(e) ); }
    }

    // Check if file content exists, and copy as needed
    let storage_file_path = storage_file_path(storage_path, &key);
    match fs::metadata(&storage_file_path) {
        Ok(_) => {
            // We presume no hash collisions due to the cryptographically
            // large hash space
        },
        Err(e) => {
            if e.kind() == io::ErrorKind::NotFound {
                // Store content
                try!( input.store(&storage_file_path) );
            }
            else {
                return Err( From::from(e) );
            }
        }
    }

    // Increment the ref count
    let mut refcount: u32 = try!( get_refcount(storage_path, &key) );
    refcount = refcount + 1;
    try!( set_refcount(storage_path, &key, refcount) );
    Ok( key )
}

/// Store data from memory
pub fn store_data(storage_path: &Path, input: &Vec<u8>) -> Result<FileKey,Error>
{
    store(storage_path, input)
}

/// Store a file
pub fn store_file(storage_path: &Path, input: &Path) -> Result<FileKey,Error>
{
    store(storage_path, &input.to_path_buf())
}

/// Retrieve into memory
pub fn retrieve_data(storage_path: &Path, key: &FileKey) -> Option<Vec<u8>>
{
    let path = storage_file_path(storage_path, key);
    match fs::metadata(&path) {
        Err(_) => None,
        Ok(_) => {
            match Storable::retrieve(&path) {
                Ok(p) => Some(p),
                Err(_) => None,
            }
        }
    }
}

/// Retrieve into a file
pub fn retrieve_file(storage_path: &Path, key: &FileKey) -> Option<PathBuf>
{
    let pathbuf = storage_file_path(storage_path, key);
    match fs::metadata(&pathbuf) {
        Err(_) => None,
        Ok(_) => {
            match Storable::retrieve(&pathbuf) {
                Ok(p) => Some(p),
                Err(_) => None,
            }
        }
    }
}

/// Delete.
pub fn delete(storage_path: &Path, key: &FileKey) -> Result<(),Error>
{
    let path = storage_file_path(storage_path, key);

    // Decrement the ref count
    let mut refcount: u32 = try!(get_refcount(storage_path, key));
    if refcount < 1 {
        return Ok(()); // nothing to delete
    }
    refcount = refcount - 1;
    try!(set_refcount(storage_path, key, refcount));

    // Actually delete if there are no more references
    if refcount < 1 {
        try!( fs::remove_file( &path )
              .map_err(|e| { (e, "Unable to remove file") } ));
    }

    Ok(())
}

fn get_refcount(storage_path: &Path, key: &FileKey) -> Result<u32,Error>
{
    let storage_refcount_path = storage_refcount_path(storage_path, key);
    match fs::metadata(&storage_refcount_path) {
        Ok(_) => {
            let mut f = try!( File::open(&storage_refcount_path)
                              .map_err(|e| { (e, "Unable to open refcount file") } ));
            match f.read_u32::<BigEndian>() {
                Ok(u) => Ok(u),
                Err(e) => {
                    match e {
                        ::byteorder::Error::UnexpectedEOF => Ok(0),
                        ::byteorder::Error::Io(e) => Err( From::from(e) ),
                    }
                }
            }
        },
        Err(e) => {
            if e.kind() == io::ErrorKind::NotFound {
                Ok(0)
            }
            else {
                Err( From::from(e) )
            }
        }
    }
}

fn set_refcount(storage_path: &Path, key: &FileKey, refcount: u32) -> Result<(),Error>
{
    let storage_refcount_path = storage_refcount_path(storage_path, key);

    // If zero, delete the refcount file
    if refcount < 1 {
        try!( fs::remove_file( &storage_refcount_path )
              .map_err(|e| { (e, "Unable to remove refcount file") } ));
        return Ok(());
    }

    // Otherwise, write the new refcount
    let mut f = try!( OpenOptions::new()
                      .create(true).write(true).truncate(true).open(&storage_refcount_path)
                      .map_err(|e| { (e, "Unable to open/create new refcount file") } ));
    if let Err(e) = f.write_u32::<BigEndian>(refcount) {
        match e {
            ::byteorder::Error::UnexpectedEOF => return Err(
                Error::new(ErrorKind::Io(io::Error::new(io::ErrorKind::Other, e)))),
            ::byteorder::Error::Io(e) => return Err( From::from(e) ),
        }
    }
    Ok(())
}
