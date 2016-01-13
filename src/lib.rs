// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

//! joist_store is a crate that handles storage and retrieval of files.
//! Files are stored along with a filename, retrieved via a key issued
//! at storage.  Content is deduplicated at storage time, so only one
//! copy of each distinct file is stored, with potentially multiple
//! references to it.

#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate log;
extern crate byteorder;
extern crate crypto;
#[cfg(feature = "rustc-serialize")]
extern crate rustc_serialize;
#[cfg(feature = "serde")]
extern crate serde;
extern crate postgres;

pub mod error;
pub mod filekey;

use std::fs;
use std::fs::{File,OpenOptions};
use std::io;
use std::io::{Read,Write};
use std::path::{Path,PathBuf};

use byteorder::{ReadBytesExt,WriteBytesExt,BigEndian};
use crypto::sha2::Sha224;
use crypto::digest::Digest;

use error::{Error,ErrorKind};

pub use filekey::FileKey;

/// A private trait Hashable (with Sha224 only)
trait Hashable {
    /// Hash (with sha224) to result in a String or io::Error
    fn hash(&self) -> Result<String,Error>;
}

impl Hashable for Vec<u8> {
    fn hash(&self) -> Result<String,Error> {
        // Start the hash
        let mut hash = Box::new(Sha224::new());

        // Add the content
        hash.input( &*self );

        // Get the result
        Ok(hash.result_str())
    }
}

impl Hashable for PathBuf {
    fn hash(&self) -> Result<String,Error> {
        // Start the hash
        let mut hash = Box::new(Sha224::new());

        // Open the file
        let mut file = try!(
            File::open(self)
                .map_err(|e| { (e, "Cannot open content file for hashing") } ));

        // Digest 4096 bytes at a time
        let mut buf: [u8; 4096] = [0_u8; 4096];
        loop {
            let count = try!( file.read(&mut buf)
                              .map_err(|e| { (e, "Unable to read file to hash") } ));
            if count==0 { return Ok(hash.result_str()); }
            hash.input(&buf[..count]); // Add to hash input
        }
    }
}

/// A trait for things which can be stored and retrieved
trait Storable: Sized {
    fn store(&self, dest_path: &Path) -> Result<(),Error>;
    fn retrieve(dest_path: &Path) -> Result<Self,Error>;
}

impl Storable for Vec<u8> {
    fn store(&self, dest_path: &Path) -> Result<(),Error> {
        let mut file = try!( OpenOptions::new()
                             .create(true).write(true).truncate(true).open(dest_path)
                             .map_err(|e| { (e, "Unable to open/creat new file") } ));
        try!( file.write_all( &*self )
              .map_err(|e| { (e, "Unable to write new file") } ));
        Ok(())
    }

    fn retrieve(dest_path: &Path) -> Result<Vec<u8>,Error>
    {
        let mut file = try!( File::open(dest_path)
                             .map_err(|e| { (e, "Unable to open file for reading") } ));
        let mut buf: Vec<u8> = Vec::new();
        try!(file.read_to_end(&mut buf)
             .map_err(|e| { (e, "Unable to read to end of file") } ));
        return Ok(buf);
    }
}

impl Storable for PathBuf {
    fn store(&self, dest_path: &Path) -> Result<(),Error> {
        try!( fs::copy(self, dest_path)
              .map_err(|e| { (e, "Unable to copy file") } ));
        Ok(())
    }
    fn retrieve(dest_path: &Path) -> Result<PathBuf,Error> {
        Ok(dest_path.to_path_buf())
    }
}

/// Returns PathBuf for directory that data will be stored into
fn storage_file_dir(storage_path: &Path, key: &FileKey) -> PathBuf {
    let r: &str = &**key;
    storage_path.to_path_buf().join( &r[..2] )
}

/// Returns short name of file that data will be stored into
fn storage_file_name(key: &FileKey) -> String {
    let r: &str = &*key;
    r[2..].to_string()
}

/// Returns full PathBuf for file that data will be stored intoa
fn storage_file_path(storage_path: &Path, key: &FileKey) -> PathBuf
{
    storage_file_dir(storage_path, key).to_path_buf().join( &storage_file_name(key)[..] )
}

/// Returns short name of file that refcount will be stored into
fn storage_refcount_name(key: &FileKey) -> String {
    let r: &str = &*key;
    (r[2..]).to_string() + ".refcount"
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
