// Copyright © 2014 - 2015 by Optimal Computing Limited (of New Zealand)
// This code is licensed under the MIT license (see LICENSE-MIT for details)

//! `filestore` is a crate that handles storage and retrieval of files.
//! Files are stored along with a filename, retrieved via a key issued
//! at storage.  Content is deduplicated at storage time, so only one
//! copy of each distinct file is stored, with potentially multiple
//! references to it.

#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

extern crate log;
extern crate byteorder;
extern crate crypto;
#[cfg(feature = "serde")]
extern crate serde;
#[cfg(feature = "postgres")]
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

use error::Error;

pub use filekey::FileKey;
use hashable::Hashable;
use storable::Storable;

/// Store data from memory.  The returned `FileKey` can be used later to
/// retrieve the data.
pub fn store_data(storage_path: &Path, input: &Vec<u8>) -> Result<FileKey, Error>
{
    store(storage_path, input)
}

/// Store a copy of a file.  The returned `FileKey` can be used later to
/// retrieve the file.
///
/// Copying is required as the input file may not be on the same filesystem as the
/// storage path.
pub fn store_file(storage_path: &Path, input: &Path) -> Result<FileKey, Error>
{
    store(storage_path, &input.to_path_buf())
}

/// Retrieve data into memory, using a `FileKey` that was returned from an earlier
/// call to `store_data()`
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

/// Retrieve a file by learning it's storage path, using a `FileKey` that was
/// returned from an earlier call to `store_file()`.
///
/// The returned `PathBuf` is the path to the actual only copy of the stored file,
/// it is not a copy. Do not delete it; use `delete()` for that purpose as it
/// manages the refcount properly.
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

/// Delete stored data (or file) based on a `FileKey` that was returned
/// from an earlier call to `store_file()` or `store_data()`.
pub fn delete(storage_path: &Path, key: &FileKey) -> Result<(), Error>
{
    let path = storage_file_path(storage_path, key);

    // Decrement the ref count
    let mut refcount: u32 = get_refcount(storage_path, key)?;
    if refcount < 1 {
        return Ok(()); // nothing to delete
    }
    refcount -= 1;
    set_refcount(storage_path, key, refcount)?;

    // Actually delete if there are no more references
    if refcount < 1 {
        fs::remove_file( &path )
            .map_err(|e| { (e, "Unable to remove file") } )?;
    }

    Ok(())
}


// Returns `PathBuf` for directory that data will be stored into
fn storage_file_dir(storage_path: &Path, key: &FileKey) -> PathBuf {
    let r: &str = &**key;
    storage_path.to_path_buf().join( &r[..2] )
}

// Returns short name of file that data will be stored into
fn storage_file_name(key: &FileKey) -> String {
    let r: &str = &*key;
    r[2..].to_owned()
}

// Returns full `PathBuf` for file that data will be stored intoa
fn storage_file_path(storage_path: &Path, key: &FileKey) -> PathBuf
{
    storage_file_dir(storage_path, key).to_path_buf().join( &storage_file_name(key)[..] )
}

// Returns short name of file that refcount will be stored into
fn storage_refcount_name(key: &FileKey) -> String {
    let r: &str = &*key;
    (r[2..]).to_owned() + ".refcount"
}

// Returns full `PathBuf` for file that refcount will be stored into
fn storage_refcount_path(storage_path: &Path, key: &FileKey) -> PathBuf
{
    storage_file_dir(storage_path, key).to_path_buf().join( &storage_refcount_name(key)[..] )
}

// Store the input at the storage_path.  Hashes, uses that as a key and
// also the filename, and manages refcounts (in case it is pre-existing)
fn store<T: Storable + Hashable>(storage_path: &Path, input: &T)
                                 -> Result<FileKey, Error>
{
    let key: FileKey = FileKey(input.hash()?);

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
                input.store(&storage_file_path)?;
            }
            else {
                return Err( From::from(e) );
            }
        }
    }

    // Increment the ref count
    let mut refcount: u32 = get_refcount(storage_path, &key)?;
    refcount += 1;
    set_refcount(storage_path, &key, refcount)?;
    Ok( key )
}

fn get_refcount(storage_path: &Path, key: &FileKey) -> Result<u32, Error>
{
    let storage_refcount_path = storage_refcount_path(storage_path, key);
    match fs::metadata(&storage_refcount_path) {
        Ok(_) => {
            let mut f = File::open(&storage_refcount_path)
                .map_err(|e| { (e, "Unable to open refcount file") } )?;
            match f.read_u32::<BigEndian>() {
                Ok(u) => Ok(u),
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        Ok(0)
                    } else {
                        Err(From::from(e))
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

fn set_refcount(storage_path: &Path, key: &FileKey, refcount: u32) -> Result<(), Error>
{
    let storage_refcount_path = storage_refcount_path(storage_path, key);

    // If zero, delete the refcount file
    if refcount < 1 {
        fs::remove_file( &storage_refcount_path )
            .map_err(|e| { (e, "Unable to remove refcount file") } )?;
        return Ok(());
    }

    // Otherwise, write the new refcount
    let mut f = OpenOptions::new()
        .create(true).write(true).truncate(true).open(&storage_refcount_path)
        .map_err(|e| { (e, "Unable to open/create new refcount file") } )?;
    f.write_u32::<BigEndian>(refcount)?;
    Ok(())
}
