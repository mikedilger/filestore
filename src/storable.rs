
use std::fs::{File,OpenOptions};
use std::path::{Path,PathBuf};
use std::io::{Read,Write};
use super::Error;

/// A trait for things which can be stored and retrieved
pub trait Storable: Sized {
    fn store(&self, dest_path: &Path) -> Result<(), Error>;
    fn retrieve(dest_path: &Path) -> Result<Self, Error>;
}

impl Storable for Vec<u8> {
    fn store(&self, dest_path: &Path) -> Result<(), Error> {
        let mut file = try!( OpenOptions::new()
                             .create(true).write(true).truncate(true).open(dest_path)
                             .map_err(|e| { (e, "Unable to open/creat new file") } ));
        try!( file.write_all( &*self )
              .map_err(|e| { (e, "Unable to write new file") } ));
        Ok(())
    }

    fn retrieve(dest_path: &Path) -> Result<Vec<u8>, Error>
    {
        let mut file = try!( File::open(dest_path)
                             .map_err(|e| { (e, "Unable to open file for reading") } ));
        let mut buf: Vec<u8> = Vec::new();
        try!(file.read_to_end(&mut buf)
             .map_err(|e| { (e, "Unable to read to end of file") } ));
        Ok(buf)
    }
}

impl Storable for PathBuf {
    fn store(&self, dest_path: &Path) -> Result<(), Error> {
        try!( ::std::fs::copy(self, dest_path)
              .map_err(|e| { (e, "Unable to copy file") } ));
        Ok(())
    }
    fn retrieve(dest_path: &Path) -> Result<PathBuf,Error> {
        Ok(dest_path.to_path_buf())
    }
}
