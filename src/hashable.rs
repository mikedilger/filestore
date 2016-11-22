
use std::fs::File;
use std::path::PathBuf;
use std::io::Read;
use crypto::sha2::Sha224;
use crypto::digest::Digest;
use error::Error;

/// A private trait Hashable (with Sha224 only)
pub trait Hashable {
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
