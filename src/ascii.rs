/// Nonsense to deal with Rust's insistence on utf8 and bencoding's insistence on ascii
use std::ops::{Deref, DerefMut};

use ::ascii::{AsciiString as AString, AsciiChar as AChar};
use bendy::{encoding::ToBencode, decoding::FromBencode};


pub struct AsciiString(pub AString);

impl ToBencode for AsciiString {
    const MAX_DEPTH:usize  = 0 as usize;

    // Bencoding is fucking idiotic
    fn to_bencode(&self) -> Result<Vec<u8>, bendy::encoding::Error> {
        let byte_slice = self.0.as_bytes();

        let mut digit_count = (byte_slice.len() as f64).log10().floor() as usize + 1;

        // 1 byte for the ":" symbol, the rest for the digits in base 10
        let mut vec = Vec::with_capacity(byte_slice.len() + 1 + digit_count);
       
        let length_in_base_10 = byte_slice.len().to_string(); 
        vec.extend_from_slice(length_in_base_10.as_bytes());

        vec.push(b':');
        vec.extend_from_slice(byte_slice);

        Ok(vec)
    }


    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        let str_slice = self.0.as_str();
        encoder.emit_str(str_slice)
    }
} 

impl FromBencode for AsciiString {
    fn decode_bencode_object(object: bendy::decoding::Object) -> Result<Self, bendy::decoding::Error>
    where
        Self: Sized {

        match object {
            bendy::decoding::Object::List(_) => Err(bendy::decoding::Error::unexpected_token("bytes", "list")),
            bendy::decoding::Object::Dict(_) => Err(bendy::decoding::Error::unexpected_token("bytes", "dict")),
            bendy::decoding::Object::Integer(_) => Err(bendy::decoding::Error::unexpected_token("bytes", "int")),
            bendy::decoding::Object::Bytes(slice) => {
                let ascii_string = AString::from_ascii(slice).map_err(|_| bendy::decoding::Error::unexpected_token("valid asccii", "invalid ascii"))?;
                Ok(Self(ascii_string))
            },
        }

    }
}

impl Deref for AsciiString {
    type Target = AString;

    fn deref(&self) -> &Self::Target {
        &self.0 
    }
}

impl DerefMut for AsciiString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}



pub struct AscciChar(pub AChar);

impl ToBencode for AscciChar {
    const MAX_DEPTH: usize = 0 as usize;

    fn encode(&self, encoder: bendy::encoding::SingleItemEncoder) -> Result<(), bendy::encoding::Error> {
        use std::str;
        let bytes = [self.0.as_byte()];
        let str = unsafe { str::from_utf8_unchecked(&bytes) };
        encoder.emit_str(str)
    }

    fn to_bencode(&self) -> Result<Vec<u8>, bendy::encoding::Error> {
        let mut encoder = bendy::encoding::Encoder::new().with_max_depth(Self::MAX_DEPTH);
        encoder.emit_with(|e| self.encode(e).map_err(bendy::encoding::Error::into))?;

        let bytes = encoder.get_output()?;
        Ok(bytes)
    }
}

impl FromBencode for AscciChar {
    fn decode_bencode_object(object: bendy::decoding::Object) -> Result<Self, bendy::decoding::Error>
    where
        Self: Sized {
        match object {
            bendy::decoding::Object::List(_) => Err(bendy::decoding::Error::unexpected_token("bytes", "list")),
            bendy::decoding::Object::Dict(_) => Err(bendy::decoding::Error::unexpected_token("bytes", "dict")),
            bendy::decoding::Object::Integer(_) => Err(bendy::decoding::Error::unexpected_token("bytes", "int")),
            bendy::decoding::Object::Bytes(slice) => {
                if slice.len() != 1 {
                      Err(bendy::decoding::Error::unexpected_token("single byte", "multi byte"))
                } else {
                    let ascci_char = AChar::from_ascii(slice[0]).map_err(|error| bendy::decoding::Error::malformed_content(error))?;
                    Ok(Self(ascci_char))
                }
            },
        }
    }
}

impl Deref for AscciChar {
    type Target = AChar;

    fn deref(&self) -> &Self::Target {
        &self.0 
    }
}

impl DerefMut for AscciChar {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

