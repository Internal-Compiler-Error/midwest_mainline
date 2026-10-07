//! A bencode reader for what comes off the wire: one pass, borrowing from the packet, and no
//! panic whatever the input (an integer past i64, a length past the end, nesting 64 deep: all
//! just not bencode). Dict keys may come in any order, as plenty of live nodes send them; a
//! repeated key keeps its last value.

use std::collections::BTreeMap;

/// Deeper than any KRPC message, and BEP 44's limit on a stored value
const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Value<'a> {
    Int(i64),
    Bytes(&'a [u8]),
    List(Vec<Value<'a>>),
    Dict(Dict<'a>),
}

pub(crate) type Dict<'a> = BTreeMap<&'a [u8], Value<'a>>;

/// `raw` as one dict with nothing after it
pub(crate) fn parse_dict(raw: &[u8]) -> Option<Dict<'_>> {
    let mut reader = Reader { raw, at: 0 };
    match reader.value(0)? {
        Value::Dict(dict) if reader.at == raw.len() => Some(dict),
        _ => None,
    }
}

impl Value<'_> {
    /// Bencoded again. For canonical bencode (sorted keys, which BEP 44 values must be) it's the
    /// bytes it was parsed from, which is what signatures and targets are computed over.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = vec![];
        self.write(&mut out);
        out
    }

    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Value::Int(i) => out.extend_from_slice(format!("i{i}e").as_bytes()),
            Value::Bytes(s) => {
                out.extend_from_slice(format!("{}:", s.len()).as_bytes());
                out.extend_from_slice(s);
            }
            Value::List(items) => {
                out.push(b'l');
                items.iter().for_each(|item| item.write(out));
                out.push(b'e');
            }
            Value::Dict(dict) => {
                out.push(b'd');
                for (k, v) in dict {
                    Value::Bytes(k).write(out);
                    v.write(out);
                }
                out.push(b'e');
            }
        }
    }
}

struct Reader<'a> {
    raw: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn peek(&self) -> Option<u8> {
        self.raw.get(self.at).copied()
    }

    fn value(&mut self, depth: usize) -> Option<Value<'a>> {
        if depth > MAX_DEPTH {
            return None;
        }
        match self.peek()? {
            b'i' => {
                self.at += 1;
                self.number_until(b'e').map(Value::Int)
            }
            b'l' => {
                self.at += 1;
                let mut items = vec![];
                while self.peek()? != b'e' {
                    items.push(self.value(depth + 1)?);
                }
                self.at += 1;
                Some(Value::List(items))
            }
            b'd' => {
                self.at += 1;
                let mut dict = Dict::new();
                while self.peek()? != b'e' {
                    let key = self.bytes()?;
                    let value = self.value(depth + 1)?;
                    dict.insert(key, value);
                }
                self.at += 1;
                Some(Value::Dict(dict))
            }
            b'0'..=b'9' => self.bytes().map(Value::Bytes),
            _ => None,
        }
    }

    /// The decimal number from here to `end`, both consumed
    fn number_until(&mut self, end: u8) -> Option<i64> {
        let rest = &self.raw[self.at..];
        let len = rest.iter().position(|&b| b == end)?;
        let digits = &rest[..len];
        self.at += len + 1;
        // i64's parser takes a leading `+`; bencode doesn't
        if digits.first() == Some(&b'+') {
            return None;
        }
        std::str::from_utf8(digits).ok()?.parse().ok()
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.number_until(b':')?).ok()?;
        let end = self.at.checked_add(len)?;
        let bytes = self.raw.get(self.at..end)?;
        self.at = end;
        Some(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_every_kind_of_value_and_writes_it_back() {
        let raw = b"d1:ai-42e1:bl3:fooi0ee1:cd1:xdeee";
        let dict = parse_dict(raw).unwrap();
        assert_eq!(dict[b"a".as_slice()], Value::Int(-42));
        assert_eq!(
            dict[b"b".as_slice()],
            Value::List(vec![Value::Bytes(b"foo"), Value::Int(0)])
        );
        assert_eq!(Value::Dict(dict).encode(), raw);
    }

    #[test]
    fn unsorted_keys_are_taken_and_the_last_of_a_repeated_one_wins() {
        let dict = parse_dict(b"d1:bi1e1:ai2e1:bi3ee").unwrap();
        assert_eq!(dict.len(), 2);
        assert_eq!(dict[b"b".as_slice()], Value::Int(3));
    }

    #[test]
    fn malformed_input_is_none_not_a_panic() {
        let deep = format!("d1:a{}{}e", "l".repeat(100), "e".repeat(100));
        let cases: &[&[u8]] = &[
            b"",
            b"d",
            b"de trailing",
            b"li1ee",
            b"d1:ai99999999999999999999ee",
            b"d1:ai1",
            b"d1:a99999999999999999999999:xe",
            b"d1:a5:abce",
            b"d1:a-1:e",
            b"d1:ai+1ee",
            b"d1:ai1x2ee",
            b"di1ei2ee",
            b"d1:axe",
            deep.as_bytes(),
        ];
        for raw in cases {
            assert_eq!(parse_dict(raw), None, "{}", String::from_utf8_lossy(raw));
        }
    }
}
