use color_eyre::{eyre::Ok, Result};
use midwest_mainline::message::ParseKrpc;
use std::io::{self, Read};

fn main() -> Result<()> {
    color_eyre::install()?;

    let mut buf: Vec<u8> = vec![];

    let mut stdin = io::stdin().lock();
    stdin.read_to_end(&mut buf)?;

    println!("Read {} bytes", buf.len());
    let msg = buf.as_slice().parse()?;
    println!("{:#?}", msg);

    Ok(())
}
