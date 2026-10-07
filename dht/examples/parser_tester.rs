use color_eyre::{Result, eyre::Ok};
use midwest_mainline::message::Krpc;
use std::io::{self, Read};

fn main() -> Result<()> {
    color_eyre::install()?;

    let mut buf: Vec<u8> = vec![];

    let mut stdin = io::stdin().lock();
    stdin.read_to_end(&mut buf)?;

    println!("Read {} bytes", buf.len());
    let msg = Krpc::decode(&buf)?;
    println!("{:#?}", msg);

    Ok(())
}
