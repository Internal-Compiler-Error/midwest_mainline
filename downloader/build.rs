use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    // The TCP_INFO bindings only exist on Linux; skip generation elsewhere so
    // the crate still builds on macOS.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    if target_os != "linux" {
        return;
    }

    println!("cargo:rustc-link-search=/usr/lib/");
    println!("cargo:rustc-link-lib=c");

    let bindings = bindgen::Builder::default()
        .header("/usr/include/linux/tcp.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from("src");
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
