use std::{
    process::Command,
};

fn main() {
    let output = Command::new("git").args(["rev-parse", "HEAD"]).output().unwrap();
    let git_hash = String::from_utf8(output.stdout).unwrap();
    let rustc_version = compile_time::rustc_version_str!();
    let compile_datetime = compile_time::datetime_str!();

    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
    println!("cargo:rustc-env=CARGO_CFG_FEATURE={}", std::env::var("CARGO_CFG_FEATURE").unwrap());
    println!("cargo:rustc-env=RUSTC_VERSION={}", rustc_version);
    println!("cargo:rustc-env=COMPILE_DATETIME={}", compile_datetime);

    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs");
    println!("cargo:rerun-if-changed=build.rs");
}
