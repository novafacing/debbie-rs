use cc::Build;
use std::path::PathBuf;

fn main() {
    let ts_c_dir: PathBuf = PathBuf::from("tree-sitter-c");
    let ts_c_src = ts_c_dir.join("src");

    Build::new()
        .include(&ts_c_src)
        .file(ts_c_src.join("parser.c"))
        .compile("tree-sitter-c");
}
