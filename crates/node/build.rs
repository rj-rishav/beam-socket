fn main() {
    // napi-build wires the linker flags a Node addon needs (e.g. allowing
    // napi_* symbols to stay undefined until the addon is loaded by Node).
    // Only relevant when the `napi` feature is on; without it this crate is a
    // plain rlib and `cargo test --workspace` must stay link-clean (CI).
    if std::env::var_os("CARGO_FEATURE_NAPI").is_some() {
        napi_build::setup();
    }
}
