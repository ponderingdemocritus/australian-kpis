fn main() {
    match au_kpis_openapi::emit() {
        Ok(doc) => print!("{doc}"),
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
