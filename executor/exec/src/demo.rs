use exec::evaluate;

fn main() {
    let mut args = std::env::args();
    args.next().expect("program name");
    let file =
        std::fs::File::open(args.next().expect("should provide file name").as_str()).unwrap();
    let result = evaluate(file);
    eprintln!("result: {result:?}");
}
