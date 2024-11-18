#[macro_export]
macro_rules! print_tid {
    () => {
        eprintln!(
            "{:?} -> {:?}",
            std::any::type_name::<Self>(),
            std::thread::current().id()
        );
    };
    ($name:tt) => {
        eprintln!("{:?} -> {:?}", $name, std::thread::current().id());
    };
}
