struct _CC;

impl _CC {
    fn _cc() {
        let _ = 1 + 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn bench_cc(b: &mut Bencher) {
        b.iter(_CC::_cc);
    }
}
