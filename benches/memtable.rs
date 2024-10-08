use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{InternalValue, Memtable};
use nanoid::nanoid;

fn memtable_get_upper_bound(c: &mut Criterion) {
    let memtable = Memtable::default();

    for _ in 0..1_000_000 {
        memtable.insert(InternalValue::from_components(
            format!("abc_{}", nanoid!()).as_bytes(),
            vec![],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    c.bench_function("memtable get", |b| {
        b.iter(|| {
            memtable.get("abc", None);
        });
    });
}
criterion_group!(benches, memtable_get_upper_bound);
criterion_main!(benches);
