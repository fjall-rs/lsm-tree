use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{InternalValue, Memtable};
use nanoid::nanoid;

fn memtable_get_upper_bound(c: &mut Criterion) {
    c.bench_function("memtable get", |b| {
        let memtable = Memtable::default();

        for _ in 0..1_000_000 {
            memtable.insert(InternalValue::from_components(
                format!("abc_{}", nanoid!()).as_bytes(),
                vec![],
                0,
                lsm_tree::ValueType::Value,
            ));
        }

        b.iter(|| {
            memtable.get("abc", None);
        });
    });
}

fn memtable_highest_seqno(c: &mut Criterion) {
    c.bench_function("memtable highest seqno", |b| {
        let memtable = Memtable::default();

        for x in 0..100_000 {
            memtable.insert(InternalValue::from_components(
                format!("abc_{}", nanoid!()).as_bytes(),
                vec![],
                x,
                lsm_tree::ValueType::Value,
            ));
        }

        b.iter(|| {
            assert_eq!(Some(99_999), memtable.get_highest_seqno());
        });
    });
}

criterion_group!(benches, memtable_get_upper_bound, memtable_highest_seqno);
criterion_main!(benches);
