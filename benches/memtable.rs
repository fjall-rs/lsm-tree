use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{InternalValue, Memtable};
use nanoid::nanoid;

fn memtable_get(c: &mut Criterion) {
    let memtable = Memtable::default();

    memtable.insert(InternalValue::from_components(
        "abc_w5wa35aw35naw",
        vec![],
        0,
        lsm_tree::ValueType::Value,
    ));

    for _ in 0..100_000 {
        memtable.insert(InternalValue::from_components(
            format!("abc_{}", nanoid!()).as_bytes(),
            vec![],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    c.bench_function("memtable get", |b| {
        b.iter(|| {
            memtable.get("abc_w5wa35aw35naw", None);
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

criterion_group!(benches, memtable_get, memtable_highest_seqno);
criterion_main!(benches);
