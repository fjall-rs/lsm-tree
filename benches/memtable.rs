use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{DefaultUserComparator, InternalValue, Memtable, SharedComparator, MAX_SEQNO};
use nanoid::nanoid;
use std::sync::Arc;

fn default_cmp() -> SharedComparator {
    Arc::new(DefaultUserComparator)
}

fn memtable_get_hit(c: &mut Criterion) {
    let memtable = Memtable::new(0, default_cmp());

    memtable.insert(InternalValue::from_components(
        "abc_w5wa35aw35naw",
        vec![1, 2, 3],
        0,
        lsm_tree::ValueType::Value,
    ));

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
            assert_eq!(
                [1, 2, 3],
                &*memtable.get(b"abc_w5wa35aw35naw", MAX_SEQNO).unwrap().value,
            )
        });
    });
}

fn memtable_get_snapshot(c: &mut Criterion) {
    let memtable = Memtable::new(0, default_cmp());

    memtable.insert(InternalValue::from_components(
        "abc_w5wa35aw35naw",
        vec![1, 2, 3],
        0,
        lsm_tree::ValueType::Value,
    ));
    memtable.insert(InternalValue::from_components(
        "abc_w5wa35aw35naw",
        vec![1, 2, 3, 4],
        1,
        lsm_tree::ValueType::Value,
    ));

    for _ in 0..1_000_000 {
        memtable.insert(InternalValue::from_components(
            format!("abc_{}", nanoid!()).as_bytes(),
            vec![],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    c.bench_function("memtable get snapshot", |b| {
        b.iter(|| {
            assert_eq!(
                [1, 2, 3],
                &*memtable.get(b"abc_w5wa35aw35naw", 1).unwrap().value,
            );
        });
    });
}

fn memtable_get_miss(c: &mut Criterion) {
    let memtable = Memtable::new(0, default_cmp());

    for _ in 0..1_000_000 {
        memtable.insert(InternalValue::from_components(
            format!("abc_{}", nanoid!()).as_bytes(),
            vec![],
            0,
            lsm_tree::ValueType::Value,
        ));
    }

    c.bench_function("memtable get miss", |b| {
        b.iter(|| assert!(memtable.get(b"abc_564321", MAX_SEQNO).is_none()));
    });
}

fn memtable_highest_seqno(c: &mut Criterion) {
    c.bench_function("memtable highest seqno", |b| {
        let memtable = Memtable::new(0, default_cmp());

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

criterion_group!(
    benches,
    memtable_get_hit,
    memtable_get_snapshot,
    memtable_get_miss,
    memtable_highest_seqno
);
criterion_main!(benches);
