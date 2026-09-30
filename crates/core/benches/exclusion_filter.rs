//! Compare exclusion strategies, including setup/drop on every broadcast.
//! Run: cargo bench -p beamsocket-core --bench exclusion_filter
//! CSV on stdout; medians are for exploration, never a shared-CI timing gate.
use std::collections::HashSet;
use std::hint::black_box;
use std::time::Instant;

use beamsocket_core::ids::ConnectionId;

#[inline(never)]
fn linear(ids: &[ConnectionId], except: &[ConnectionId]) -> usize {
    ids.iter().filter(|id| !except.contains(id)).count()
}

#[inline(never)]
fn sorted(ids: &[ConnectionId], except: &[ConnectionId]) -> usize {
    let mut index = except.to_vec();
    index.sort_unstable_by_key(|id| id.0);
    ids.iter()
        .filter(|id| index.binary_search_by_key(&id.0, |x| x.0).is_err())
        .count()
}

#[inline(never)]
fn hashed(ids: &[ConnectionId], except: &[ConnectionId]) -> usize {
    let index: HashSet<_> = except.iter().copied().collect();
    ids.iter().filter(|id| !index.contains(id)).count()
}

#[inline(never)]
fn adaptive(ids: &[ConnectionId], except: &[ConnectionId]) -> usize {
    if except.len() >= 32 && ids.len() >= 64 && ids.len() >= except.len() {
        sorted(ids, except)
    } else {
        linear(ids, except)
    }
}

fn main() {
    type Filter = fn(&[ConnectionId], &[ConnectionId]) -> usize;
    let variants: [(&str, Filter); 4] = [
        ("linear", linear),
        ("sorted", sorted),
        ("hashed", hashed),
        ("adaptive", adaptive),
    ];
    println!("recipients,exclusions,pattern,variant,round,iterations,ns_per_broadcast");
    for (recipients, excludes) in [
        (1, 4096),
        (8, 4096),
        (32, 64),
        (64, 64),
        (128, 64),
        (128, 128),
        (128, 4096),
        (1024, 0),
        (1024, 1),
        (1024, 16),
        (1024, 32),
        (1024, 64),
        (1024, 256),
        (4096, 1024),
        (16384, 4096),
    ] {
        let ids: Vec<_> = (0..recipients)
            .map(|i| ConnectionId::new((i % 16) as u8, 3, (i / 16) as u32))
            .collect();
        for pattern in ["miss", "mixed", "duplicates", "all-hit"] {
            let except: Vec<_> = (0..excludes)
                .map(|i| match pattern {
                    "mixed" if i % 2 == 0 => ids[(i * 17) % ids.len()],
                    "duplicates" => ids[0],
                    "all-hit" => ids[i % ids.len()],
                    _ => ConnectionId::new((i % 16) as u8, 2, (i / 16) as u32),
                })
                .collect();
            let expected = linear(&ids, &except);
            let mut iters = [0; 4];
            for (v, (_, run)) in variants.iter().enumerate() {
                assert_eq!(run(&ids, &except), expected);
                let start = Instant::now();
                for _ in 0..10 {
                    black_box(run(black_box(&ids), black_box(&except)));
                }
                iters[v] = (5_000_000 / (start.elapsed().as_nanos() as usize / 10).max(1))
                    .clamp(2, 100_000);
            }
            // Rotate order to avoid always rewarding the same thermal/cache state.
            for round in 0..7 {
                for step in 0..4 {
                    let v = (round + step) % 4;
                    let (name, run) = variants[v];
                    let start = Instant::now();
                    for _ in 0..iters[v] {
                        black_box(run(black_box(&ids), black_box(&except)));
                    }
                    let ns = start.elapsed().as_nanos() as f64 / iters[v] as f64;
                    println!(
                        "{recipients},{excludes},{pattern},{name},{round},{},{ns:.2}",
                        iters[v]
                    );
                }
            }
        }
    }
}
