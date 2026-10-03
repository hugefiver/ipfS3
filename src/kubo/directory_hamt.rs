//! UnixFS HAMTShard fanout-256 addressing (Boxo's Murmur3 x64-128, seed 0).
//! MurmurHash3 is in the public domain: Austin Appleby,
//! https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp.
use std::collections::BTreeMap;

use super::{BuildResult, DirectoryBuildError, Link};

pub(super) struct Shard {
    pub(super) bitmap: Vec<u8>,
    pub(super) links: Vec<ShardLink>,
}

pub(super) enum ShardLink {
    File(Link),
    Child { prefix: String, index: usize },
}

struct HashedLink {
    link: Link,
    hash: [u8; 8],
}

pub(super) fn shards(links: Vec<Link>) -> BuildResult<Vec<Shard>> {
    let entries = links
        .into_iter()
        .map(|link| HashedLink {
            hash: murmur3_x64_64(link.name.as_bytes()),
            link,
        })
        .collect();
    let mut nodes = Vec::new();
    let root = insert(entries, 0, &mut nodes)?;
    debug_assert_eq!(root, nodes.len() - 1);
    Ok(nodes)
}

fn insert(entries: Vec<HashedLink>, depth: usize, nodes: &mut Vec<Shard>) -> BuildResult<usize> {
    if depth == 8 {
        return Err(DirectoryBuildError::HashCollision);
    }
    let mut buckets = BTreeMap::<u8, Vec<HashedLink>>::new();
    for entry in entries {
        buckets.entry(entry.hash[depth]).or_default().push(entry);
    }
    let mut bits = [0_u8; 32];
    let mut links = Vec::with_capacity(buckets.len());
    for (bucket, mut group) in buckets {
        bits[31 - usize::from(bucket / 8)] |= 1 << (bucket % 8);
        let prefix = format!("{bucket:02X}");
        if group.len() == 1 {
            let mut link = group.pop().expect("single bucket entry").link;
            link.name.insert_str(0, &prefix);
            links.push(ShardLink::File(link));
        } else {
            let index = insert(group, depth + 1, nodes)?;
            links.push(ShardLink::Child { prefix, index });
        }
    }
    links.sort_by(|a, b| name(a).as_bytes().cmp(name(b).as_bytes()));
    let first = bits.iter().position(|byte| *byte != 0).unwrap_or(32);
    // Each path can create at most eight shard levels. The builder's 10k
    // manifest cap therefore also bounds the number of dag/put metadata RPCs.
    if nodes.len() >= 8 * super::MAX_FILES {
        return Err(DirectoryBuildError::BlockTooLarge);
    }
    nodes.push(Shard {
        bitmap: bits[first..].to_vec(),
        links,
    });
    Ok(nodes.len() - 1)
}

fn name(link: &ShardLink) -> &str {
    match link {
        ShardLink::File(link) => &link.name,
        ShardLink::Child { prefix, .. } => prefix,
    }
}

pub(super) fn data(bitmap: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0x08, 0x05, 0x12]; // Type = HAMTShard, Data = bitmap
    // At fanout 256 the bitmap is never longer than 32 bytes.
    bytes.push(bitmap.len() as u8);
    bytes.extend_from_slice(bitmap);
    bytes.extend_from_slice(&[0x28, 0x22, 0x30, 0x80, 0x02]); // hashType=0x22, fanout=256
    bytes
}

// MurmurHash3_x64_128's h1, serialized big-endian for Boxo's bucket iterator.
fn murmur3_x64_64(key: &[u8]) -> [u8; 8] {
    const C1: u64 = 0x87c37b91114253d5;
    const C2: u64 = 0x4cf5ad432745937f;
    let (mut h1, mut h2) = (0_u64, 0_u64);
    let (chunks, tail) = key.as_chunks::<16>();
    for chunk in chunks {
        let mut k1 = u64::from_le_bytes(chunk[..8].try_into().expect("eight bytes"));
        let mut k2 = u64::from_le_bytes(chunk[8..].try_into().expect("eight bytes"));
        k1 = k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
        h1 ^= k1;
        h1 = h1.rotate_left(27).wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);
        k2 = k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
        h2 ^= k2;
        h2 = h2.rotate_left(31).wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
    }
    let mut k1 = 0_u64;
    let mut k2 = 0_u64;
    for (i, byte) in tail.iter().enumerate() {
        if i < 8 {
            k1 |= u64::from(*byte) << (i * 8);
        } else {
            k2 |= u64::from(*byte) << ((i - 8) * 8);
        }
    }
    if tail.len() > 8 {
        h2 ^= k2.wrapping_mul(C2).rotate_left(33).wrapping_mul(C1);
    }
    if !tail.is_empty() {
        h1 ^= k1.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
    }
    let len = key.len() as u64;
    h1 ^= len;
    h2 ^= len;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);
    h1 = fmix(h1);
    h2 = fmix(h2);
    h1.wrapping_add(h2).to_be_bytes()
}

fn fmix(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^ (hash >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boxo_murmur_golden_and_bitmap_order() {
        assert_eq!(
            murmur3_x64_64(b"hello"),
            0xcbd8a7b341bd9b02_u64.to_be_bytes()
        );
        let mut bits = [0_u8; 32];
        for bucket in [0, 7, 8] {
            bits[31 - bucket / 8] |= 1 << (bucket % 8);
        }
        assert_eq!(&bits[30..], &[0x01, 0x81]);
        assert_eq!(data(&bits[30..]), [8, 5, 18, 2, 1, 129, 40, 34, 48, 128, 2]);
    }

    #[test]
    fn collision_at_all_eight_levels_fails_explicitly() {
        let file = |name: &str| HashedLink {
            link: Link {
                name: name.to_owned(),
                cid: cid::Cid::try_from(
                    "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
                )
                .unwrap(),
                size: 0,
            },
            hash: [0xCB; 8],
        };
        let error = insert(vec![file("a"), file("b")], 0, &mut Vec::new()).unwrap_err();
        assert!(matches!(error, DirectoryBuildError::HashCollision));
    }
}
