//! Table ID assignments for Reth tables.
//!
//! Each Reth table gets a unique prefix byte, mapping the strongly-typed
//! table system to ClawStore's flat namespace.

use reth_db_api::table::Table;

/// Get the table ID prefix byte for a Reth table by name.
///
/// Returns a unique u8 for each known Reth table. Unknown tables
/// get a hash-based ID to avoid collisions.
pub fn table_id_for_name(name: &str) -> u8 {
    match name {
        "CanonicalHeaders" => 0x01,
        "HeaderNumbers" => 0x02,
        "Headers" => 0x03,
        "BlockBodyIndices" => 0x04,
        "BlockOmmers" => 0x05,
        "BlockWithdrawals" => 0x06,
        "Transactions" => 0x07,
        "TransactionHashNumbers" => 0x08,
        "TransactionBlocks" => 0x09,
        "TransactionSenders" => 0x0A,
        "Receipts" => 0x0B,
        "PlainAccountState" => 0x0C,
        "PlainStorageState" => 0x0D,
        "Bytecodes" => 0x0E,
        "AccountsTrie" => 0x0F,
        "StoragesTrie" => 0x10,
        "HashedAccounts" => 0x11,
        "HashedStorages" => 0x12,
        "AccountsHistory" => 0x13,
        "StoragesHistory" => 0x14,
        "AccountChangeSets" => 0x15,
        "StorageChangeSets" => 0x16,
        "StageCheckpoints" => 0x17,
        "StageCheckpointProgresses" => 0x18,
        "PruneCheckpoints" => 0x19,
        "VersionHistory" => 0x1A,
        "ChainState" => 0x1B,
        "Metadata" => 0x1C,
        _ => {
            // Hash-based fallback for unknown tables
            let mut hash: u8 = 0xF0;
            for b in name.bytes() {
                hash = hash.wrapping_add(b);
            }
            // Ensure we're in the 0xE0..=0xFF range to avoid collisions
            0xE0 | (hash & 0x1F)
        }
    }
}

/// Build a prefixed key: `[table_id][encoded_key_bytes]`
pub fn prefixed_key<T: Table>(key_bytes: &[u8]) -> Vec<u8> {
    let table_id = table_id_for_name(T::NAME);
    let mut prefixed = Vec::with_capacity(1 + key_bytes.len());
    prefixed.push(table_id);
    prefixed.extend_from_slice(key_bytes);
    prefixed
}

/// Extract the raw key bytes from a prefixed key (strips the table ID byte).
pub fn strip_prefix(prefixed: &[u8]) -> &[u8] {
    if prefixed.is_empty() { prefixed } else { &prefixed[1..] }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_tables_unique() {
        let tables = [
            "CanonicalHeaders", "HeaderNumbers", "Headers",
            "BlockBodyIndices", "BlockOmmers", "BlockWithdrawals",
            "Transactions", "TransactionHashNumbers", "TransactionBlocks",
            "TransactionSenders", "Receipts", "PlainAccountState",
            "PlainStorageState", "Bytecodes", "AccountsTrie",
            "StoragesTrie", "HashedAccounts", "HashedStorages",
            "AccountsHistory", "StoragesHistory", "AccountChangeSets",
            "StorageChangeSets", "StageCheckpoints", "StageCheckpointProgresses",
            "PruneCheckpoints", "VersionHistory", "ChainState", "Metadata",
        ];
        let mut ids: Vec<u8> = tables.iter().map(|t| table_id_for_name(t)).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), tables.len(), "Table IDs must be unique");
    }

    #[test]
    fn test_prefixed_key() {
        let key = b"test_key";
        let prefixed = {
            let table_id = table_id_for_name("PlainAccountState");
            let mut p = Vec::with_capacity(1 + key.len());
            p.push(table_id);
            p.extend_from_slice(key);
            p
        };
        assert_eq!(prefixed[0], 0x0C);
        assert_eq!(strip_prefix(&prefixed), key.as_slice());
    }
}
