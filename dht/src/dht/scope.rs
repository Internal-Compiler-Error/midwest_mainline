//! Where in the database a node keeps what is its own: its identity, the external address
//! other nodes see, and its routing table. A host normally runs one node per address family
//! (BEP 32), each in its family's scope. BEP 45's multiple-address operation runs more, one per
//! global address of the host: each needs a node id of its own and a routing table built around
//! that id, so each gets a scope of its own, keyed by its address. The announced peers they
//! store are shared; to the rest of the network they're separate nodes.

use std::net::IpAddr;

use diesel::SqliteConnection;

use crate::types::Family;
use crate::utils::{db_get, db_put};

/// The first routing table number handed to an address of its own; 4 and 6 are the families'
const FIRST_OWN_TABLE: i32 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scope {
    pub(crate) family: Family,
    /// the `node` table's `family` column: 4 or 6, or a number of the address's own
    pub(crate) table: i32,
    /// what `misc` keys get: nothing (IPv4) or `6` for the families, `@<address>` otherwise
    suffix: String,
}

impl Scope {
    pub(crate) fn primary(family: Family) -> Scope {
        Scope {
            family,
            table: family.db(),
            suffix: match family {
                Family::V4 => String::new(),
                Family::V6 => "6".to_string(),
            },
        }
    }

    /// The scope of a node bound to `ip`, numbered the first time it's asked for
    pub(crate) fn of_address(ip: IpAddr, conn: &mut SqliteConnection) -> Result<Scope, diesel::result::Error> {
        let suffix = format!("@{ip}");
        conn.immediate_transaction(|conn| {
            let key = format!("table{suffix}");
            let table = match db_get(&key, conn)?.and_then(|t| t.parse().ok()) {
                Some(table) => table,
                None => {
                    let table = db_get("next_table", conn)?
                        .and_then(|t| t.parse().ok())
                        .unwrap_or(FIRST_OWN_TABLE);
                    db_put("next_table".to_string(), (table + 1).to_string(), conn)?;
                    db_put(key, table.to_string(), conn)?;
                    table
                }
            };
            Ok(Scope {
                family: Family::of_ip(&ip),
                table,
                suffix,
            })
        })
    }

    /// The `misc` key `base` goes by in this scope
    pub(crate) fn key(&self, base: &str) -> String {
        format!("{base}{}", self.suffix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::memory_pool;

    #[test]
    fn each_address_gets_a_table_and_keys_of_its_own_for_good() {
        let pool = memory_pool();
        let mut conn = pool.get().unwrap();
        assert_eq!(Scope::primary(Family::V4).key("id"), "id");
        assert_eq!(Scope::primary(Family::V6).key("id"), "id6");

        let a: IpAddr = "2001:470:1:2::5".parse().unwrap();
        let b: IpAddr = "2001:470:9:9::5".parse().unwrap();
        let scope_a = Scope::of_address(a, &mut conn).unwrap();
        let scope_b = Scope::of_address(b, &mut conn).unwrap();
        assert_eq!((scope_a.table, scope_b.table), (100, 101));
        assert_eq!(scope_a.family, Family::V6);
        assert_eq!(scope_a.key("id"), "id@2001:470:1:2::5");
        assert_eq!(Scope::of_address(a, &mut conn).unwrap(), scope_a);
    }
}
