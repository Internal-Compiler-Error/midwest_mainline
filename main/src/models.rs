use diesel::prelude::*;

#[derive(Queryable, Selectable, AsChangeset)]
#[diesel(table_name = crate::schema::swarm)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Swarm {
    pub info_hash: Vec<u8>,
}

#[derive(Queryable, Selectable, AsChangeset)]
#[diesel(table_name = crate::schema::peer)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Peer {
    pub ip_addr: i64, // sqlite refuses to believe the existence of unsigned numbers, so stuff a u32 into a i64
    pub port: i32,    // ditto except it's u16 inside of i32
    pub last_announced: i64, // 63 bits should be enough even for milliseconds
    pub swarm: Vec<u8>,
}
