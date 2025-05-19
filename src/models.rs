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
    pub ip_addr: String,
    pub port: i32,
    pub last_announced: i64, // 63 bits should be enough even for milliseconds
    pub swarm: Vec<u8>,
}

#[derive(Queryable, Selectable, AsChangeset, Insertable)]
#[diesel(table_name = crate::schema::node)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Node {
    pub id: Vec<u8>,
    pub last_contacted: i64,
    pub ip_addr: String,
    pub port: i32,
    pub failed_requests: i32,
    pub removed: bool,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::node)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct NodeNoMetaInfo {
    pub id: Vec<u8>,
    pub ip_addr: String,
    pub port: i32,
}
