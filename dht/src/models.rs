use diesel::prelude::*;

#[derive(Queryable, Selectable, AsChangeset, Insertable)]
#[diesel(table_name = crate::schema::node)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct NodeRow {
    pub id: Vec<u8>,
    pub family: i32,
    pub bucket: i32,
    pub last_contacted: i64,
    pub ip_addr: String,
    pub ip_group: Option<String>,
    pub port: i32,
    pub failed_requests: i32,
    pub removed: bool,
    pub bep42: Option<bool>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::node)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct NodeNoMetaInfo {
    pub id: Vec<u8>,
    pub ip_addr: String,
    pub port: i32,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::misc)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct MiscVal {
    pub value: String,
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::schema::misc)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct Misc {
    pub key: String,
    pub value: String,
}
