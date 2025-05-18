// @generated automatically by Diesel CLI.

diesel::table! {
    peer (ip_addr, port, swarm) {
        ip_addr -> BigInt,
        port -> Integer,
        last_announced -> BigInt,
        swarm -> Binary,
    }
}

diesel::table! {
    swarm (info_hash) {
        info_hash -> Binary,
    }
}

diesel::joinable!(peer -> swarm (swarm));

diesel::allow_tables_to_appear_in_same_query!(
    peer,
    swarm,
);
