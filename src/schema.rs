// @generated automatically by Diesel CLI.

diesel::table! {
    misc (key) {
        key -> Text,
        value -> Text,
    }
}

diesel::table! {
    node (id) {
        id -> Binary,
        bucket -> Integer,
        last_contacted -> BigInt,
        ip_addr -> Text,
        port -> Integer,
        failed_requests -> Integer,
        removed -> Bool,
        last_sent -> Nullable<BigInt>,
        added -> BigInt,
    }
}

diesel::table! {
    peer (ip_addr, port, swarm) {
        ip_addr -> Text,
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

diesel::table! {
    txn_id (id) {
        id -> Binary,
        next_txn_id -> Integer,
    }
}

diesel::joinable!(peer -> swarm (swarm));

diesel::allow_tables_to_appear_in_same_query!(
    misc,
    node,
    peer,
    swarm,
    txn_id,
);
