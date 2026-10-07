-- BEP 51: info hashes other nodes said they store, as a crawler collects them
create table sampled_infohash (
    info_hash blob not null primary key,
    first_sampled bigint not null, -- unix timestamp in milliseconds
    last_sampled bigint not null,
    times_sampled int not null default 1
) without rowid;
