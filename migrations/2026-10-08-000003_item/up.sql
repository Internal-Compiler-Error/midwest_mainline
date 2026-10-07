-- BEP 44: items put to us. Immutable ones have only a value; mutable ones the key, salt,
-- sequence number and signature they were put with.
create table item (
    target blob not null primary key,
    value blob not null, -- bencoded
    key blob,
    salt blob,
    seq bigint,
    sig blob,
    last_put bigint not null -- unix timestamp in milliseconds
) without rowid;
