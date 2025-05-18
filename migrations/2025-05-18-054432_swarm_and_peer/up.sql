create table swarm (
    info_hash blob primary key
) without ROWID;

create table peer (
    ip_addr bigint not null, -- unsigned 32 bit integer of an ipv4 port
    port integer not null, -- unsigned 16 bit integer port
    last_announced bigint not null, -- unix timestamp in milliseconds
    swarm blob not null references swarm(info_hash) on delete cascade on update cascade,
    primary key(ip_addr, port, swarm)
) WITHOUT ROWID;

create view peer_with_human_date as
select
    ip_addr,
    port,
    datetime(last_announced / 1000, 'unixepoch') as last_announced,
    swarm
from peer;

create index 'peer-swarm-last_announced' on peer(swarm, last_announced);

