-- BEP 32: one routing table per address family. Many nodes run both with the same id, so the
-- family is part of the key.
create table node_new(
    id blob not null,
    family int not null, -- 4 or 6
    bucket int not null,
    last_contacted bigint not null,
    ip_addr text not null,
    -- one node per address: the IPv4 address, or the IPv6 /64; null where the rule doesn't apply (LAN)
    ip_group text,
    port int not null,
    failed_requests int not null,
    removed boolean not null default FALSE,
    last_sent bigint,
    added bigint not null default (unixepoch('subsec') * 1000),
    primary key(family, id)
);

insert into node_new (id, family, bucket, last_contacted, ip_addr, ip_group, port, failed_requests, removed, last_sent, added)
select id, 4, bucket, last_contacted, ip_addr, ip_addr, port, failed_requests, removed, last_sent, added from node;

drop table node;
alter table node_new rename to node;

create index 'idx-node-bucket-on-alive-nodes' on node (family, bucket) where removed = FALSE;
create index 'idx-node-bucket-removed-last_contacted' on node(family, bucket, removed, last_contacted);
create index 'idx-node-ip_group' on node(family, ip_group) where removed = FALSE;
