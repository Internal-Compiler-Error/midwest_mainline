create table node_old(
    id blob not null primary key,
    bucket int not null,
    last_contacted bigint not null,
    ip_addr text not null,
    port int not null,
    failed_requests int not null,
    removed boolean not null default FALSE,
    last_sent bigint,
    added bigint not null default (unixepoch('subsec') * 1000)
);

insert into node_old (id, bucket, last_contacted, ip_addr, port, failed_requests, removed, last_sent, added)
select id, bucket, last_contacted, ip_addr, port, failed_requests, removed, last_sent, added from node where family = 4;

drop table node;
alter table node_old rename to node;

create index 'idx-node-bucket-on-alive-nodes' on node (bucket) where removed = FALSE;
create index 'idx-node-bucket-removed-last_contacted' on node(bucket, removed, last_contacted);
