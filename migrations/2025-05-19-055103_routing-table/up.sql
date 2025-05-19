create table misc(
    key text not null primary key,
    value text not null
);

create table node(
    id blob not null primary key,
    bucket int not null,
    last_contacted bigint not null,
    ip_addr text not null,
    port int not null,
    failed_requests int not null,
    removed boolean not null default FALSE,
    last_sent bigint, -- nullable for the case of bootstrapping, for the first ping, we don't know the node id yet so we can't lookup direcly
    added bigint not null default (unixepoch('subsec') * 1000)
);

create table `txn_id`(
    id blob not null primary key,
    next_txn_id integer not null default -1
);

create index 'idx-node-bucket-on-alive-nodes' on node (bucket) where removed = FALSE;
create index 'idx-node-bucket-removed-last_contacted' on node(bucket, removed, last_contacted);
