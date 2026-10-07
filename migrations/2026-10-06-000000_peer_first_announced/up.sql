-- when a peer was first announced to us, so a node kept as a long-term index has the history
alter table peer add column first_announced bigint not null default 0;
update peer set first_announced = last_announced;
