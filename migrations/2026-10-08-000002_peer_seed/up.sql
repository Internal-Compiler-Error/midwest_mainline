-- BEP 33: the peer said it's a seed (`seed=1`) in its last announce
alter table peer add column seed boolean not null default false;
