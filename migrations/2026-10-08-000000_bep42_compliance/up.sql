-- BEP 42: whether the node's id matches its address; null until computed (at the next start for
-- rows that predate this)
alter table node add column bep42 boolean;
