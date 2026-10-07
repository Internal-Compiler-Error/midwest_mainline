-- when a node last answered a query of ours: unread, since `last_contacted` (when we last heard
-- from it at all) decides when a failing node is refreshed
alter table node drop column last_sent;
