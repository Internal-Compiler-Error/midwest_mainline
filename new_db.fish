#!/bin/env fish

rm -rvf dht.db dht.db-wal dht.db-shm
touch dht.db
diesel setup
litecli -e "PRAGMA journal_mode = WAL" dht.db
