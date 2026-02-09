CREATE USER wal_user WITH REPLICATION LOGIN PASSWORD 'super_password';
GRANT pg_read_all_wal TO wal_user;