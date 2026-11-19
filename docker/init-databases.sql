-- One database per Raft node, so nodes never share storage.
-- The tables themselves are created by PostgresRaftStorage on startup.
CREATE DATABASE raft_n1;
CREATE DATABASE raft_n2;
CREATE DATABASE raft_n3;
CREATE DATABASE raft_n4;
CREATE DATABASE raft_n5;
