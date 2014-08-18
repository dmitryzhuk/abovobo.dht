-- ===================================================
-- Abovobo DHT Implementation
--
-- This file is provided under terms and conditions of
-- Eclipse Public License v. 1.0
-- http://www.opensource.org/licenses/eclipse-1.0
--
-- Developed by Dmitry Zhuk for Abovobo project.
-- ===================================================
-- This file contains tables, which are capable of
-- storing IPv4 addresses.
-- ===================================================

-- ===========================
-- Represents actual node info
-- ===========================
create table node(

    -- node id (Integer160)
    id binary(40) primary key,

    -- address of node and port for UDP communications compacted into a byte array
    address binary(6) not null,

    -- a time when the node has last replied to our query
    replied timestamp,

    -- a time when the node has last queried us
    queried timestamp,

    -- ensure that node at least replied or queried, both cannot be null
    check replied is not null or queried is not null,

    -- number of times node failed to respond to query
    failcount int not null default 0
);

-- ====================
-- Represents peer info
-- ====================
create table peer(

    -- an infohash of the content this peer is on
    infohash binary(40) not null,

    -- an actual address (IP/port) of the peer
    address binary(6) not null,

    -- the combination of infohash and address is a primary key of this record
    primary key (infohash, address),

    -- a time when this peer has been announced last
    announced timestamp not null
);
