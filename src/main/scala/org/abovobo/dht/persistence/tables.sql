create schema ipv4;
create schema ipv6;

-- ==============================
-- stores an id of operating node
-- ==============================
create table ipv4.self(id binary(40) primary key);
create table ipv6.self(id binary(40) primary key);

-- ==========================
-- represents bucket of nodes
-- ==========================
create table ipv4.bucket(

    -- starting value of bucket (Integer160)
    id binary(40) primary key,

    -- a time when the bucket has last been touched
    seen timestamp not null
);
create table ipv6.bucket(

    -- starting value of bucket (Integer160)
    id binary(40) primary key,

    -- a time when the bucket has last been touched
    seen timestamp not null
);

-- ===========================
-- represents actual node info
-- ===========================
create table ipv4.node(

    -- node id (Integer160)
    id binary(40) primary key,

    -- bucket id owning this node (Integer160)
    -- note that `bucket` value must always be less than `id`
    -- db does not force check constraint as it must be managed by code
    bucket binary(40) not null,

    -- specifies that `bucket` references actual record in corresponding table
    foreign key(bucket) references bucket(id) on delete cascade on update restrict,

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
create table ipv6.node(

    -- node id (Integer160)
    id binary(40) primary key,

    -- bucket id owning this node (Integer160)
    -- note that `bucket` value must always be less than `id`
    -- db does not force check constraint as it must be managed by code
    bucket binary(40) not null,

    -- specifies that `bucket` references actual record in corresponding table
    foreign key(bucket) references bucket(id) on delete cascade on update restrict,

    -- address of node and port for UDP communications compacted into a byte array
    address binary(18) not null,

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
-- represents peer info
-- ====================
create table ipv4.peer(

    -- an infohash of the content this peer is on
    infohash binary(40) not null,

    -- an actual address (IP/port) of the peer
    address binary(6) not null,

    -- the combination of infohash and address is a primary key of this record
    primary key (infohash, address),

    -- a time when this peer has been announced last
    announced timestamp not null
);
create table ipv6.peer(

    -- an infohash of the content this peer is on
    infohash binary(40) not null,

    -- an actual address (IP/port) of the peer
    address binary(18) not null,

    -- the combination of infohash and address is a primary key of this record
    primary key (infohash, address),

    -- a time when this peer has been announced last
    announced timestamp not null
);