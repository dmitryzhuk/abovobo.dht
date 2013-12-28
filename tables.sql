-- ==============================
-- stores an id of operating node
-- ==============================
create table self(id binary(40) primary key);

-- ==========================
-- represents bucket of nodes
-- ==========================
create table bucket(

    -- starting value of bucket (Integer160)
    id binary(40) primary key,

    -- a time when the bucket has last been touched
    seen timestamp not null
);

-- ===========================
-- represents actual node info
-- ===========================
create table node(

    -- node id (Integer160)
    id binary(40) primary key,

    -- bucket id owning this node (Integer160)
    -- note that `bucket` value must always be less than `id`
    -- db does not force check constraint as it must be managed by code
    bucket binary(40) not null,

    -- specifies that `bucket` references actual record in corresponding table
    foreign key(bucket) references bucket(id) on delete cascade on update restrict,

    -- IPv4 address of node and port for UDP communications compacted into a byte array
    ipv4u binary(6),

    -- IPv4 address of node and port for TCP communications compacted into a byte array
    ipv4t binary(6),

    -- IPv6 address of node and port for UDP communications compacted into a byte array
    ipv6u binary(8),

    -- IPv6 address of node and port for TCP communications compacted into a byte array
    ipv6t binary(8),

    -- at least one endpoint must be presented
    check ipv4u is not null or ipv4t is not null or ipv6u is not null or ipv6t is not null,

    -- a time when the node has last replied to our query
    replied timestamp,

    -- a time when the node has last queried us
    queried timestamp,

    -- ensure that node at least replied or queried, both cannot be null
    check replied is not null or queried is not null,

    -- number of times node failed to respond to query
    failcount int not null default 0
);
