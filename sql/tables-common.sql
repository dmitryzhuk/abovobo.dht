-- ===================================================
-- Abovobo DHT Implementation
--
-- This file is provided under terms and conditions of
-- Eclipse Public License v. 1.0
-- http://www.opensource.org/licenses/eclipse-1.0
--
-- Developed by Dmitry Zhuk for Abovobo project.
-- ===================================================
-- This file contains tables, which are neutral to the
-- size of IP addresses, as they do not store them.
-- ===================================================

-- ==============================
-- Stores an id of operating node
-- ==============================
create table self(id binary(40) primary key);

-- ==========================
-- Represents bucket of nodes
-- ==========================
create table bucket(

    -- starting value of bucket (Integer160)
    id binary(40) primary key,

    -- a time when the bucket has last been touched
    seen timestamp not null
);

