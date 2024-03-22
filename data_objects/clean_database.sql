/* 
Script to create a new database with different objects.


*/

use role sysadmin;
use warehouse hol_wh;

create database if not exists sauvignon;
create schema if not exists edelzwicker;
create schema if not exists chasselas with managed access;

use role securityadmin;
create role if not exists cepage;
create role if not exists assemblage;

create role if not exists vendange;

create or replace user brian;

grant role assemblage to role cepage;
grant usage on database sauvignon to role assemblage;
grant usage on database sauvignon to role vendange;

grant usage on schema sauvignon.edelzwicker to role assemblage;
grant usage on schema sauvignon.chasselas to role assemblage;

grant usage on schema sauvignon.edelzwicker to role vendange;
grant usage on schema sauvignon.chasselas to role vendange;

grant create table on all schemas in database sauvignon to role assemblage;

grant usage on warehouse compute_wh to role assemblage;
grant all privileges on all tables in schema sauvignon.edelzwicker to role assemblage;
grant all privileges on future tables in schema sauvignon.edelzwicker to role assemblage;

grant all privileges on all tables in schema sauvignon.chasselas to role assemblage;
grant all privileges on future tables in schema sauvignon.chasselas to role assemblage;

grant role cepage to user maarten;
grant role vendange to user maarten;

use role sysadmin;
use database sauvignon; 
use schema edelzwicker;

create table bouchon (
    field01 integer,
    field02 varchar(255)
);

use schema chasselas;

create table if not exists bouteille (
    field01 number(38,0),
    field02 text
);

use role cepage;
use schema edelzwicker;
create table if not exists cave (
    field01 integer,
    field02 varchar(255)
);

use schema chasselas;

create table if not exists degustation (
    field01 number(38,0),
    field02 text
);

-- use role sysadmin;
-- drop database sauvignon;


/*
Check options for different tables;
*/
-- use role  cepage;
-- grant select on table edelzwicker.cave to role vendange;
-- grant select on table chasselas.degustation to role vendange;

-- use role vendange;

-- select *
-- from edelzwicker.cave;


-- select * 
-- from chasselas.degustation;
