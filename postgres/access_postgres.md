docker exec -it postgres bash
psql -U marcos etl



\l - Display database
\c - Connect to database
\dn - List schemas
\dn+ - List schemas more details
\dt - List tables inside public schemas
\dt schema1. - List tables inside particular schemas. For eg: 'schema1'. (point'.' must be used)

DROP SCHEMA IF EXISTS accounting;


ALTER SCHEMA schema_name RENAME TO new_name;


SELECT current_database();

ALTER TABLE table_name ADD COLUMN new_column_name data_type constraint;

ALTER TABLE table_name MODIFY COLUMN column_name datatype NULL;

ALTER TABLE stocks ALTER COLUMN open DROP NOT  NULL;
 high double precision NULL, 
 low double precision NULL, 
 close double precision NULL, 
 volume double precision NULL, 
 avg_price double precision NULL, 
 ma3 double precision NULL, 
 ma5 double precision NULL, 
 ma15 double precision NULL,
 ma30 double precision NULL;
