CREATE SCHEMA etl;

DROP TABLE IF EXISTS stocks;
CREATE TABLE stocks (
  date timestamptz NOT NULL,
  symbol varchar(10) NOT NULL,
  category varchar(64),
  open double precision,
  high double precision,
  low double precision,
  close double precision,
  volume double precision,
  avg_price double precision,
  ma3 double precision,
  ma5 double precision,
  ma15 double precision,
  ma30 double precision,
  PRIMARY KEY(date, symbol)
);
