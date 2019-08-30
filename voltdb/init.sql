DROP PROCEDURE Transfer IF EXISTS;
DROP PROCEDURE Deposit IF EXISTS;
DROP TABLE kv IF EXISTS;

CREATE TABLE kv
(
      key      varchar(250) not null,
      value    INTEGER not null,
      PRIMARY KEY (key)

);

PARTITION TABLE kv ON COLUMN key;

load classes generated-data/storedprocs.jar;
CREATE PROCEDURE
   FROM CLASS Transfer;
CREATE PROCEDURE
  FROM CLASS Deposit;
