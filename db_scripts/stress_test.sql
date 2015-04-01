DROP FUNCTION IF EXISTS StressTest();

CREATE OR REPLACE FUNCTION StressTest() RETURNS VOID AS $$
DECLARE
myint INT;
rec RECORD;

BEGIN

RAISE INFO 'Start of StressTest function. Time: %',clock_timestamp();

SELECT MakeProjTable('t0') INTO myint;
SELECT MakeProjTable('t1') INTO myint;
SELECT MakeProjTable('t2') INTO myint;
SELECT MakeProjTable('t3') INTO myint;
SELECT MakeProjTable('t4') INTO myint;
SELECT MakeProjTable('t5') INTO myint;
SELECT MakeProjTable('t6') INTO myint;
SELECT MakeProjTable('t7') INTO myint;
SELECT MakeProjTable('t8') INTO myint;
SELECT MakeProjTable('t9') INTO myint;

RAISE INFO 'Done making tables. Time: %',clock_timestamp();

FOR i IN 0..10000 LOOP
  SELECT InsertIntoProjTable('t0',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t1',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t2',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t3',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t4',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t5',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t6',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t7',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t8',1,2,TRUE) INTO myint;
  SELECT InsertIntoProjTable('t9',1,2,TRUE) INTO myint;
END LOOP;

RAISE INFO 'Done inserting into tables. Time: %',clock_timestamp();

SELECT UpdateProjStatus('t0',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t1',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t2',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t3',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t4',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t5',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t6',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t7',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t8',1,2,1::SMALLINT,17::SMALLINT) INTO myint;
SELECT UpdateProjStatus('t9',1,2,1::SMALLINT,17::SMALLINT) INTO myint;

RAISE INFO 'Done updating status from 0 to 1 in one row in each table. Time: %',clock_timestamp();

SELECT GetRuns(ARRAY['t0','t1','t2','t3','t4','t5','t6','t7','t8','t9'],ARRAY[1::SMALLINT,1::SMALLINT,1::SMALLINT,1::SMALLINT,1::SMALLINT,1::SMALLINT,1::SMALLINT,1::SMALLINT,1::SMALLINT,1::SMALLINT]) INTO rec;

RAISE INFO 'Result of GetRuns is %',rec;

RAISE INFO 'Done with GetRuns call. Time: %s',clock_timestamp();


DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t6;
DROP TABLE IF EXISTS t7;
DROP TABLE IF EXISTS t8;
DROP TABLE IF EXISTS t9;


END;
$$ LANGUAGE PLPGSQL;



