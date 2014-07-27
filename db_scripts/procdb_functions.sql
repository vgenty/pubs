--Overwrite functions if they already exist
DROP FUNCTION IF EXISTS DoesTableExist(TEXT);
DROP FUNCTION IF EXISTS MakeProjTable(TEXT);
DROP FUNCTION IF EXISTS InsertIntoProjTable(TEXT,INT,INT,BOOLEAN);
DROP FUNCTION IF EXISTS UpdateProjStatus(TEXT,INT,INT,SMALLINT,SMALLINT);
DROP FUNCTION IF EXISTS GetRuns(TEXT[],SMALLINT[]);


--Homemade types
DROP TYPE IF EXISTS RunSubrunList;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Check if a table already exists. Used by many other functions here.

CREATE OR REPLACE FUNCTION DoesTableExist(tname TEXT) RETURNS BOOLEAN AS $$
DECLARE

doesExist BOOLEAN;

BEGIN

  SELECT TRUE FROM INFORMATION_SCHEMA.columns WHERE table_name = tname LIMIT 1 INTO doesExist;
  
  IF doesExist THEN
    RETURN TRUE;
  ELSE RETURN FALSE;
  END IF;
      	
END;
$$ LANGUAGE PLPGSQL;




---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Create a Project table with columns: Run, Subrun, Status, Seq

CREATE OR REPLACE FUNCTION MakeProjTable(myName TEXT) RETURNS INT AS $$
DECLARE
--local variables can go here
myQuery TEXT;

BEGIN

  IF DoesTableExist(myName) THEN
    RAISE EXCEPTION 'Project table % already exists.',myName;
  ELSE
    myQuery := format('CREATE TABLE %s (RUN INT NOT NULL, SUBRUN INT NOT NULL, STATUS SMALLINT NOT NULL, SEQ SMALLINT NOT NULL, PRIMARY KEY (RUN,SUBRUN,SEQ))',myName);
    EXECUTE myQuery;
  END IF;
    
RETURN 1;  	
END;
$$ LANGUAGE PLPGSQL;


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--NOTE: rewrite this! the checks for run/subrun are already done since the table is
--created with PRIMARY KEY (RUN,SUBRUN,SEQ) flags. This doesn't allow duplicates (for that triplet).

--Insert values into a Project table. Default status is always used as 0.
--If the update sequence number boolean is not supplied (default false):
     --Check if this run/subrun are in here already. If so, EXCEPTION. If not, insert, with seq number = 0
--If the update sequence number boolean is supplied as true:
     --Check if this run/subrun are in here already (seq = 0). If so, EXCEPTION. 
     	     --If not, insert with +1 to the highest sequence number that this run/subrun have.
--This function returns the sequence number that is being added.

CREATE OR REPLACE FUNCTION InsertIntoProjTable(tname TEXT, myrun INT, mysubrun INT, updateSeq BOOLEAN DEFAULT FALSE) RETURNS INT AS $$
DECLARE
myQuery TEXT;
myRecord RECORD;
myMaxSeq INT;

BEGIN
  --make sure table exists  
  IF NOT DoesTableExist(tname) THEN
    RAISE EXCEPTION 'Table % does not exist.', tname;
  END IF;

  --no negative values in this table
  IF myrun < 0 OR mysubrun < 0 THEN
    RAISE EXCEPTION 'Can''t have negative values in table %.', tname;
  END IF;

  --check to make sure run/subrun is not already in table
  myQuery := format('(SELECT * FROM %s WHERE %s.run = %s AND %s.subrun = %s)',tname,tname,myrun,tname,mysubrun,tname);
  EXECUTE myQuery INTO myRecord;
   
  --if run/subrun are already in table
  IF myRecord IS NOT NULL THEN
    --if updateSeq boolean is false
    IF updateSeq IS FALSE THEN
      RAISE EXCEPTION 'Entry with this run, subrun, sequence number already exists in table, and updateSeq is FALSE!';
    --if updateSeq boolean is true, use highest seq # for that run/subrun PLUS ONE
    ELSE 
      myQuery := format('SELECT MAX(seq) FROM %s WHERE %s.run = %s AND %s.subrun = %s',tname,tname,myrun,tname,mysubrun,tname);
      EXECUTE myQuery INTO myMaxSeq;
      myQuery := format('INSERT INTO %s (run,subrun,status,seq) VALUES (%s,%s,0::SMALLINT,%s)',tname,myrun,mysubrun,myMaxSeq+1);
      EXECUTE myQuery;
      RETURN myMaxSeq+1;
    END IF;
  --if run/subrun are not already in table, use sequence number = 0
  ELSE 
    myQuery := format('INSERT INTO %s (run,subrun,status,seq) VALUES (%s,%s,0::SMALLINT,0)',tname,myrun,mysubrun);
    EXECUTE myQuery;  
    RETURN 0;
  END IF;
  

END;
$$ LANGUAGE PLPGSQL;




---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Update status entry in an existing row of Project table

CREATE OR REPLACE FUNCTION UpdateProjStatus(tname TEXT, myrun INT, mysubrun INT, mystatus SMALLINT, myseq SMALLINT) RETURNS INT AS $$
DECLARE
dummy TEXT;
rowExists BOOLEAN;
BEGIN

  --make sure table exists  
  IF NOT DoesTableExist(tname) THEN
    RAISE EXCEPTION 'Table % does not exist.', tname;
  END IF;

  --check if row exists in table
  dummy := format('SELECT TRUE FROM %s WHERE run = %s AND subrun = %s AND seq = %s LIMIT 1',tname,myrun,mysubrun,myseq);
  EXECUTE dummy INTO rowExists;

  IF rowExists THEN
      dummy := format('UPDATE ONLY %s SET status = %s WHERE run = %s AND subrun = %s AND seq = %s',tname,mystatus,myrun,mysubrun,myseq);
    EXECUTE dummy;
  ELSE 
    RAISE EXCEPTION 'This row does not exist in the table! Can''t modify status.';  
  END IF;

RETURN 1;
END;
$$ LANGUAGE PLPGSQL;





---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--This function looks at all project tables supplied arrayOfTables, 
--and for each project table you ask for a specific status supplied in arrayOfStatuses.
--The function returns the run/subrun #s when each project has that respective status

CREATE TYPE RunSubrunList AS (myRun INT, mySubrun INT);

CREATE OR REPLACE FUNCTION GetRuns(arrayOfTables TEXT[], arrayOfStatuses SMALLINT[]) RETURNS SETOF RunSubrunList AS $$
DECLARE
iTable TEXT;
dummy TEXT;
myRunSubrunList RunSubrunList;
myRecord RECORD;
loopCounter INT;

BEGIN
  --make sure all of the tables exist
  FOREACH iTable IN ARRAY arrayOfTables LOOP
    IF NOT DoesTableExist(iTable) THEN
      RAISE EXCEPTION 'Table % does not exist.', iTable;
    END IF;
  END LOOP;

  --each table needs its own status, so the two input arrays need to be the same length
  IF array_length(arrayOfTables,1) != array_length(arrayOfStatuses,1) THEN
    RAISE EXCEPTION 'Your input tables array is different length than input status array.';
  END IF;

  --build a query to inner join each table with the first to get the end result.
  dummy := format('SELECT %s.run, %s.subrun FROM %s',arrayOfTables[1],arrayOfTables[1],arrayOfTables[1]);
  loopCounter := 1;
  FOREACH iTable IN ARRAY arrayOfTables LOOP
    --skip the first table... don't need to INNER JOIN it with itself
    --[1] is the first element in psql, not [0]!  
    IF iTable = arrayOfTables[1] THEN 
      loopCounter := loopCounter + 1; 
      CONTINUE;
    END IF;
  
  dummy := format('%s INNER JOIN %s ON %s.run = %s.run AND %s.subrun = %s.subrun AND %s.status = %s AND %s.status = %s',dummy,iTable,arrayOfTables[1],iTable,arrayOfTables[1],iTable,arrayOfTables[1],arrayOfStatuses[1],arrayOfTables[loopCounter],arrayOfStatuses[loopCounter]);

  loopCounter := loopCounter + 1;
  END LOOP;

  --debug: print the actual query you're doing
  --RAISE INFO '%',dummy;
 
  FOR myRecord IN EXECUTE dummy LOOP
    myRunSubrunList.myRun = myRecord.run;
    myRunSubrunList.mySubrun = myRecord.subrun;
    RETURN NEXT myRunSubrunList;
  END LOOP;


END;
$$ LANGUAGE PLPGSQL;
