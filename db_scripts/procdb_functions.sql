--Overwrite functions if they already exist
DROP FUNCTION IF EXISTS DoesTableExist(TEXT);
DROP FUNCTION IF EXISTS MakeProjTable(TEXT);
DROP FUNCTION IF EXISTS InsertIntoProjTable(TEXT,INT,INT,SMALLINT,SMALLINT);
DROP FUNCTION IF EXISTS UpdateProjStatus(TEXT,INT,INT,SMALLINT,SMALLINT);




---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Check if a table already exists. Used by many other functions here.

CREATE OR REPLACE FUNCTION DoesTableExist(tname TEXT) RETURNS BOOLEAN AS $$
DECLARE
--local variables can go here
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
    myQuery := format('CREATE TABLE %s (RUN INT NOT NULL, SUBRUN INT NOT NULL, STATUS SMALLINT NOT NULL, SEQ SMALLINT NOT NULL)',myName);
    EXECUTE myQuery;
  END IF;
      	
  RETURN 1;
END;
$$ LANGUAGE PLPGSQL;


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Insert values into a Project table

CREATE OR REPLACE FUNCTION InsertIntoProjTable(tname TEXT, myrun INT, mysubrun INT, mystatus SMALLINT, myseq SMALLINT) RETURNS INT AS $$
DECLARE
myQuery TEXT;

BEGIN
  --make sure table exists  
  IF NOT DoesTableExist(tname) THEN
    RAISE EXCEPTION 'Table % does not exist.', tname;
  END IF;

  --no negative values in this table
  IF myrun < 0 OR mysubrun < 0 OR mystatus < 0 OR myseq < 0 THEN
    RAISE EXCEPTION 'Can''t have negative values in table %.', tname;
  END IF;
  
  myQuery := format('INSERT INTO %s (run,subrun,status,seq) VALUES (%s,%s,%s,%s)',tname,myrun,mysubrun,mystatus,myseq);
  EXECUTE myQuery;

  RETURN 1;
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
