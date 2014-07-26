--Overwrite functions if they already exist
DROP FUNCTION IF EXISTS MakeProject(TEXT);
DROP FUNCTION IF EXISTS InsertIntoProjTable(TEXT,INT,INT,SMALLINT,SMALLINT);


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Check if a project table already exists, if not, create that table
--with columns: Run, Subrun, Status, Seq

CREATE OR REPLACE FUNCTION MakeProject(myName TEXT) RETURNS INT AS $$
DECLARE
--local variables can go here
doesExist BOOLEAN;
myQuery TEXT;

BEGIN
  SELECT TRUE FROM INFORMATION_SCHEMA.columns WHERE table_name = myName LIMIT 1 INTO doesExist;

  IF doesExist THEN
    RAISE EXCEPTION 'Table % already exists. You suck',myName;
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
  --no negative values in this table
  IF myrun < 0 OR mysubrun < 0 OR mystatus < 0 OR myseq < 0 THEN
    RAISE EXCEPTION 'Can''t have negative values in this table. You suck';
  ELSE
    myQuery := format('INSERT INTO %s (run,subrun,status,seq) VALUES (%s,%s,%s,%s)',tname,myrun,mysubrun,mystatus,myseq);
    EXECUTE myQuery;
  END IF;

  RETURN 1;
END;
$$ LANGUAGE PLPGSQL;
