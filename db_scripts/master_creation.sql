--SET ROLE uboonedaq_admin;
--SET ROLE uboone_admin;

--CREATE EXTENSION HSTORE;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Check if a table already exists. Used by many other functions here.
DROP FUNCTION IF EXISTS DoesTableExist(TEXT);
CREATE OR REPLACE FUNCTION DoesTableExist(tname TEXT) RETURNS BOOLEAN AS $$
DECLARE
doesExist BOOLEAN;
BEGIN

  SELECT TRUE FROM INFORMATION_SCHEMA.columns WHERE table_name = lower(tname) LIMIT 1 INTO doesExist;
  
  IF doesExist THEN
    RETURN TRUE;
  ELSE RETURN FALSE;
  END IF;
      	
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION CreateDaemonTable() RETURNS VOID AS $$
CREATE TABLE IF NOT EXISTS DaemonTable ( Server        TEXT      NOT NULL,
					 MaxProjCtr    INT       NOT NULL,
 					 LifeTime      INT       NOT NULL,
					 LogRange      INT       NOT NULL,
					 RunSyncPeriod INT       NOT NULL,
					 UpdatePeriod  INT       NOT NULL,
					 CleanUpPeriod INT       NOT NULL,
					 EMail         TEXT      NOT NULL,
					 Enabled       BOOLEAN   NOT NULL,
					 LogTime       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					 PRIMARY KEY (Server) );
$$ LANGUAGE SQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION UpdateDaemonTable( nodename       TEXT,
       	  	  	   		      max_proj_ctr   INT,
					      max_uptime     INT,
					      log_duration   INT,
					      sync_period    INT,
					      update_period  INT,
					      cleanup_period INT,
					      mail_address   TEXT,
					      enable         BOOLEAN ) RETURNS VOID AS $$
DECLARE
  does_exist BOOLEAN;
BEGIN
  IF NOT DoesTableExist('DaemonTable') THEN
    RAISE EXCEPTION 'DaemonTable does not exist!';
  END IF;

  SELECT TRUE FROM DaemonTable WHERE Server = nodename INTO does_exist;

  IF does_exist IS NULL THEN
    INSERT INTO DaemonTable
  	   	(Server, MaxProjCtr, LifeTime, LogRange, RunSyncPeriod, UpdatePeriod, CleanUpPeriod, EMail, Enabled)
	   VALUES
		(nodename,max_proj_ctr,max_uptime,log_duration,sync_period,update_period,cleanup_period,mail_address,enable);
  ELSE
    UPDATE DaemonTable SET
    	   MaxProjCtr    = max_proj_ctr,
	   LifeTime      = max_uptime,
	   LogRange      = log_duration,
	   RunSyncPeriod = sync_period,
	   UpdatePeriod  = update_period,
	   CleanUpPeriod = cleanup_period,
	   EMail         = mail_address,
	   Enabled       = enable
           WHERE Server  = nodename;
  END IF;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

CREATE OR REPLACE FUNCTION CreateDaemonLogTable() RETURNS VOID AS $$
CREATE TABLE IF NOT EXISTS DaemonLogTable( ID         BIGSERIAL,
				      	   Server     TEXT NOT NULL,
       	     	    	   	      	   MaxProjCtr INT  NOT NULL,
				      	   LifeTime   INT  NOT NULL,
       	     	    	   	      	   ProjCtr    INT  NOT NULL,
				      	   UpTime     INT  NOT NULL,
		    		      	   LogItem    HSTORE  NOT NULL,
				      	   LogTime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				      	   PRIMARY KEY (ID,SERVER) );
				      
$$ LANGUAGE SQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS UpdateDaemonLog( TEXT,
     	      	 			 INT, INT, INT, INT,
					 HSTORE );
CREATE OR REPLACE FUNCTION UpdateDaemonLog( nodename     TEXT,
       	  	  	   		    max_proj_ctr INT,
					    max_uptime   INT,
					    proj_ctr     INT,
					    uptime       INT,
					    log_item     HSTORE) RETURNS VOID AS $$
DECLARE
  range INT;
  query TEXT;
BEGIN

  IF NOT DoesTableExist('DaemonLogTable') THEN
    RAISE EXCEPTION 'DaemonLogTable does not exist!';
  END IF;

  INSERT INTO DaemonLogTable (Server, MaxProjCtr, LifeTime, ProjCtr, UpTime, LogItem)
  VALUES (nodename, max_proj_ctr, max_uptime, proj_ctr, uptime, log_item);
  
  query := format(' INSERT INTO DaemonLogTable (Server, MaxProjCtr, LifeTime, ProjCtr, UpTime, LogItem)
   	   	    VALUES 
		    (''%s'',%s,%s,%s,%s,''%s''',nodename,max_proj_ctr,max_uptime,proj_ctr,uptime,log_item);

  RAISE WARNING '%', query;

  SELECT LogRange::TEXT FROM DaemonTable WHERE Server = nodename INTO range;

  IF range IS NULL THEN
    range := 3600;
  END IF;

--  range := format('%s seconds',range);

  query := format(' DELETE FROM DaemonLogTable
  	   	    WHERE Server = ''%s'' 
		    	  AND 
			  LogTime < ( CURRENT_TIMESTAMP - INTERVAL ''%s seconds'')', nodename,range);
  EXECUTE query;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Check if a table already exists. Used by many other functions here.
CREATE OR REPLACE FUNCTION DoesDaemonExist(tname TEXT) RETURNS BOOLEAN AS $$
DECLARE
doesExist BOOLEAN;
BEGIN
	
  IF NOT DoesTableExist('DaemonTable') THEN
    --RAISE EXCEPTION 'Process Table not found!!!!';
    RETURN FALSE;
  END IF;
  --RAISE WARNING 'Process Table found...';  
  --RAISE EXCEPTION 'SELECT TRUE FROM ProcessTable WHERE Project = % LIMIT 1 INTO doesExist;', tname;
  SELECT TRUE FROM DaemonTable WHERE lower(Server) = lower(tname) LIMIT 1 INTO doesExist;
  IF doesExist IS NULL THEN
    RETURN FALSE;
  ELSE
    RETURN TRUE;
  END IF;
      	
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Check if a table already exists. Used by many other functions here.
CREATE OR REPLACE FUNCTION DoesProjectExist(tname TEXT) RETURNS BOOLEAN AS $$
DECLARE
doesExist BOOLEAN;
BEGIN
	
  IF NOT DoesTableExist('processtable') THEN
    --RAISE EXCEPTION 'Process Table not found!!!!';
    RETURN FALSE;
  END IF;
  --RAISE WARNING 'Process Table found...';  
  --RAISE EXCEPTION 'SELECT TRUE FROM ProcessTable WHERE Project = % LIMIT 1 INTO doesExist;', tname;
  SELECT TRUE FROM ProcessTable WHERE lower(Project) = lower(tname) LIMIT 1 INTO doesExist;
  IF doesExist IS NULL THEN
    RETURN FALSE;
  ELSE
    RETURN TRUE;
  END IF;
      	
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Function to clear ALL projects registered in ProcessTable
DROP FUNCTION IF EXISTS RemoveProject(project_name TEXT);
CREATE OR REPLACE FUNCTION RemoveProject(project_name TEXT) RETURNS VOID AS $$
DECLARE
myBool  BOOLEAN;
myQuery TEXT;
BEGIN

  SELECT DoesProjectExist(project_name) INTO myBool;
  IF myBool IS NULL THEN
    RAISE INFO '+++++++++ Project % table not found! +++++++++++', project_name;
  ELSE
    myQuery := format('DROP TABLE %s',project_name);
    EXECUTE myQuery;
    RAISE INFO 'Dropped table project %',project_name;
    DELETE FROM ProcessTable WHERE Project = project_name;
    RAISE INFO 'Dropped project %',project_name;
  END IF;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Function to clear ALL projects registered in ProcessTable
DROP FUNCTION IF EXISTS RemoveProcessDB();
CREATE OR REPLACE FUNCTION RemoveProcessDB() RETURNS VOID AS $$
DECLARE
myBool  BOOLEAN;
myRec   RECORD;
myQuery TEXT;
BEGIN
  -- Check if ProcessTable exists. If not, don't throw exception but simply return
  SELECT DoesTableExist('processtable') INTO myBool;
  IF NOT myBool THEN
    RAISE INFO '+++++++++ ProcessTable does not exist yet++++++++++';
    RETURN;
  END IF;

  -- For projects in ProcessTable, attempt to drop its project table & remove entry from ProcessTable
  FOR myRec IN SELECT Project FROM ProcessTable LOOP
    EXECUTE RemoveProject(myRec.Project);
  END LOOP;
  DROP TABLE IF EXISTS ProcessTable;
  DROP TABLE IF EXISTS DaemonTable;
  DROP TABLE IF EXISTS DaemonLogTable;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Create TestRunTable
DROP FUNCTION IF EXISTS CreateTestRunTable();
DROP FUNCTION IF EXISTS CreateTestRunTable(TEXT);

CREATE OR REPLACE FUNCTION CreateTestRunTable(RunTableName TEXT) RETURNS VOID AS $$
DECLARE
query TEXT;
mybool BOOLEAN;
BEGIN

  -- Cannot remove run table --
  SELECT DoesTableExist(RunTableName) INTO mybool;
  IF mybool THEN
    RAISE EXCEPTION '+++++++++ Run Table w/ name % already exists ++++++++++',RunTableName;
  END IF;

  query := format( ' CREATE TABLE %s 
  	   	     ( RunNumber    INT NOT NULL,
     	     	       SubRunNumber INT NOT NULL,
		       TimeStart    TIMESTAMP NOT NULL,
		       TimeStop     TIMESTAMP NOT NULL,
		       ConfigID     INT NOT NULL,
		       PRIMARY KEY (RunNumber,SubRunNumber) ); ', RunTableName);
  EXECUTE query;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Clear TestRunTable
DROP FUNCTION IF EXISTS ClearTestRunTable();
DROP FUNCTION IF EXISTS ClearTestRunTable(TEXT);

CREATE OR REPLACE FUNCTION ClearTestRunTable(RunTableName TEXT) RETURNS VOID AS $$
DECLARE
query TEXT;
mybool BOOLEAN;
BEGIN

  -- Cannot remove run table --
  SELECT DoesTableExist(RunTableName) INTO mybool;
  IF NOT mybool THEN
    RAISE EXCEPTION '+++++++++ Run Table w/ name % does not exist ++++++++++',RunTableName;
  END IF;

  query := format( ' TRUNCATE %s; ', RunTableName ); 
  EXECUTE query;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Duplicate TestRunTable
DROP FUNCTION IF EXISTS DuplicateTestRunTable();
DROP FUNCTION IF EXISTS DuplicateTestRunTable(TEXT,TEXT);

CREATE OR REPLACE FUNCTION DuplicateTestRunTable(InRunTableName TEXT, OutRunTableName TEXT) RETURNS VOID AS $$
DECLARE
query TEXT;
mybool BOOLEAN;
BEGIN

  -- Does input table exist? --
  SELECT DoesTableExist(InRunTableName) INTO mybool;
  IF NOT mybool THEN
    RAISE EXCEPTION '++++++++ Run Table w/ name % does not exist +++++++++++',InRunTableName;
  END IF;

  -- Does output table exist? --
  SELECT DoesTableExist(OutRunTableName) INTO mybool;
  IF mybool THEN
    RAISE EXCEPTION '+++++++++ Run Table w/ name % already exists ++++++++++',OutRunTableName;
  END IF;

  query := format( 'CREATE TABLE %s (LIKE %s INCLUDING ALL);
                    INSERT INTO %s SELECT * FROM %s;', 
		    OutRunTableName, InRunTableName,
		    OutRunTableName, InRunTableName);

  
  EXECUTE query;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Copy TestRunTable
DROP FUNCTION IF EXISTS CopyTestRunTable();
DROP FUNCTION IF EXISTS CopyTestRunTable(TEXT,TEXT);

CREATE OR REPLACE FUNCTION CopyTestRunTable(InRunTableName TEXT, OutRunTableName TEXT) RETURNS VOID AS $$
DECLARE
query TEXT;
mybool BOOLEAN;
BEGIN

  -- Does input table exist? --
  SELECT DoesTableExist(InRunTableName) INTO mybool;
  IF NOT mybool THEN
    RAISE EXCEPTION '++++++++ Run Table w/ name % does not exist +++++++++++',InRunTableName;
  END IF;

  -- Does output table exist? --
  SELECT DoesTableExist(OutRunTableName) INTO mybool;
  IF NOT mybool THEN
    RAISE EXCEPTION '+++++++++ Run Table w/ name % does not exist ++++++++++',OutRunTableName;
  END IF;

  query := format( 'INSERT INTO %s SELECT * FROM %s;', 
		    OutRunTableName, InRunTableName);

  
  EXECUTE query;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Copy TestRunTable
DROP FUNCTION IF EXISTS CopySomeTestRunTable();
DROP FUNCTION IF EXISTS CopySomeTestRunTable(TEXT,TEXT,INT[]);

CREATE OR REPLACE FUNCTION CopySomeTestRunTable(InRunTableName TEXT, OutRunTableName TEXT, Runs INT[]) RETURNS VOID AS $$
DECLARE
query TEXT;
mybool BOOLEAN;
Run INT;
BEGIN

  -- Does input table exist? --
  SELECT DoesTableExist(InRunTableName) INTO mybool;
  IF NOT mybool THEN
    RAISE EXCEPTION '++++++++ Run Table w/ name % does not exist +++++++++++',InRunTableName;
  END IF;

  -- Does output table exist? --
  SELECT DoesTableExist(OutRunTableName) INTO mybool;
  IF NOT mybool THEN
    RAISE EXCEPTION '+++++++++ Run Table w/ name % does not exist ++++++++++',OutRunTableName;
  END IF;

  FOREACH Run in ARRAY Runs
  LOOP
         query := format( 'INSERT INTO %s SELECT * FROM %s WHERE %s.runnumber=%s;',
	       	           OutRunTableName, InRunTableName, InRunTableName, Run);

  
	 EXECUTE query;
  END LOOP;

  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Delete TestRunTable
DROP FUNCTION IF EXISTS RemoveTestRunTable(TEXT);
CREATE OR REPLACE FUNCTION RemoveTestRunTable(RunTableName TEXT) RETURNS VOID AS $$
DECLARE
query TEXT;
mybool BOOLEAN;
BEGIN

  -- Cannot remove non-existing run table --
  SELECT DoesTableExist(RunTableName) INTO mybool;
  IF NOT mybool THEN
    RAISE EXCEPTION '+++++++++ Run Table w/ name % does not exist ++++++++++',RunTableName;
  END IF;

  query := format('DROP TABLE %s;',RunTableName);
  -- Make sure there is no project using this run table --
  SELECT DoesTableExist('ProcessTable') INTO mybool;
  IF mybool THEN
    SELECT TRUE FROM ProcessTable WHERE lower(RefName) = lower(RunTableName) LIMIT 1 INTO mybool;
    IF mybool THEN
      RAISE EXCEPTION '++++++++++ Cannot drop a run table used by projects! ++++++++++';
    END IF;
  END IF;
  query := format('DROP TABLE %s;',RunTableName);
  EXECUTE query;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
DROP FUNCTION IF EXISTS InsertIntoTestRunTable( Run INT, SubRun INT, 
     	      	 				TimeStart TIMESTAMP,
						TimeEnd   TIMESTAMP);

DROP FUNCTION IF EXISTS InsertIntoTestRunTable( RunTableName TEXT,
     	      	 				Run INT, SubRun INT, 
     	      	 				TimeStart TIMESTAMP,
						TimeEnd   TIMESTAMP);

CREATE OR REPLACE FUNCTION InsertIntoTestRunTable( RunTableName TEXT,
       	  	  	   			   Run INT, SubRun INT, 
     	      	 				   TStart TIMESTAMP,
						   TEnd   TIMESTAMP) RETURNS VOID AS $$
DECLARE
  query TEXT;
  myrec RECORD;
BEGIN

  IF Run < 0 OR SubRun < 0 THEN
    RAISE EXCEPTION 'Run/SubRun must be a positive integer!';
  END IF;

  IF NOT DoesTableExist(RunTableName) THEN
    RAISE EXCEPTION 'Run table % does not exist!', RunTableName;
  END IF;

  query := format( ' SELECT TRUE AS PRESENCE FROM %s WHERE RunNumber = %s AND SubRunNumber = %s',
  	   	   RunTableName, Run, SubRun );
  EXECUTE query INTO myrec;

  IF myrec.PRESENCE IS NOT NULL THEN
    RAISE EXCEPTION 'Specified (Run,SubRun) already exists in the Run Table %s!',RunTableName;
  END IF;

  query := format( ' INSERT INTO %s 
  	   	     (RunNumber,SubRunNumber,TimeStart,TimeStop,ConfigID)
		     VALUES
		     (%s,%s,''%s''::TIMESTAMP,''%s''::TIMESTAMP,0);',
		   RunTableName,
		   Run,SubRun,TStart,TEnd);
  EXECUTE query;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- (Re-)create & fill TestRunTable

DROP FUNCTION IF EXISTS FillTestRunTable( NumRuns    INT,
     	      	 			  NumSubRuns INT);

DROP FUNCTION IF EXISTS FillTestRunTable( RunTableName TEXT,
     	      	 			  NumRuns    INT,
     	      	 			  NumSubRuns INT);

CREATE OR REPLACE FUNCTION FillTestRunTable( RunTableName TEXT,
       	  	  	   		     NumRuns    INT DEFAULT NULL,
     	      	 			     NumSubRuns INT DEFAULT NULL) RETURNS VOID AS $$
DECLARE
  run_start TIMESTAMP;
  run_end   TIMESTAMP;
  make_newone BOOLEAN;
BEGIN

  IF NumRuns IS NULL THEN NumRuns := 100; END IF;

  IF NumSubRuns IS NULL THEN NumSubRuns := 10; END IF;

  IF NumRuns < 0 OR NumSubRuns < 0 THEN
    RAISE EXCEPTION 'Provide positive run/sub-run number (%/%)!',NumRuns,NumSubRuns;
  END IF;

  IF NOT DoesTableExist(RunTableName) THEN
    RAISE EXCEPTION 'Run table % does not exist!',RunTableName;
  END IF;

  run_start := '2015-01-01 00:00:00'::TIMESTAMP;

  FOR run IN 0..NumRuns LOOP
    FOR subrun IN 0..NumSubRuns LOOP
      run_start := run_start + INTERVAL '20 minutes';
      run_end   := run_start + INTERVAL '10 minutes';
      EXECUTE InsertIntoTestRunTable(RunTableName,run,subrun,run_start,run_end);
    END LOOP;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS GetRunTimeStamp( Run INT, SubRun INT);
DROP FUNCTION IF EXISTS GetRunTimeStamp( RunTableName TEXT, Run INT, SubRun INT);

CREATE OR REPLACE FUNCTION GetRunTimeStamp ( RunTableName TEXT, Run INT, SubRun INT) 
       	  	  RETURNS TABLE (TimeStart TIMESTAMP, TimeStop TIMESTAMP) AS $$
DECLARE
rec RECORD;
query TEXT;
BEGIN
 query := format( ' SELECT TimeStart, TimeStop FROM %s 
       	  	    WHERE RunNumber = %s AND SubRunNumber = %s LIMIT 1;',
		  RunTableName, Run, SubRun );
  
 EXECUTE query INTO rec;
 RETURN QUERY SELECT rec.TimeStart, rec.TimeStop;
-- SELECT TimeStart, TimeStop FROM MainRun WHERE RunNumber = Run AND SubRunNumber = SubRun LIMIT 1;
END
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
DROP FUNCTION IF EXISTS GetVersionRunRange(project_name TEXT);

CREATE OR REPLACE FUNCTION GetVersionRunRange(project_name TEXT) RETURNS TABLE( ProjectVer  SMALLINT,
       	  	  	   				   	 	 	StartRun    INT,
										StartSubRun INT,
										EndRun      INT,
										EndSubRun   INT ) AS $$
DECLARE
  rec RECORD;
  start_run    INT;
  start_subrun INT;
  end_run      INT;
  end_subrun   INT;
  query TEXT;
  runtablename TEXT;
BEGIN

  IF NOT DoesProjectExist(project_name) THEN
    RAISE EXCEPTION 'Project % does not exist!', project_name;
  END IF;

  query := format('SELECT RefName FROM ProcessTable WHERE Project=''%s'' LIMIT 1', project_name);
  EXECUTE QUERY INTO rec;

  runtablename := rec.RefName;
  IF NOT DoesTableExist(runtablename) THEN
    RAISE EXCEPTION 'Project reference table % does not exist!',runtablename;
  END IF;

  query := format('SELECT MAX(RunNumber) AS MAXRUN FROM %s',runtablename);

  rec := NULL;
  EXECUTE query INTO rec;
  IF rec IS NULL THEN
  --SELECT MAX(RunNumber) FROM MainRun INTO start_run;
  --IF start_run IS NULL THEN
    RAISE EXCEPTION '%s table contains no run number!', runtablename;
  END IF;

  start_run := rec.MAXRUN;

  query := format('SELECT MAX(SubRunNumber) AS MAXSUBRUN FROM %s WHERE RunNumber = %s',runtablename,start_run);
  --SELECT MAX(SubRunNumber) FROM MainRun WHERE RunNumber = start_run INTO start_subrun;
  rec := NULL;
  EXECUTE query INTO rec;
  IF rec IS NULL THEN
    RAISE EXCEPTION '% table contains no sub-run number!',runtablename;
  END IF;

  start_subrun := rec.MAXSUBRUN;
  start_subrun := start_subrun+1;
  end_run      := start_run;
  end_subrun   := start_subrun;

  FOR rec IN SELECT ProcessTable.ProjectVer,
      	     	    ProcessTable.StartRun,
		    ProcessTable.StartSubRun FROM ProcessTable ORDER BY ProjectVer DESC LOOP
    IF rec.StartRun < start_run OR (rec.StartRun = start_run AND rec.StartSubRun < start_subrun) THEN
      start_run       := rec.StartRun;
      start_subrun    := rec.StartSubRun;
      RETURN QUERY SELECT rec.ProjectVer, start_run, start_subrun, end_run, end_subrun;
      end_run    := start_run;
      end_subrun := start_subrun;

    END IF;
  END LOOP;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- (Re-)create ProcessTable

DROP FUNCTION IF EXISTS CreateProcessTable();
CREATE OR REPLACE FUNCTION CreateProcessTable() RETURNS VOID AS $$
CREATE TABLE IF NOT EXISTS ProcessTable ( ID          SERIAL,
       	     		    		  Project     TEXT      NOT NULL,
			    		  ProjectVer  SMALLINT  NOT NULL,
			    		  Command     TEXT      NOT NULL,
       	     		    		  Frequency   INT       NOT NULL,
					  SleepAfter  INT       NOT NULL,
			    		  EMail       TEXT      NOT NULL,
					  Server      TEXT      NOT NULL,
			    		  Resource    HSTORE    NOT NULL,
					  RefName     TEXT      NOT NULL,
			    		  StartRun    INT       NOT NULL DEFAULT 0,
			    		  StartSubRun INT       NOT NULL DEFAULT 0,
			    		  Enabled     BOOLEAN   DEFAULT TRUE,
			    		  Running     BOOLEAN   NOT NULL,
			    		  LogTime     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					  PRIMARY KEY(ID,Project,ProjectVer));
$$ LANGUAGE SQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Function to check the basic assumption of the process database
-- (1) ProcessTable must exist
-- (2) Projects registered in ProcessTable must have a corresponding table
-- add whatever is needed from further development
DROP FUNCTION IF EXISTS CheckDBIntegrity();
CREATE OR REPLACE FUNCTION CheckDBIntegrity() RETURNS BOOLEAN AS $$

DECLARE
t TEXT;
BEGIN
  -- Check ProcessTable presence
  IF NOT EXISTS ProcessTable THEN
    RAISE WARNING '++++++++++ ProcessTable must exist! ++++++++++';
    RETURN FALSE;
  END IF;
  -- Check Project table presence
  FOR t IN SELECT DISTINCT Project FROM ProcessTable LOOP
    IF NOT (SELECT DoesProjectExist(t)) THEN 
      RAISE WARNING 'Project % has no table! ',t;
      RETURN FALSE;
    END IF;
  END LOOP; 
  RETURN TRUE;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Function to define a new project
-- (1) Create a project table
-- (2) Insert a project into ProcessTable
-- Returns -1 in case of failure, and integer >=0 (unique key for a project) upon success
DROP FUNCTION IF EXISTS DefineProject( project_name TEXT,
     	      	 		       command      TEXT,
				       frequency    INT,
				       email        TEXT,
				       start_run    INT,
				       start_subrun INT,
				       resource     HSTORE,
				       enabled      BOOLEAN);
DROP FUNCTION IF EXISTS DefineProject( project_name TEXT,
     	      	 		       command      TEXT,
				       frequency    INT,
				       email        TEXT,
				       sleepAfter   INT,
				       nodename     TEXT,
				       runtable     TEXT,
				       start_run    INT,
				       start_subrun INT,
				       resource     HSTORE,
				       enabled      BOOLEAN);

CREATE OR REPLACE FUNCTION DefineProject( project_name TEXT,
     	      	 		       	  command      TEXT,
				       	  frequency    INT,
				       	  email        TEXT,
					  sleepAfter   INT DEFAULT 5,
					  nodename     TEXT DEFAULT '',
					  runtable     TEXT DEFAULT 'MainRun',
				       	  start_run    INT DEFAULT 0,
				       	  start_subrun INT DEFAULT 0,
				       	  resource     HSTORE DEFAULT '',
				       	  enabled      BOOLEAN DEFAULT TRUE) RETURNS INT AS $$
DECLARE
myBool    BOOLEAN;
myInt     INT;
myVersion SMALLINT;
BEGIN

  IF start_run < 0 OR start_subrun < 0 THEN
    RAISE WARNING '+++++++++ Negative run/subrun (%/%) not allowed! +++++++++',start_run,start_subrun;
    RETURN -1;
  END IF;

  -- Make sure frequency is positive
  IF frequency < 0 THEN
    RAISE WARNING '+++++++++ Negative frequency (%) invalid! +++++++++', frequency;
    RETURN -1;
  END IF;
  
  -- Make sure project does not yet exist in ProcessTable
  SELECT TRUE FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO myBool;
  IF NOT myBool IS NULL THEN
    RAISE WARNING '+++++++++ Project % already exists... +++++++++',project_name;
    RETURN -1;
  END IF;

  -- Make sure reference run table exists --
  IF NOT DoesTableExist(runtable) THEN
    RAISE EXCEPTION '% table does not exist!', runtable;
  END IF;

  -- Get the version number
  SELECT MAX(ProjectVer) FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO myVersion;
  IF myVersion IS NULL THEN
    myVersion := 0;
  ELSE
    myVersion := myVersion + 1;
  END IF;

  -- Insert into ProcessTable
  INSERT INTO ProcessTable ( Project,  Command, ProjectVer, Frequency, SleepAfter, RefName,
  	      		     StartRun, StartSubRun, Email, Server, Resource, Enabled, Running)
  	      VALUES ( project_name, command, myVersion, frequency, sleepAfter, runtable,
	      	       start_run, start_subrun, email, nodename, resource, enabled, FALSE);
  SELECT ID FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO myInt;
  IF myInt IS NULL THEN
    --SELECT DropStatusTable(project_name);
    --SELECT DropFileTable(project_name);
    RAISE EXCEPTION '+++++++++ Somehow failed to insert project %! +++++++++', project_name;
    RETURN -1;
  END IF;

  -- Attempt to make this project status table
  SELECT MakeProjTable(project_name) INTO myInt;
  IF myInt IS NULL THEN
    RAISE EXCEPTION '+++++++++ Failed to create a project table for %! ++++++++++',project_name;
    RETURN -1;
  END IF;

  SELECT MAX(ID) FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO myInt;

  EXECUTE OneProjectRunSynch(project_name, myVersion, start_run, start_subrun);

  RETURN myInt;

END;
$$ LANGUAGE PLPGSQL;


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

-- Function to update project version
DROP FUNCTION IF EXISTS UpdateProjectConfig( project_name TEXT,
     	      	 		       	     command      TEXT,
				       	     frequency    INT,
				       	     email        TEXT,
				       	     resource     HSTORE,
				       	     enabled      BOOLEAN,
					     version      INT);

-- Function to update project version
DROP FUNCTION IF EXISTS UpdateProjectConfig( project_name TEXT,
     	      	 		       	     command      TEXT,
				       	     frequency    INT,
				       	     email        TEXT,
				       	     resource     HSTORE,
				       	     enabled      BOOLEAN);

-- Function to update project version
DROP FUNCTION IF EXISTS UpdateProjectConfig( project_name TEXT,
     	      	 		       	     command      TEXT,
				       	     frequency    INT,
					     sleepAfter   INT,
				       	     email        TEXT,
					     nodename     TEXT,
				       	     resource     HSTORE,
				       	     enabled      BOOLEAN);

CREATE OR REPLACE FUNCTION UpdateProjectConfig( project_name TEXT,
     	      	 		       	     	command      TEXT DEFAULT NULL,
				       	     	frequency    INT  DEFAULT NULL,
						sleepAfter   INT  DEFAULT NULL,
				       	     	email        TEXT DEFAULT NULL,
						nodename     TEXT DEFAULT NULL,
				       	     	resource     HSTORE  DEFAULT NULL,
				       	     	enabled      BOOLEAN DEFAULT NULL) RETURNS BOOLEAN AS $$
DECLARE
project_ver INT;
query       TEXT;
myBool      BOOLEAN;
BEGIN

  -- Make sure project exists in ProcessTable
  SELECT TRUE FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO myBool;
  IF myBool IS NULL THEN
    RAISE WARNING '+++++++++ Project % does not exist... +++++++++',project_name;
    RETURN FALSE;
  END IF;

  IF command IS NULL AND frequency IS NULL AND email IS NULL AND nodename IS NULL AND resource IS NULL AND enabled IS NULL THEN
    RAISE EXCEPTION '+++++++++ Nothing to update! +++++++++';
    RETURN FALSE;
  END IF;

  SELECT MAX(ProjectVer) FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO project_ver; 

  query := 'UPDATE ProcessTable SET ';

  IF NOT command IS NULL THEN
    query := format('%s Command=''%s'',',query,command);
  END IF;

  IF NOT frequency IS NULL THEN
    query := format('%s Frequency=%s,',query,frequency);
  END IF;

  IF NOT sleepAfter IS NULL THEN
    query := format('%s SleepAfter=%s,',query,sleepAfter);
  END IF;

  IF NOT email IS NULL THEN
    query := format('%s Email=''%s'',',query,email);
  END IF;

  IF NOT nodename IS NULL THEN
    query := format('%s Server=''%s'',',query,nodename);
  END IF;

  IF NOT resource IS NULL THEN
    query := format('%s Resource=''%s''::HSTORE,',query,resource);
  END IF;

  IF NOT enabled IS NULL THEN
    IF enabled THEN
      query := format('%s Enabled=TRUE,',query);
    ELSE
      query := format('%s Enabled=FALSE,',query);
    END IF;
  END IF;

  query := TRIM( TRAILING ',' FROM query);

  query := format('%s WHERE ProjectVer=%s AND lower(Project)=lower(''%s'')',query,project_ver,project_name);

  EXECUTE query;

  RETURN TRUE;

END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
DROP FUNCTION IF EXISTS ProjectRunning(TEXT);
CREATE OR REPLACE FUNCTION ProjectRunning(project_name TEXT) RETURNS VOID AS $$
BEGIN
  IF DoesTableExist('ProcessTable') THEN
    UPDATE ProcessTable SET Running=TRUE WHERE lower(Project) = lower(project_name);
  END IF;
END
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
DROP FUNCTION IF EXISTS ProjectStopped(TEXT);
CREATE OR REPLACE FUNCTION ProjectStopped(project_name TEXT) RETURNS VOID AS $$
BEGIN
  IF DoesTableExist('ProcessTable') THEN
    UPDATE ProcessTable SET Running=FALSE WHERE lower(Project) = lower(project_name);
  END IF;
END
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Function to update project version
DROP FUNCTION IF EXISTS ProjectVersionUpdate( project_name TEXT,
     	      	 		       	      new_cmd      TEXT,
				       	      new_freq     INT,
				       	      new_email    TEXT,
					      new_run      INT,
					      new_subrun   INT,
				       	      resource     HSTORE,
				       	      new_en       BOOLEAN);

DROP FUNCTION IF EXISTS ProjectVersionUpdate( project_name TEXT,
     	      	 		       	      new_cmd      TEXT,
				       	      new_freq     INT,
      					      new_sleepAfter INT,
				       	      new_email    TEXT,
					      nodename     TEXT,
					      new_run      INT,
					      new_subrun   INT,
				       	      resource     HSTORE,
				       	      new_en       BOOLEAN);

CREATE OR REPLACE FUNCTION ProjectVersionUpdate( project_name TEXT,
     	      	 		       	      	 new_cmd      TEXT DEFAULT NULL,
				       	      	 new_freq     INT  DEFAULT NULL,
					      	 new_sleepAfter INT DEFAULT NULL,
				       	      	 new_email    TEXT DEFAULT NULL,
						 new_nodename TEXT DEFAULT NULL,
						 new_run      INT  DEFAULT NULL,
						 new_subrun   INT  DEFAULT NULL,
				       	      	 new_src      HSTORE DEFAULT NULL,
				       	    	 new_en       BOOLEAN DEFAULT NULL) RETURNS INT AS $$
DECLARE
  current_ver  INT;
  value_int    INT;
  value_bool   BOOLEAN;
  query_field  TEXT;
  query_values TEXT;
  query        TEXT;
  running_state BOOLEAN;
  runtable_ref  TEXT;
BEGIN

  -- Make sure project exists in ProcessTable
  SELECT TRUE FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO value_bool;
  IF value_bool IS NULL THEN
    RAISE WARNING '+++++++++ Project % does not exist... +++++++++',project_name;
    RETURN -1;
  END IF;

  SELECT MAX(ProjectVer) FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO current_ver;

  IF new_cmd IS NULL THEN
    SELECT Command FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_cmd;
  END IF;

  IF new_freq IS NULL THEN
    SELECT Frequency FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_freq;
  END IF;

  IF new_sleepAfter IS NULL THEN
    SELECT SleepAfter FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_sleepAfter;
  END IF;

  IF new_email IS NULL THEN
    SELECT Email FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_email;
  END IF;

  IF new_nodename IS NULL THEN
    SELECT Server FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_nodename;
  END IF;

  IF new_src IS NULL THEN
    SELECT Resource FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_src;
  END IF;

  IF new_en IS NULL THEN
    SELECT Enabled FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_en;
  END IF;

  IF new_run IS NULL THEN
    SELECT StartRun FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_run;
  END IF;

  IF new_subrun IS NULL THEN
    SELECT StartSubRun FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO new_subrun;
  END IF;

  SELECT Running FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO running_state;

  SELECT RefName FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = current_ver INTO runtable_ref;

  current_ver := current_ver + 1;

  query := format('INSERT INTO ProcessTable ( Project,
  	   		       		      RefName,
  	   		       		      Command,
					      ProjectVer,
					      Frequency,
					      SleepAfter,
					      StartRun,
					      StartSubRun,
					      Email,
					      Server,
					      Resource,
					      Enabled,
					      Running) 
			       VALUES ( ''%s'', ''%s'', ''%s'', %s, %s, %s,
			       	      	%s, %s, ''%s'', ''%s'', ''%s''::HSTORE, %s, %s)',
			       project_name,
			       runtable_ref,
			       new_cmd,
			       current_ver,
			       new_freq,
			       new_sleepAfter,
			       new_run,
			       new_subrun,
			       new_email,
			       new_nodename,
			       new_src::TEXT,
			       new_en::TEXT,
			       running_state::TEXT);
 
  EXECUTE query;

  SELECT ID FROM ProcessTable WHERE Project=project_name AND ProjectVer = current_ver INTO value_int;
  RETURN value_int;
  
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ListDaemonLog(TEXT,TIMESTAMP,TIMESTAMP);

CREATE OR REPLACE FUNCTION ListDaemonLog( NodeName TEXT,
       	  	  	   		  TStart TIMESTAMP,
					  TEnd   TIMESTAMP)
			   RETURNS TABLE( MaxProjCtr INT,
			   	   	  LifeTime   INT,
			   	   	  ProjCtr    INT,
					  UpTime     INT,
					  LogItem    HSTORE,
					  LogTime    TIMESTAMP) AS $$
DECLARE
BEGIN
  IF TStart IS NULL AND TEnd IS NULL THEN
    RETURN QUERY SELECT A.MaxProjCtr, A.LifeTime, A.ProjCtr, A.UpTime, A.LogItem, A.LogTime
	       	 FROM   DaemonLogTable AS A
	       	 WHERE  lower(Server) = lower(NodeName)
		 ORDER BY A.LogTime;
  ELSIF TStart IS NULL THEN
    RETURN QUERY SELECT A.MaxProjCtr, A.LifeTime, A.ProjCtr, A.UpTime, A.LogItem, A.LogTime
	       	 FROM   DaemonLogTable AS A
	       	 WHERE  lower(Server) = lower(NodeName) AND LogTime < TEnd
		 ORDER BY A.LogTime;
  ELSIF TEnd IS NULL THEN
    RETURN QUERY SELECT A.MaxProjCtr, A.LifeTime, A.ProjCtr, A.UpTime, A.LogItem, A.LogTime
	       	 FROM   DaemonLogTable AS A
	       	 WHERE  lower(Server) = lower(NodeName) AND LogTime > TStart
		 ORDER BY A.LogTime;
  ELSE
    RETURN QUERY SELECT A.MaxProjCtr, A.LifeTime, A.ProjCtr, A.UpTime, A.LogItem, A.LogTime
	       	 FROM   DaemonLogTable AS A
	       	 WHERE  lower(Server) = lower(NodeName) AND LogTime > TStart AND LogTime < TEnd
		 ORDER BY A.LogTime;
  END IF;
END;
$$ LANGUAGE PLPGSQL;


DROP FUNCTION IF EXISTS ListDaemonLog(TIMESTAMP,TIMESTAMP);
CREATE OR REPLACE FUNCTION ListDaemonLog( TStart TIMESTAMP,
					  TEnd   TIMESTAMP)
			   RETURNS TABLE( Server     TEXT,
			   	   	  MaxProjCtr INT,
			   	   	  LifeTime   INT,
			   	   	  ProjCtr    INT,
					  UpTime     INT,
					  LogItem    HSTORE,
					  LogTime    TIMESTAMP) AS $$
DECLARE
BEGIN
  IF TStart IS NULL AND TEnd IS NULL THEN
    RETURN QUERY SELECT A.Server, A.MaxProjCtr, A.LifeTime, A.ProjCtr, A.UpTime, A.LogItem, A.LogTime
	       	 FROM   DaemonLogTable AS A 
		 ORDER BY A.Server, A.LogTime;
  ELSIF TStart IS NULL THEN
    RETURN QUERY SELECT A.Server, A.MaxProjCtr, A.LifeTime, A.ProjCtr, A.UpTime, A.LogItem, A.LogTime
	       	 FROM   DaemonLogTable AS A
	       	 WHERE  LogTime < TEnd
		 ORDER BY A.Server, A.LogTime;
  ELSIF TEnd IS NULL THEN
    RETURN QUERY SELECT A.Server, A.MaxProjCtr, A.LifeTime, A.ProjCtr, A.UpTime, A.LogItem, A.LogTime
	       	 FROM   DaemonLogTable AS A
	       	 WHERE  LogTime > TStart
		 ORDER BY A.Server, A.LogTime;
  ELSE
    RETURN QUERY SELECT A.Server, A.MaxProjCtr, A.LifeTime, A.ProjCtr, A.UpTime, A.LogItem, A.LogTime
	       	 FROM   DaemonLogTable AS A
	       	 WHERE  LogTime > TStart AND LogTime < TEnd
		 ORDER BY A.Server, A.LogTime;
  END IF;
END;
$$ LANGUAGE PLPGSQL;
---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ListEnabledDaemon();

CREATE OR REPLACE FUNCTION ListEnabledDaemon() 
       	  	  	   RETURNS TABLE ( Server        TEXT, 
			   	   	   MaxProjCtr    INT,
					   LifeTime      INT,
					   LogRange      INT,
					   RunSyncPeriod INT,
					   UpdatePeriod  INT,
					   CleanUpPeriod INT,
					   EMail         TEXT,
					   LogTime       TIMESTAMP ) AS $$
DECLARE
BEGIN
  RETURN QUERY SELECT A.Server, A.MaxProjCtr, A.LifeTime, A.LogRange, A.RunSyncPeriod,
  	      	      A.UpdatePeriod, A.CleanUpPeriod, A.EMail, A.LogTime
	       FROM   DaemonTable AS A
	       WHERE  Enabled;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION ListDaemon() 
       	  	  	   RETURNS TABLE ( Server        TEXT, 
			   	   	   MaxProjCtr    INT,
					   LifeTime      INT,
					   LogRange      INT,
					   RunSyncPeriod INT,
					   UpdatePeriod  INT,
					   CleanUpPeriod INT,
					   EMail         TEXT,
					   Enabled       BOOLEAN,
					   LogTime       TIMESTAMP ) AS $$
DECLARE
BEGIN
  RETURN QUERY SELECT A.Server, A.MaxProjCtr, A.LifeTime, A.LogRange, A.RunSyncPeriod,
  	       	      A.UpdatePeriod, A.CleanUpPeriod, A.EMail, A.Enabled, A.LogTime
	       FROM   DaemonTable AS A;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ListEnabledProject();

CREATE OR REPLACE FUNCTION ListEnabledProject() 
       	  	  	   RETURNS TABLE ( Project TEXT, 
			   	   	   Command TEXT, 
					   Frequency INT,
					   SleepAfter INT,
					   RunTable TEXT,
					   StartRun INT,
					   StartSubRun INT,
					   Email TEXT,
					   Server TEXT,
					   Resource HSTORE,
					   ProjectVer SMALLINT) AS $$
DECLARE
BEGIN
  RETURN QUERY SELECT A.Project, A.Command, A.Frequency, A.SleepAfter, A.RefName, A.StartRun, A.StartSubRun,
  	       	      A.Email, A.Server, A.Resource, A.ProjectVer 
  		      FROM ProcessTable AS A JOIN 
  		      ( SELECT B.Project AS Project, MAX(B.ProjectVer) AS ProjectVer 
    		      FROM ProcessTable AS B 
    		      WHERE B.ENABLED GROUP BY B.Project) 
  		      AS FOO ON A.Project=FOO.Project AND A.ProjectVer = FOO.ProjectVer;
END;
$$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS ListProject();

CREATE OR REPLACE FUNCTION ListProject() 
       	  	  RETURNS TABLE ( Project TEXT, 
		   	   	  Command TEXT, 
				  Frequency INT,
				  SleepAfter INT,
				  RunTable TEXT,
				  StartRun INT,
				  StartSubRun INT,
				  Email TEXT,
				  Server TEXT,
				  Resource HSTORE,
				  Enabled  BOOLEAN,
				  ProjectVer SMALLINT) AS $$
DECLARE
BEGIN
  RETURN QUERY SELECT A.Project, A.Command, A.Frequency, A.SleepAfter, A.RefName, A.StartRun, A.StartSubRun,
  	       	      A.Email, A.Server, A.Resource, A.Enabled, A.ProjectVer 
  		      FROM ProcessTable AS A JOIN 
  		      ( SELECT B.Project AS Project, MAX(B.ProjectVer) AS ProjectVer 
    		      FROM ProcessTable AS B 
    		      GROUP BY B.Project) 
  		      AS FOO ON A.Project=FOO.Project AND A.ProjectVer = FOO.ProjectVer;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS AllProjectRunSynch();

CREATE OR REPLACE FUNCTION AllProjectRunSynch()
       	  	  	   RETURNS VOID AS $$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN SELECT Project, ProjectVer, StartRun, StartSubRun  FROM ListEnabledProject() LOOP
    EXECUTE OneProjectRunSynch(rec.Project, rec.ProjectVer, rec.StartRun, rec.StartSubRun);
  END LOOP;

END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS OneProjectRunSynch( project_name      TEXT,
					    project_ver  SMALLINT,
					    start_run    INT,
					    start_subrun INT );

--
-- This function will get a new argument and codechanges such that MainRun -> OnlineDAQdb.MainRun below, and then
-- we will get our mainruns from the table the DAQ is actually filling from assemblerApp and sebApp.
--
CREATE OR REPLACE FUNCTION OneProjectRunSynch( project_name      TEXT,
					       project_ver  SMALLINT,
					       start_run    INT,
					       start_subrun INT ) RETURNS VOID AS $$
DECLARE
  runtable TEXT;
  query TEXT;
BEGIN

  -- Identify a reference table --
  SELECT RefName FROM ProcessTable WHERE Project=project_name AND ProjectVer=project_ver INTO runtable;
  
  IF NOT DoesTableExist(runtable) THEN
    RAISE EXCEPTION '% table does not exist!', runtable;
  END IF;

  query := format( 'SELECT %s.RunNumber AS Run, %s.SubRunNumber AS SubRun, 0, %s, 1 
  	   	    FROM %s LEFT JOIN %s ON %s.Run=%s.RunNumber AND %s.SubRun=%s.SubRunNumber AND %s.ProjectVer=%s
		    WHERE (%s.Run IS NULL AND %s.SubRun IS NULL)  AND
		    (%s.RunNumber>%s OR (%s.RunNumber=%s AND %s.SubRunNumber>=%s))
		    ORDER BY %s.RunNumber, %s.SubRunNumber',
  	   	   runtable, runtable, project_ver,
		   runtable, project_name, project_name, runtable, project_name, runtable, project_name, project_ver,
		   project_name, project_name, 
		   runtable, start_run, runtable, start_run, runtable, start_subrun,
		   runtable, runtable );

  query := format(' INSERT INTO %s (%s)', project_name, query);
            	   
  EXECUTE query;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--
-- This function will get a new argument and codechanges such that MainRun -> OnlineDAQdb.MainRun below, and then
-- we will get our mainruns from the table the DAQ is actually filling from assemblerApp and sebApp.
--
CREATE OR REPLACE FUNCTION OneProjectRunSynch( project_name TEXT ) RETURNS VOID AS $$
DECLARE
  query TEXT;
  rec RECORD;
BEGIN

  IF NOT DoesProjectExist(project_name) THEN
    RAISE EXCEPTION '% project does not exist!', project_name;
  END IF;

  SELECT RunTable,StartRun,StartSubRun,ProjectVer FROM ProjectInfo(project_name) INTO rec;

  IF NOT DoesTableExist(rec.RunTable) THEN
    RAISE EXCEPTION '% table does not exist!', rec.RunTable;
  END IF;

  query := format( 'SELECT %s.RunNumber AS Run, %s.SubRunNumber AS SubRun, 0, %s, 1 
  	   	    FROM %s LEFT JOIN %s ON %s.Run=%s.RunNumber AND %s.SubRun=%s.SubRunNumber AND %s.ProjectVer=%s
		    WHERE (%s.Run IS NULL AND %s.SubRun IS NULL)  AND
		    (%s.RunNumber>%s OR (%s.RunNumber=%s AND %s.SubRunNumber>=%s))
		    ORDER BY %s.RunNumber, %s.SubRunNumber',
  	   	   rec.RunTable, rec.RunTable, rec.ProjectVer,
		   rec.RunTable, project_name, project_name, rec.RunTable, project_name, rec.RunTable, project_name, rec.ProjectVer,
		   project_name, project_name, 
		   rec.RunTable, rec.StartRun, rec.RunTable, rec.StartRun, rec.RunTable, rec.StartSubRun,
		   rec.RunTable, rec.RunTable );

  query := format(' INSERT INTO %s (%s)', project_name, query);
            	   
  EXECUTE query;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ProjectInfo(project_name TEXT);
DROP FUNCTION IF EXISTS ProjectInfo(project_name TEXT, project_ver INT);

CREATE OR REPLACE FUNCTION ProjectInfo( project_name TEXT,
       	  	  	   		project_ver INT DEFAULT NULL)

       	  	  	   		RETURNS TABLE ( Project TEXT, 
			      	     	   	        Command TEXT, 
					   	      	Frequency INT,
							SleepAfter INT,
							RunTable TEXT,							
					   	      	StartRun INT,
					 	      	StartSubRun INT,
					   	      	Email TEXT,
							Server TEXT,
					   	      	Resource HSTORE,
					   	      	ProjectVer SMALLINT,
							Enabled BOOLEAN) AS $$
DECLARE
is_there BOOLEAN;
BEGIN
  IF NOT DoesProjectExist(project_name) THEN
    RAISE EXCEPTION 'Project % does not exist!',project_name;
  END IF;
  IF project_ver IS NULL THEN
    SELECT A.ProjectVer FROM ProcessTable AS A
    	   WHERE lower(A.Project) = lower(project_name)
	   ORDER BY A.ProjectVer 
	   DESC LIMIT 1
	   INTO project_ver;
  END IF;

  RETURN QUERY SELECT A.Project, A.Command, A.Frequency, A.SleepAfter, A.RefName,
  	       	      A.StartRun, A.StartSubRun, A.Email, A.Server,
		      A.Resource, A.ProjectVer, A.Enabled
		      FROM ProcessTable AS A 
		      WHERE lower(A.Project) = lower(project_name) AND A.ProjectVer = project_ver;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ProjectResource(TEXT);
CREATE OR REPLACE FUNCTION ProjectResource( project_name TEXT)
       	  	  	   		   RETURNS HSTORE AS $$
DECLARE
res_resource HSTORE;

BEGIN
  IF NOT DoesProjectExist(project_name) THEN
    RAISE EXCEPTION 'Project % does not exist!',project_name;
  END IF;

  SELECT Resource FROM ProcessTable 
  	 	  WHERE lower(Project) = lower(project_name)
		  ORDER BY ProjectVer DESC
		  LIMIT 1
		  INTO res_resource;

  RETURN res_resource;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ProjectInfo( project_name TEXT, 
     	      	 		     project_info TEXT,
     	      	 		     version SMALLINT);

CREATE OR REPLACE FUNCTION ProjectInfo( project_name TEXT, 
          	      	 		project_info TEXT,
     	      	 			version SMALLINT DEFAULT -1) RETURNS RECORD AS $$
DECLARE
  project_validity BOOLEAN;
  project_ver INT;
  query TEXT;
  rec RECORD;
BEGIN

  rec := NULL;
  project_info := lower(project_info);
  SELECT TRUE FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO project_validity;
  IF project_validity IS NULL THEN
    RAISE WARNING '+++++++++ There is no such project: % +++++++++',project_name;
  END IF;
  SELECT TRUE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='processtable' AND COLUMN_NAME=project_info INTO project_validity;
  IF project_validity IS NULL THEN
    RAISE WARNING '+++++++++ There is no such info: % +++++++++',project_info;
    RETURN rec;
  END IF;

  IF version < 0 THEN
    SELECT Max(ProjectVer) FROM ProcessTable WHERE lower(Project) = lower(project_name) INTO project_ver;
  ELSE
    SELECT TRUE FROM ProcessTable WHERE lower(Project) = lower(project_name) AND ProjectVer = version INTO project_validity;
    IF project_validity IS NULL THEN
      RAISE WARNING '++++++++++ Project % does not have a version % +++++++++',project_name,version;
      RETURN rec;
    END IF;
  END IF;
  
  query := format('SELECT %s FROM ProcessTable WHERE lower(Project)=lower(''%s'') AND ProjectVer=%s',project_info,project_name,project_ver);
  EXECUTE query INTO rec;
  RETURN rec;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS GetStatusSummary();

CREATE OR REPLACE FUNCTION GetStatusSummary()
       	  	  RETURNS TABLE ( Project TEXT, NRuns INT, NSubRuns INT, Status SMALLINT )
		  AS $$
DECLARE
BEGIN

END;
$$ LANGUAGE PLPGSQL;


