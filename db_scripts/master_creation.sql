
---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

--Check if a table already exists. Used by many other functions here.
DROP FUNCTION IF EXISTS DoesTableExist(TEXT);
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
-- Function to clear ALL projects registered in ProcessTable
DROP FUNCTION IF EXISTS RemoveProcessDB();
CREATE OR REPLACE FUNCTION RemoveProcessDB() RETURNS VOID AS $$
DECLARE
myBool  BOOLEAN;
myRec   RECORD;
myQuery TEXT;
BEGIN
  -- Check if ProcessTable exists. If not, don't throw exception but simply return
  SELECT DoesTableExist('ProcessTable') INTO myBool;
  IF myBool IS NULL THEN
    RAISE INFO '+++++++++ ProcessTable does not exist yet++++++++++';
    RETURN;
  END IF;

  -- For projects in ProcessTable, attempt to drop its project table & remove entry from ProcessTable
  FOR myRec IN SELECT Project FROM ProcessTable LOOP
    SELECT DoesTableExist(myRec.Project) INTO myBool;
    IF myBool IS NULL THEN
      RAISE INFO '+++++++++ Project % table does not exist but found in ProcessTable ?! +++++++++++',myRec.Project;
    ELSE
      myQuery := format('DROP TABLE %s',myRec.Project);
      EXECUTE myQuery;
      RAISE INFO 'Dropped table project %',myRec.Project;
      DELETE FROM ProcessTable WHERE Project = myRec.Project;
      RAISE INFO 'Dropped project %',myRec.Project;
    END IF;
  END LOOP;
  DROP TABLE ProcessTable;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- (Re-)create TestRunTable

DROP FUNCTION IF EXISTS FillTestRunTable( NumRuns    INT,
     	      	 			  NumSubRuns INT);

CREATE OR REPLACE FUNCTION FillTestRunTable( NumRuns    INT DEFAULT NULL,
     	      	 			     NumSubRuns INT DEFAULT NULL) RETURNS VOID AS $$
DECLARE
  run_start TIMESTAMP;
  make_newone BOOLEAN;
BEGIN

  IF NumRuns IS NULL THEN NumRuns := 100; END IF;
  IF NumSubRuns IS NULL THEN NumSubRuns := 10; END IF;

  IF NumRuns < 0 OR NumSubRuns < 0 THEN
    RAISE EXCEPTION 'Provide positive run/sub-run number (%/%)!',NumRuns,NumSubRuns;
  END IF;

  --IF DoesTableExist('MainRun') THEN
    --RAISE EXCEPTION 'MainRun table already exist!';
  --END IF;
  
  DROP TABLE IF EXISTS MainRun;
  
  run_start := '2015-01-01 00:00:00'::TIMESTAMP;

  CREATE TABLE MainRun ( RunNumber    INT NOT NULL,
       	     	    	 SubRunNumber INT NOT NULL,
			 TimeStart    TIMESTAMP NOT NULL,
			 PRIMARY KEY (RunNumber,SubRunNumber) );

  FOR run IN 1..NumRuns LOOP
    FOR subrun IN 1..NumSubRuns LOOP
      run_start := run_start + INTERVAL '20 minutes';
      INSERT INTO MainRun (RunNumber,SubRunNumber,TimeStart) VALUES (run,subrun,run_start);
    END LOOP;
  END LOOP;
END;
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
BEGIN
  IF NOT DoesTableExist(project_name) THEN
    RAISE EXCEPTION 'Project % does not exist!', project_name;
  END IF;

  SELECT MAX(RunNumber) FROM MainRun INTO start_run;
  IF start_run IS NULL THEN
    RAISE EXCEPTION 'MainRun table contains no run number!';
  END IF;

  SELECT MAX(SubRunNumber) FROM MainRun WHERE RunNumber = start_run INTO start_subrun;
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

CREATE TABLE IF NOT EXISTS ProcessTable ( ID          SERIAL    PRIMARY KEY,
       	     		    		  Project     TEXT      NOT NULL UNIQUE,
			    		  ProjectVer  SMALLINT  NOT NULL,
			    		  Command     TEXT      NOT NULL,
       	     		    		  Frequency   INT       NOT NULL,
			    		  EMail       TEXT      NOT NULL,
			    		  Resource    HSTORE    NOT NULL,
			    		  StartRun    INT       NOT NULL DEFAULT 0,
			    		  StartSubRun INT       NOT NULL DEFAULT 0,
			    		  Enabled     BOOLEAN   DEFAULT TRUE,
			    		  Running     BOOLEAN   NOT NULL,
			    		  LogTime     TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

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
    IF NOT (SELECT DoesTableExist(t)) THEN 
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


CREATE OR REPLACE FUNCTION DefineProject( project_name TEXT,
     	      	 		       	  command      TEXT,
				       	  frequency    INT,
				       	  email        TEXT,
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
  SELECT TRUE FROM ProcessTable WHERE Project = project_name INTO myBool;
  IF NOT myBool IS NULL THEN
    RAISE WARNING '+++++++++ Project % already exists... +++++++++',project_name;
    RETURN -1;
  END IF;

  -- Get the version number
  SELECT MAX(ProjectVer) FROM ProcessTable WHERE Project = project_name INTO myVersion;
  IF myVersion IS NULL THEN
    myVersion := 0;
  ELSE
    myVersion := myVersion + 1;
  END IF;

  -- Insert into ProcessTable
  INSERT INTO ProcessTable (Project,Command,ProjectVer,Frequency,StartRun,StartSubRUn,Email,Resource,Enabled,Running) VALUES (project_name, command, myVersion, frequency, start_run, start_subrun, email, resource, enabled, FALSE);
  SELECT ID FROM ProcessTable WHERE Project = project_name INTO myInt;
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

  SELECT MAX(ID) FROM ProcessTable WHERE Project = project_name INTO myInt;
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

CREATE OR REPLACE FUNCTION UpdateProjectConfig( project_name TEXT,
     	      	 		       	     	command      TEXT DEFAULT NULL,
				       	     	frequency    INT  DEFAULT NULL,
				       	     	email        TEXT DEFAULT NULL,
				       	     	resource     HSTORE  DEFAULT NULL,
				       	     	enabled      BOOLEAN DEFAULT NULL,
					     	myversion    INT  DEFAULT NULL) RETURNS BOOLEAN AS $$
DECLARE
query_field TEXT;
query_value TEXT;
project_ver INT;
query       TEXT;
myBool      BOOLEAN;
BEGIN
  
  -- Make sure project exists in ProcessTable
  SELECT TRUE FROM ProcessTable WHERE Project = project_name INTO myBool;
  IF myBool IS NULL THEN
    RAISE WARNING '+++++++++ Project % does not exist... +++++++++',project_name;
    RETURN FALSE;
  END IF;

  SELECT MAX(ProjectVer) FROM ProcessTable WHERE Project = project_name INTO project_ver; 
  IF myversion IS NOT NULL AND myversion >= project_ver THEN 

  IF NOT project_ver IS NULL THEN
    IF myversion IS NOT NULL THEN
    SELECT ProjectVer FROM ProcessTable WHERE Project = project_name AND ProjectVer = myversion INTO project_ver;
    IF project_ver IS NULL THEN
      RAISE WARNING '+++++++++ Project % does not have a version number % +++++++++',project_name,myversion;
      RETURN FALSE;
    END IF;
  END IF;


  IF NOT command IS NULL THEN
    query_field := query_field || 'Command,';
    query_value := query_value||''||command||''',';
  END IF;

  IF NOT frequency IS NULL THEN
    query_field := query_field || 'Frequency,';
    query_value := format('%s%d,',query_value,frequency);
  END IF;

  IF NOT email IS NULL THEN
    query_field := query_field || 'Email,';
    query_value := query_value||''||email||''',';
  END IF;

  IF NOT resource IS NULL THEN
    query_field := query_field || 'Resource,';
    query_value := query_value||''||resource||''',';
  END IF;

  IF NOT enabled IS NULL THEN
    query_field := query_field || 'Enabled,';
    IF enabled IS TRUE THEN
      query_value := query_value || 'TRUE,';
    ELSE
      query_value := query_value || 'FALSE,';
    END IF;
  END IF;

  query_field := TRIM( TRAILING ',' FROM query_field);
  query_value := TRIM( TRAILING ',' FROM query_value);    

  query := format('UPDATE ProcessTable SET (%s) = (%s) WHERE ProjectVer = %d',query_field,query_value,project_ver);

  EXECUTE query;

  RETURN TRUE;

END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Function to update project version
DROP FUNCTION IF EXISTS ProjectVersionUpdate( project_name TEXT,
     	      	 		       	      command      TEXT,
				       	      frequency    INT,
				       	      email        TEXT,
					      start_time   TIMESTAMP,
				       	      resource     HSTORE,
				       	      enabled      BOOLEAN);

CREATE OR REPLACE FUNCTION ProjectVersionUpdate( project_name TEXT,
     	      	 		       	      	 command      TEXT DEFAULT NULL,
				       	      	 frequency    INT  DEFAULT NULL,
				       	      	 email        TEXT DEFAULT NULL,
					      	 start_time   TIMESTAMP DEFAULT NULL,
				       	      	 resource     HSTORE DEFAULT NULL,
				       	    	 enabled      BOOLEAN DEFAULT NULL) RETURNS INT AS $$
DECLARE
  current_ver  INT;
  value_int    INT;
  value_bool   BOOLEAN;
  query_field  TEXT;
  query_values TEXT;
  query        TEXT;
BEGIN

  IF start_time IS NULL THEN
    start_time := '2014-01-01 00:00:00'::TIMESTAMP;
  END IF;

  -- Make sure project exists in ProcessTable
  SELECT TRUE FROM ProcessTable WHERE Project = project_name INTO value_bool;
  IF value_bool IS NULL THEN
    RAISE WARNING '+++++++++ Project % does not exist... +++++++++',project_name;
    RETURN -1;
  END IF;

  SELECT MAX(ProjectVer) FROM ProcessTable WHERE Project = project_name INTO current_ver;

  IF command IS NULL THEN
    SELECT Command FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO command;
  END IF;

  IF frequency IS NULL THEN
    SELECT Frequency FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO frequency;
  END IF;

  IF email IS NULL THEN
    SELECT Email FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO email;
  END IF;

  IF resource IS NULL THEN
    SELECT Resource FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO resource;
  END IF;

  IF enabled IS NULL THEN
    SELECT Enabled FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO enabled;
  END IF;

  current_ver := current_ver + 1;


  query := format('INSERT INTO ProcessTable (Project,Command,ProjectVer,Frequency,TimeStart,Email,Resource,Enabled,Running) VALUES (%s, %s, %d, %d, ''%s'', %s, %s, %s, FALSE)',project_name,command,current_ver,frequency,start_time::TEXT,email,resource::TEXT,enabled::TEXT);
 
  EXECUTE query;

  SELECT ID FROM ProcessTable WHERE Project=project_name AND ProjectVer = current_ver INTO value_int;
  RETURN value_int;
  
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ProjectInfo( project_name TEXT, 
     	      	 		     project_info TEXT,
     	      	 		     version SMALLINT);

CREATE OR REPLACE FUNCTION ProjectInfo( project_name TEXT, 
     	      	 			version SMALLINT DEFAULT -1) RETURNS RECORD AS $$
DECLARE
  project_validity BOOLEAN;
  project_ver INT;
  query TEXT;
  rec RECORD;
BEGIN

  rec := NULL;

  SELECT TRUE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=project_name AND COLUMN_NAME=project_info INTO project_validity;
  IF project_validity IS NULL THEN
    RAISE WARNING '+++++++++ There is no such table/column: %/% +++++++++',project_name,project_info;
    RETURN rec;
  END IF;

  IF version < 0 THEN
    SELECT Max(ProjectVer) FROM ProcessTable WHERE Project = project_name INTO project_ver;
  ELSE
    SELECT TRUE FROM ProcessTable WHERE Project = project_name AND ProjectVer = version INTO project_validity;
    IF project_validity IS NULL THEN
      RAISE WARNING '++++++++++ Project % does not have a version % +++++++++',project_name,version;
      RETURN rec;
    END IF;
  END IF;
  
  query := format('SELECT %s FROM ProcessTable WHERE Project=%s AND ProjectVer=%d',project_info,project_name,project_ver);
  EXECUTE query INTO rec;
  RETURN rec;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

			    


