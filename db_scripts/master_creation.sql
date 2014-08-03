
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

--Check if a table already exists. Used by many other functions here.
DROP FUNCTION IF EXISTS DoesProcessExist(TEXT);
CREATE OR REPLACE FUNCTION DoesProjectExist(tname TEXT) RETURNS BOOLEAN AS $$
DECLARE
doesExist BOOLEAN;
rec  RECORD;
BEGIN
	
  IF NOT DoesTableExist('processtable') THEN
    RETURN FALSE;
  END IF;

  SELECT TRUE FROM ProcessTable WHERE Project = tname LIMIT 1 INTO doesExist;
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
myRec   RECORD;
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
  IF myBool IS NULL THEN
    RAISE INFO '+++++++++ ProcessTable does not exist yet++++++++++';
    RETURN;
  END IF;

  -- For projects in ProcessTable, attempt to drop its project table & remove entry from ProcessTable
  FOR myRec IN SELECT Project FROM ProcessTable LOOP
    EXECUTE RemoveProject(myRec.Project);
  END LOOP;
  DROP TABLE ProcessTable;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- (Re-)create TestRunTable
DROP FUNCTION IF EXISTS CreateTestRunTable();

CREATE OR REPLACE FUNCTION CreateTestRunTable() RETURNS VOID AS $$

  DROP TABLE IF EXISTS MainRun;
  
  CREATE TABLE MainRun ( RunNumber    INT NOT NULL,
       	     	    	 SubRunNumber INT NOT NULL,
			 TimeStart    TIMESTAMP NOT NULL,
			 TimeStop     TIMESTAMP NOT NULL,
			 ConfigID     INT NOT NULL,
			 PRIMARY KEY (RunNumber,SubRunNumber) );

$$ LANGUAGE SQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
DROP FUNCTION IF EXISTS InsertIntoTestRunTable( Run INT, SubRun INT, 
     	      	 				TimeStart TIMESTAMP,
						TimeEnd   TIMESTAMP);

CREATE OR REPLACE FUNCTION InsertIntoTestRunTable( Run INT, SubRun INT, 
     	      	 				   TStart TIMESTAMP,
						   TEnd   TIMESTAMP) RETURNS VOID AS $$
DECLARE
  presence BOOLEAN;
BEGIN

  IF Run <= 0 OR SubRun <= 0 THEN
    RAISE EXCEPTION 'Run/SubRun must be a positive integer!';
  END IF;

  IF NOT DoesTableExist('mainrun') THEN
    RAISE EXCEPTION 'MainRun table does not exist!';
  END IF;

  SELECT TRUE FROM MainRun WHERE RunNumber = Run AND SubRunNumber = SubRun INTO presence;

  IF presence IS NOT NULL THEN
    RAISE EXCEPTION 'Specified (Run,SubRun) already exists in the MainRun Table!';
  END IF;
  INSERT INTO MainRun (RunNumber,SubRunNumber,TimeStart,TimeStop,ConfigID) 
  	      VALUES  (Run, SubRun, TStart, TEnd, 0);
  RETURN;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- (Re-)create & fill TestRunTable

DROP FUNCTION IF EXISTS FillTestRunTable( NumRuns    INT,
     	      	 			  NumSubRuns INT);

CREATE OR REPLACE FUNCTION FillTestRunTable( NumRuns    INT DEFAULT NULL,
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

  EXECUTE 'SELECT CreateTestRunTable();';

  run_start := '2015-01-01 00:00:00'::TIMESTAMP;

  FOR run IN 1..NumRuns LOOP
    FOR subrun IN 1..NumSubRuns LOOP
      run_start := run_start + INTERVAL '20 minutes';
      run_end   := run_start + INTERVAL '10 minutes';
      EXECUTE InsertIntoTestRunTable(run,subrun,run_start,run_end);
    END LOOP;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS GetRunTimeStamp(Run INT, SubRun INT);

CREATE OR REPLACE FUNCTION GetRunTimeStamp ( Run INT, SubRun INT) 
       	  	  RETURNS TABLE (TimeStart TIMESTAMP, TimeStop TIMESTAMP) AS $$
DECLARE
BEGIN
 SELECT TimeStart, TimeStop FROM MainRun WHERE RunNumber = Run AND SubRunNumber = SubRun LIMIT 1;
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
BEGIN
  IF NOT DoesProjectExist(project_name) THEN
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

CREATE TABLE IF NOT EXISTS ProcessTable ( ID          SERIAL,
       	     		    		  Project     TEXT      NOT NULL,
			    		  ProjectVer  SMALLINT  NOT NULL,
			    		  Command     TEXT      NOT NULL,
       	     		    		  Frequency   INT       NOT NULL,
			    		  EMail       TEXT      NOT NULL,
			    		  Resource    HSTORE    NOT NULL,
			    		  StartRun    INT       NOT NULL DEFAULT 0,
			    		  StartSubRun INT       NOT NULL DEFAULT 0,
			    		  Enabled     BOOLEAN   DEFAULT TRUE,
			    		  Running     BOOLEAN   NOT NULL,
			    		  LogTime     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
					  PRIMARY KEY(ID,Project,ProjectVer));

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

-- Function to update project version
DROP FUNCTION IF EXISTS UpdateProjectConfig( project_name TEXT,
     	      	 		       	     command      TEXT,
				       	     frequency    INT,
				       	     email        TEXT,
				       	     resource     HSTORE,
				       	     enabled      BOOLEAN);

CREATE OR REPLACE FUNCTION UpdateProjectConfig( project_name TEXT,
     	      	 		       	     	command      TEXT DEFAULT NULL,
				       	     	frequency    INT  DEFAULT NULL,
				       	     	email        TEXT DEFAULT NULL,
				       	     	resource     HSTORE  DEFAULT NULL,
				       	     	enabled      BOOLEAN DEFAULT NULL) RETURNS BOOLEAN AS $$
DECLARE
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

  IF command IS NULL AND frequency IS NULL AND email IS NULL AND resource IS NULL AND enabled IS NULL THEN
    RAISE EXCEPTION '+++++++++ Nothing to update! +++++++++';
    RETURN FALSE;
  END IF;

  SELECT MAX(ProjectVer) FROM ProcessTable WHERE Project = project_name INTO project_ver; 

  query := 'UPDATE ProcessTable SET ';

  IF NOT command IS NULL THEN
    query := format('%s Command=''%s'',',query,command);
  END IF;

  IF NOT frequency IS NULL THEN
    query := format('%s Frequency=%s,',query,frequency);
  END IF;

  IF NOT email IS NULL THEN
    query := format('%s Email=''%s'',',query,email);
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

  query := format('%s WHERE ProjectVer=%s AND Project=''%s''',query,project_ver,project_name);

  EXECUTE query;

  RETURN TRUE;

END;
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

CREATE OR REPLACE FUNCTION ProjectVersionUpdate( project_name TEXT,
     	      	 		       	      	 new_cmd      TEXT DEFAULT NULL,
				       	      	 new_freq     INT  DEFAULT NULL,
				       	      	 new_email    TEXT DEFAULT NULL,
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
BEGIN

  -- Make sure project exists in ProcessTable
  SELECT TRUE FROM ProcessTable WHERE Project = project_name INTO value_bool;
  IF value_bool IS NULL THEN
    RAISE WARNING '+++++++++ Project % does not exist... +++++++++',project_name;
    RETURN -1;
  END IF;

  SELECT MAX(ProjectVer) FROM ProcessTable WHERE Project = project_name INTO current_ver;

  IF new_cmd IS NULL THEN
    SELECT Command FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO new_cmd;
  END IF;

  IF new_freq IS NULL THEN
    SELECT Frequency FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO new_freq;
  END IF;

  IF new_email IS NULL THEN
    SELECT Email FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO new_email;
  END IF;

  IF new_src IS NULL THEN
    SELECT Resource FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO new_src;
  END IF;

  IF new_en IS NULL THEN
    SELECT Enabled FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO new_en;
  END IF;

  IF new_run IS NULL THEN
    SELECT StartRun FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO new_run;
  END IF;

  IF new_subrun IS NULL THEN
    SELECT StartSubRun FROM ProcessTable WHERE Project = project_name AND ProjectVer = current_ver INTO new_subrun;
  END IF;

  current_ver := current_ver + 1;

  query := format('INSERT INTO ProcessTable ( Project,
  	   		       		      Command,
					      ProjectVer,
					      Frequency,
					      StartRun,
					      StartSubRun,
					      Email,
					      Resource,
					      Enabled,
					      Running) 
			       VALUES ( ''%s'', ''%s'', %s, %s, 
			       	      	%s, %s, ''%s'', ''%s''::HSTORE, %s, FALSE)',
			       project_name,
			       new_cmd,
			       current_ver,
			       new_freq,
			       new_run,
			       new_subrun,
			       new_email,
			       new_src::TEXT,
			       new_en::TEXT);
 
  EXECUTE query;

  SELECT ID FROM ProcessTable WHERE Project=project_name AND ProjectVer = current_ver INTO value_int;
  RETURN value_int;
  
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
					   StartRun INT,
					   StartSubRun INT,
					   Email TEXT, 
					   Resource HSTORE,
					   ProjectVer SMALLINT) AS $$
  SELECT A.Project, A.Command, A.Frequency, A.StartRun, A.StartSubRun,
  	 A.Email, A.Resource, A.ProjectVer 
  FROM ProcessTable AS A JOIN 
  (SELECT Project, MAX(ProjectVer) AS ProjectVer FROM ProcessTable WHERE ENABLED GROUP BY Project) 
  AS FOO ON A.Project=FOO.Project AND A.ProjectVer = FOO.ProjectVer;
$$ LANGUAGE SQL;

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

DROP FUNCTIOn IF EXISTS OneProjectRunSynch( project      TEXT,
					    project_ver  SMALLINT,
					    start_run    INT,
					    start_subrun INT );

CREATE OR REPLACE FUNCTION OneProjectRunSynch( project      TEXT,
					       project_ver  SMALLINT,
					       start_run    INT,
					       start_subrun INT ) RETURNS VOID AS $$
DECLARE
  query TEXT;
BEGIN
  IF NOT DoesTableExist('mainrun') THEN
    RAISE EXCEPTION 'MainRun table does not exist!';
  END IF;

  query := format(' INSERT INTO %s 
  	   	    ( SELECT MainRun.RunNumber AS Run, MainRun.SubRunNumber AS SubRun, 0, %s, 1
		       FROM MainRun LEFT JOIN %s ON %s.Run=MainRun.RunNumber AND %s.SubRun=MainRun.SubRunNumber
		       WHERE %s.Run IS NULL AND 
		       	     %s.SubRun IS NULL AND 
			     (MainRun.RunNumber>%s OR (MainRun.RunNumber=%s AND MainRun.SubRunNumber>=%s))
		       ORDER BY MainRun.RunNumber, MainRun.SubRunNumber)',
		    project,
		    project_ver,
		    project,
		    project,
		    project,
		    project,
		    project,
		    start_run,
		    start_run,
		    start_subrun);
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
					   	      	StartRun INT,
					 	      	StartSubRun INT,
					   	      	Email TEXT, 
					   	      	Resource HSTORE,
					   	      	ProjectVer SMALLINT) AS $$
DECLARE
is_there BOOLEAN;
BEGIN
  IF NOT DoesProjectExist(project_name) THEN
    RAISE EXCEPTION 'Project % does not exist!',project_name;
  END IF;
  IF project_ver IS NULL THEN
    SELECT A.ProjectVer FROM ProcessTable AS A
    	   WHERE A.Project = project_name 
	   ORDER BY A.ProjectVer 
	   DESC LIMIT 1
	   INTO project_ver;
  END IF;

  RETURN QUERY SELECT A.Project, A.Command, A.Frequency, A.StartRun, 
  	       	      A.StartSubRun, A.Email, A.Resource, A.ProjectVer 
		      FROM ProcessTable AS A 
		      WHERE A.Project = project_name AND A.ProjectVer = project_ver;
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
  SELECT TRUE FROM ProcessTable WHERE Project = project_name INTO project_validity;
  IF project_validity IS NULL THEN
    RAISE WARNING '+++++++++ There is no such project: % +++++++++',project_name;
  END IF;
  SELECT TRUE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='processtable' AND COLUMN_NAME=project_info INTO project_validity;
  IF project_validity IS NULL THEN
    RAISE WARNING '+++++++++ There is no such info: % +++++++++',project_info;
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
  
  query := format('SELECT %s FROM ProcessTable WHERE Project=''%s'' AND ProjectVer=%s',project_info,project_name,project_ver);
  EXECUTE query INTO rec;
  RETURN rec;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

			    


