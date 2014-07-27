
---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- Function to clear ALL projects registered in ProcessTable
DROP FUNCTION IF EXISTS RemoveProcessDB() RETURNS VOID AS $$
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
      RAISE INFO '+++++++++ Project % table does not exist but found in ProcessTable but ?! +++++++++++',myRec.Project;
    ELSE
      query := format('DROP TABLE %s',myRec.Project);
      EXECUTE query;
      DELETE FROM ProcessTable WHERE Project = myRec.Project;
    END IF;
  END LOOP;
  RETURN;
END;
$$ LANGUAGE PLPGSQL;


---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
-- (Re-)create ProcessTable
DROP TABLE IF EXISTS ProcessTable;

CREATE TABLE ProcessTable ( ID        SERIAL    PRIMARY KEY,
       	     		    Project   TEXT      NOT NULL UNIQUE,
			    Version   SMALLINT  NOT NULL,
			    Command   TEXT      NOT NULL,
       	     		    Frequency INT       NOT NULL,
			    EMail     TEXT      NOT NULL,
			    Resource  HSTORE    NOT NULL,
			    Enabled   BOOLEAN   DEFAULT TRUE,
			    Running   BOOLEAN   NOT NULL,
			    LogTime   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP );

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
BEGIN
  -- Check ProcessTable presence
  IF NOT EXISTS ProcessTable THEN
    RAISE WARNING '++++++++++ ProcessTable must exist! ++++++++++';
    RETURN FALSE;
  END IF;
  -- Check Project table presence
  FOR t IN SELECT Project FROM ProcessTable LOOP
    IF NOT SELECT DoesTableExist(t) THEN 
      RAISE WARNING 'Project % has no table! ',t;
      RETURN FALSE;
    END IF;
  END LOOP; 
  RETRUN TRUE;
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
				       resource     HSTORE DEFAULT '',
				       enabled      DEFAULT TRUE );				       


CREATE OR REPLACE FUNCTION DefineProject( project_name TEXT NOT NULL,
     	      	 		       	  command      TEXT NOT NULL,
				       	  frequency    INT  NOT NULL,
				       	  email        TEXT NOT NULL,
				       	  resource     HSTORE NOT NULL DEFAULT '',
				       	  enabled      BOOLEAN NOT NULL DEFAULT TRUE) RETURNS INT AS $$
DECLARE
myBool    BOOLEAN;
myInt     INT;
myVersion SMALLINT;
BEGIN
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

  -- Attempt to make this project table
  SELECT MakeProjTable(project_name) INTO myInt;
  IF myInt IS NULL THEN
    RAISE EXCEPTION '+++++++++ Failed to create a project table for %! ++++++++++',project_name;
    RETURN -1;
  END IF;

  -- Get the version number
  SELECT MAX(Version) FROM ProcessTable WHERE Project = project_name INTO myVersion;
  IF myVersion IS NULL:
    myVersion  = 0;
  ELSE
    myVersion += 1;
  END IF;

  -- Insert into ProcessTable
  INSERT INTO ProcessTable (Project,Command,Version,Frequency,Email,Resource,Enabled,Running) VALUES (project_name, command, myVersion, frequency, email, resource, enabled, FALSE);
  SELECT ID FROM ProcessTable WHERE Project = project_name INTO myInt;
  IF myInt IS NULL THEN
    RAISE EXCEPTION '+++++++++ Somehow failed to insert project %! +++++++++', project_name;
    RETURN -1;
  END IF;

  SELECT MAX(ID) FROM ProcessTable WHERE Project = project_name INTO myInt;
  RETURN myInt;

END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ProjectResource( project_name TEXT, 
     	      	 			 version SMALLINT DEFAULT -1) RETURNS HSTORE AS $$
DECLARE
  params HSTORE;
BEGIN
  IF version < 0:
    SELECT Resource FROM ProcessTable WHERE Project = project_name ORDER BY Version DESC LIMIT 1 INTO params;
    IF params IS NULL THEN
      RAISE WARNING '+++++++++ Project % not found ++++++++++', project_name;
      params := ''::HSTORE;
    END IF;
  ELSE
    SELECT Resource FROM ProcessTable WHERE Project = project_name AND Version = version INTO params;
    IF params IS NULL THEN
      RAISE WARNING '+++++++++ Project % Version % not found ++++++++++', project_name, version;
      params := ''::HSTORE;
    END IF;
  RETURN params;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------

DROP FUNCTION IF EXISTS ProjectVersion( project_name TEXT, 
     	      	 		 	version SMALLINT DEFAULT -1) RETURNS HSTORE AS $$
DECLARE
  params HSTORE;
BEGIN
  SELECT Version FROM ProcessTable WHERE Project = project_name INTO params ORDER BY Version DESC LIMIT 1;
  IF params IS NULL THEN
    RAISE WARNING '+++++++++ Project % not found ++++++++++', project_name;
    RETURN ''::HSTORE;
  END IF;
  RETURN params;
END;
$$ LANGUAGE PLPGSQL;

---------------------------------------------------------------------
--/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/--
---------------------------------------------------------------------
			    


