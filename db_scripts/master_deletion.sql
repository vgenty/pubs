--SET ROLE uboonedaq_admin;
--SET ROLE uboone_admin;
DROP FUNCTION IF EXISTS DoesProcessExist(TEXT);
DROP FUNCTION IF EXISTS DoesProjectExist(TEXT);
DROP FUNCTION IF EXISTS DoesTableExist(TEXT);
DROP FUNCTION IF EXISTS RemoveProject(project_name TEXT);
DROP FUNCTION IF EXISTS RemoveProcessDB();
DROP FUNCTION IF EXISTS RemoveTestRunTable(TEXT);
DROP FUNCTION IF EXISTS CreateTestRunTable();
DROP FUNCTION IF EXISTS CreateTestRunTable(TEXT);
DROP FUNCTION IF EXISTS CreateDaemonTable();
DROP FUNCTION IF EXISTS ListDaemonLog(TEXT,TIMESTAMP,TIMESTAMP);
DROP FUNCTION IF EXISTS ListDaemonLog(TIMESTAMP,TIMESTAMP);
DROP FUNCTION IF EXISTS ProjectRunning(TEXT);
DROP FUNCTION IF EXISTS ProjectStopped(TEXT);
DROP FUNCTIOn IF EXISTS UpdateDaemonTable( nodename       TEXT,
                        		   max_proj_ctr   INT,
                                           max_uptime     INT,
                                           log_duration   INT,
                                           sync_period    INT,
                                           update_period  INT,
                                           cleanup_period INT,
                                           mail_address   TEXT,
                                           enable         BOOLEAN );
DROP FUNCTION IF EXISTS CreateDaemonLogTable();
DROP FUNCTION IF EXISTS UpdateDaemonLog( TEXT,
     	      	 			 INT, INT, INT, INT,
					 HSTORE );
DROP FUNCTION DoesDaemonExist(tname TEXT);
DROP FUNCTION IF EXISTS InsertIntoTestRunTable( Run INT, SubRun INT, 
     	      	 				TimeStart TIMESTAMP,
						TimeEnd   TIMESTAMP);
DROP FUNCTION IF EXISTS InsertIntoTestRunTable( RunTableName TEXT,
     	      	 				Run INT, SubRun INT, 
     	      	 				TimeStart TIMESTAMP,
						TimeEnd   TIMESTAMP);
DROP FUNCTION IF EXISTS FillTestRunTable( RunTableName TEXT,
     	      	 			  NumRuns    INT,
     	      	 			  NumSubRuns INT);
DROP FUNCTION IF EXISTS FillTestRunTable( NumRuns    INT,
     	      	 			  NumSubRuns INT);

DROP FUNCTION IF EXISTS GetRunTimeStamp(Run INT, SubRun INT);
DROP FUNCTION IF EXISTS GetRunTimeStamp(RunTableName TEXT, Run INT, SubRun INT);
DROP FUNCTION IF EXISTS GetVersionRunRange(project_name TEXT);
DROP FUNCTION IF EXISTS CreateProcessTable();
DROP FUNCTION IF EXISTS CheckDBIntegrity();
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

DROP FUNCTION IF EXISTS DefineProject( project_name TEXT,
     	      	 		       command      TEXT,
				       frequency    INT,
				       email        TEXT,
				       start_run    INT,
				       start_subrun INT,
				       resource     HSTORE,
				       enabled      BOOLEAN);

DROP FUNCTION IF EXISTS UpdateProjectConfig( project_name TEXT,
     	      	 		       	     command      TEXT,
				       	     frequency    INT,
				       	     email        TEXT,
				       	     resource     HSTORE,
				       	     enabled      BOOLEAN,
					     version      INT);
DROP FUNCTION IF EXISTS UpdateProjectConfig( project_name TEXT,
     	      	 		       	     command      TEXT,
				       	     frequency    INT,
					     sleepAfter   INT,
				       	     email        TEXT,
					     nodename     TEXT,
				       	     resource     HSTORE,
				       	     enabled      BOOLEAN);

DROP FUNCTION IF EXISTS UpdateProjectConfig( project_name TEXT,
     	      	 		       	     command      TEXT,
				       	     frequency    INT,
				       	     email        TEXT,
				       	     resource     HSTORE,
				       	     enabled      BOOLEAN);
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

DROP FUNCTION IF EXISTS ListEnabledProject();
DROP FUNCTION IF EXISTS ListProject();
DROP FUNCTION IF EXISTS ListEnabledDaemon();
DROP FUNCTION IF EXISTS ListDaemon();
DROP FUNCTION IF EXISTS AllProjectRunSynch();
DROP FUNCTION IF EXISTS OneProjectRunSynch( project      TEXT,
					    project_ver  SMALLINT,
					    start_run    INT,
					    start_subrun INT );
DROP FUNCTION IF EXISTS ProjectInfo(project_name TEXT);
DROP FUNCTION IF EXISTS ProjectInfo(project_name TEXT, project_ver INT);
DROP FUNCTION IF EXISTS ProjectResource(project_name TEXT);
DROP FUNCTION IF EXISTS ProjectInfo( project_name TEXT, 
     	      	 		     project_info TEXT,
     	      	 		     version SMALLINT);


			    


