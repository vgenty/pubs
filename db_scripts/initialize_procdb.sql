--SET ROLE uboonedaq_admin;
--SET ROLE uboone_admin;
SELECT RemoveProcessDB();
DROP TABLE IF EXISTS TestRun;
SELECT CreateTestRunTable('TestRun');
SELECT CreateProcessTable();
SELECT CreateDaemonTable();
SELECT CreateDaemonLogTable();





