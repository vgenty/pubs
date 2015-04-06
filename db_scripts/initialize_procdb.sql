SELECT RemoveProcessDB();
DROP TABLE IF EXISTS TestRun;
SELECT CreateTestRunTable('TestRun');
SELECT CreateProcessTable();
SELECT CreateDaemonTable();
SELECT CreateDaemonLogTable();





