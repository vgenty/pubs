<?php

function __autoload($class_name) 
{
    $source = '/Users/davidkaleko/Sites/src/' . $class_name . '.php';
    if (file_exists($source)) require $source; 
    else throw new Exception('class "' . $class_name . '" source file not found. Failed to autoload...');
}

//debug
/*
foreach ($_POST as $key => $value)
    echo "$key, $value<BR>\n";
*/

if(isset($_POST['myProjScrollName'])) $projectName = $_POST['myProjScrollName'];    
else return false;

if(isset($_POST['myRunScrollName'])) $runNumber = (int)$_POST['myRunScrollName'];
else return false;

$cont = "";
$cont = $cont . "<B>Run/Status table for Project $projectName, Run $runNumber:</B><BR>\n";

$myProjectRunStatusTable = new GenProjectRunStatusTable();
$myProjectRunStatusTable->setRunNum($runNumber);
$myProjectRunStatusTable->setProjName("$projectName");
$cont = $cont . $myProjectRunStatusTable->genTable();

echo $cont;


return;
?>