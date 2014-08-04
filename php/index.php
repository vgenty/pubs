<?php

function __autoload($class_name) 
{
  $source = '/Users/davidkaleko/Sites/src/' . $class_name . '.php';
  if (file_exists($source)) require $source; 
  else throw new Exception('class "' . $class_name . '" source file not found. Failed to autoload...');
}


#test of genhtml_selectbox class
$mySelBox = new GenHTML_SelectBox();
//run script is what's executed upon "select" button
$mySelBox->setRunScript("scripts/test_selectbox_script.php");
//$mySelBox->appendColumn("test column");
//$mySelBox->appendColumn("test column2");
$mySelBox->addBox("Enter table name to query","box_tablename");
$mySelBox->addCheckBox("Do you want to query the DB?","cb_query_toggle","\"kTrue\"");
//$mySelBox->setInputOption("cbname","value=aho");
//$mySelBox->addText("cbtitle2","cbname2","this is the text");
echo $mySelBox->genSelectBox();



#test of DBInterface on davidkaleko's local testphptable
/*
$myQueryResult = DBInterface::get()->query("SELECT * FROM testphptable");
echo "Echoing query result:<BR>";
echo $myQueryResult;
echo "<BR>\n";
*/


#test of genhtml_table class
/*
$myTable = new GenHTML_Table();
//$myTable->showField();
$myTable->addField("test field");
$myTable->setCellHeader("header");
$myTable->addField("second field");
$myTable->setCellHeader("head2");

$myTable->setCellOption("align=center");
$myTable->setCellColor("FF1493");

$myTable->appendElement("el1","test field");
$myTable->appendElement("el2","test field");
$myTable->appendElement("el3","test field");
$myTable->appendElement("blah","second field");
$myTable->appendElement("awef","second field");
$myTable->appendElement("poop","second field");
$myTable->modifyElement("fixedpoop","second field",2);

echo $myTable->getTable(True);
*/


#test of GenHTML class
/*
$myweb=new GenHTML();
$myweb->addBody("<h3> _ENV variable </h3>");

$myweb->addBody("<BR><BR>\n");
$myweb->addBody("<h3> ENV variable </h3>");

$myweb->addBody("<BR><BR>\n");
$myweb->addBody("<h3> _GET variable </h3>");

$myweb->addBody("<BR><BR>\n");
//$myweb->addBody((string)FileInfoTable::get("CT")->isOnHPSS(8171));

//foreach($_ENV as $key => $value);
//{
//  $myweb->addBody("<BR>\n" . var_dump($key) . " => " . var_dump($value) . " \n");
//}

$myweb->addBody("<BR>\n PARSED_STRING ... " . $ENV['QUERY_STRING'] . "\n");

echo $myweb->genWeb();
*/

?>