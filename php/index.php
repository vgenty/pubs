<?php

function __autoload($class_name) 
{
  $source = '/Users/davidkaleko/Sites/src/' . $class_name . '.php';
  if (file_exists($source)) require $source; 
  else throw new Exception('class "' . $class_name . '" source file not found. Failed to autoload...');
}



#test of genhtml_selectbox class
/*
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
*/

$myweb=new GenHTML();

echo $myweb->genWeb("QC","Quality Control");

?>