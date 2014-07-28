<?php

function __autoload($class_name) 
{
  $source = '/Users/davidkaleko/Sites/src/' . $class_name . '.php';
  if (file_exists($source)) require $source; 
  else throw new Exception('class "' . $class_name . '" source file not found. Failed to autoload...');
}



$myQueryResult = DBInterface::get()->query("SELECT * FROM testphptable");
echo "Echoing query result:<BR>";
echo $myQueryResult;









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

?>