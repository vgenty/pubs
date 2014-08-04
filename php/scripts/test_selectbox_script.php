<?php

function __autoload($class_name) 
{
    $source = '/Users/davidkaleko/Sites/src/' . $class_name . '.php';
    if (file_exists($source)) require $source; 
    else throw new Exception('class "' . $class_name . '" source file not found. Failed to autoload...');
}


if ($_POST['cb_query_toggle']=='kTrue'){
    if ($_POST['box_tablename'] != null){
        $querystring = "SELECT * FROM " . $_POST['box_tablename'];
        $myQueryResult = DBInterface::get()->query($querystring);
    }
    else {
        echo "Dude you need to input a table name!<BR>\n";
        return;
    }
}
else {
    echo "Why don't you want to query the table?!<BR>\n";
    return;
}


$myTable = new GenHTML_Table();
$myTable->setCellOption("align=center");
$myTable->setCellColor("FF1493");

$ncolumns = count(pg_fetch_row($myQueryResult));
for ($i=0; $i<$ncolumns; $i++){
    $myTable->addField("column".$i);
    $myTable->setCellHeader("column".$i);
}

while($row = pg_fetch_row($myQueryResult)) {
    foreach ($row as $key=>$value)
        $myTable->appendElement("$value","column".$key);
}

echo $myTable->getTable(True);
?>