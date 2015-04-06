<?php

class GenProjectListTable
{
    
    public function __construct()
    {
        ;
    }
    
    public function __destruct()
    {
        ;
    }
    
    public function genTable()
    {
        $querystring = "SELECT Project, Enabled FROM ListProject()";
        $myQueryResult = DBInterface::get()->query($querystring);
        $myTable = new GenHTML_Table();
        $myTable->setCellOption("align=center");
        $myTable->addField("Project");
        $myTable->setCellHeader("Project Name");
        
        while($row = pg_fetch_row($myQueryResult)) {
            if($row[1]=="t") $myTable->setCellColor("00FF00");
            else $myTable->setCellColor("FF0000");
            $myTable->appendElement("$row[0]","Project");
        }
  
        return $myTable->getTable(True);
        
    }
    
}


?>