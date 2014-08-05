<?php

class GenProjectRunStatusTable
{
    private $selected_run_num=null;
    private $selected_proj_name=null;
    
    public function __construct()
    {
        $this->selected_run_num='';
        $this->selected_proj_name='';
    }
    
    public function __destruct()
    {
        ;
    }
    
    public function setRunNum($selection)
    {
        $this->selected_run_num=(int)$selection;
    }
    
    public function setProjName($selection)
    {
        $this->selected_proj_name=$selection;
    }
  
    public function genTable()
    {

        $this->selected_run_num=(int)1;
        $this->selected_proj_name="dummy_daq";

        if($this->selected_run_num==null) return false;
        
        if($this->selected_proj_name==null) return false;
        


        $myTable = new GenHTML_Table();
        $myTable->setCellOption("align=center");
        
        //create column headers
        $myTable->addField("Run");
        $myTable->setCellHeader("Run");
        $myTable->addField("Subrun");
        $myTable->setCellHeader("Subrun");
        $myTable->addField("$this->selected_proj_name");
        $myTable->setCellHeader("$this->selected_proj_name");
        
//this code is for when table will contain multiple projects
//$querystring = "SELECT Project FROM ListProject() WHERE Enabled='t'";
//$myQueryResult = DBInterface::get()->query($querystring);

//while($row = pg_fetch_row($myQueryResult)) {
//$myTable->addField("$row[0]");
//$myTable->setCellHeader("$row[0]");
//}

        //fill columns
        $querystring = "SELECT Run, Subrun, Status from $this->selected_proj_name WHERE Run = $this->selected_run_num";
        $myQueryResult = DBInterface::get()->query($querystring);
        while($row = pg_fetch_row($myQueryResult)) {
            $myTable->setCellColor("FFFFFF");
            $myTable->appendElement("$row[0]","Run");
            $myTable->appendElement("$row[1]","Subrun");
            if($row[2]=="1") $myTable->setCellColor("00FF00");
            else $myTable->setCellColor("FF0000");
            $myTable->appendElement("$row[2]","$this->selected_proj_name");
        }
     
        return $myTable->getTable(True);
  
        //      return 'hello';
            
    }    
}


?>