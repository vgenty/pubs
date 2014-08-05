<?php

class GenHTML_SelectBox
{
    private $boxName;
    private $runScript;
    private $method;
    
    private $box=array();
    private $box_title=array();
    private $box_tdopt=array();
    private $box_inputopt=array();
    
    private $checkbox=array();
    private $checkbox_title=array();
    private $checkbox_tdopt=array();
    private $checkbox_inputopt=array();
    
    private $scroll=array();
    private $scroll_title=array();
    private $scroll_tdopt=array();
    private $scroll_inputopt=array();
    
    private $text=array();
    private $text_tite=array();
    private $text_tdopt=array();
    private $text_inputopt=array();
    
    private $currentColumn;
    private $columnName=array();
    private $check_box_scroll=array();
    
    public function __construct($name=null,$columnName=null)
    {
        if($name!=null && (string)$name!="")
            $this->boxName=(string)$name;
        else
            $this->boxName="default";
        $this->runScript=null;
        $this->method="POST";
        $this->currentColumn=0;
        $this->check_box_scroll[$this->currentColumn]=array();
        if($columnName!=null)
            $columnName=(string)$columnName;
        else
            $columnName=(string)$currentColumn;
        $this->columnName[$this->currentColumn]=$columnName;
    }
    
    public function appendColumn($columnName=null)
    {
        $this->currentColumn=$this->currentColumn+1;
        $this->check_box_scroll[$this->currentColumn]=array();
        if($columnName!=null)
            $columnName=(string)$columnName;
        else
            $columnName=(string)$this->currentColumn;
        $this->columnName[$this->currentColumn]=$columnName;
    }
    
    public function goToColumn($column=0)
    {
        if(!(array_key_exists((int)$column,$this->check_box_scroll)))
            return false;
        $this->currentColumn=$column;
        return true;
    }
    
    public function setInputOption($name,$opt)
    {
        if(array_key_exists((string)$name,$this->box))
            $this->box_inputopt[(string)$name]=(string)$opt;
        else if(array_key_exists((string)$name,$this->scroll))
            $this->scroll_inputopt[(string)$name]=(string)$opt;
        else if(array_key_exists((string)$name,$this->checkbox))
            $this->checkbox_inputopt[(string)$name]=(string)$opt;
        else if(array_key_exists((string)$name,$this->text))
            $this->text_inputopt[(string)$name]=(string)$opt;
    }
    
    public function setInputTDOption($name,$opt)
    {
        if(array_key_exists((string)$name,$this->box))
            $this->box_tdopt[(string)$name]=(string)$opt;
        else if(array_key_exists((string)$name,$this->scroll))
            $this->scroll_tdopt[(string)$name]=(string)$opt;
        else if(array_key_exists((string)$name,$this->checkbox))
            $this->checkbox_tdopt[(string)$name]=(string)$opt;
        else if(array_key_exists((string)$name,$this->text))
            $this->text_tdopt[(string)$name]=(string)$opt;
    }
    
    public function setMethod($method)
    {
        $this->method=(string)$method;
    }
    
    public function setRunScript($name)
    {
        if((string)$name!="")
            $this->runScript=(string)$name;
    }
    
    public function setBoxName($name)
    {
        if($name!=null && (string)$name!="")
            $this->boxName=(string)$name;
    }

    public function addBox($title,$name,$default_value=null)
    {
        if(array_key_exists((string)$name,$this->box) || 
        array_key_exists((string)$name,$this->scroll) || 
        array_key_exists((string)$name,$this->checkbox) ||
        array_key_exists((string)$name,$this->text))
            return;
        $this->append2Array($this->check_box_scroll[$this->currentColumn],(string)$name);
        $this->box[(string)$name]=$default_value;
        $this->box_title[(string)$name]=(string)$title;
    }

    public function addScroll($title,$name)
    {
        if(array_key_exists((string)$name,$this->box) || 
        array_key_exists((string)$name,$this->scroll) ||
        array_key_exists((string)$name,$this->checkbox) ||
        array_key_exists((string)$name,$this->text))
            return;
        $this->append2Array($this->check_box_scroll[$this->currentColumn],(string)$name);
        $this->scroll[(string)$name] = array();
        $this->scroll_title[(string)$name] = (string)$title;
    }

    public function addCheckBox($title,$name,$default_value=null)
    { 
        if(array_key_exists((string)$name,$this->box) || 
        array_key_exists((string)$name,$this->scroll) ||
        array_key_exists((string)$name,$this->checkbox) ||
        array_key_exists((string)$name,$this->text))
            return;
        $this->append2Array($this->check_box_scroll[$this->currentColumn],(string)$name);
        $this->checkbox[(string)$name] = $default_value;
        $this->checkbox_title[(string)$name] = (string)$title;   
    }

    public function addText($title,$name,$default_value=null)
    {
        if(array_key_exists((string)$name,$this->box) ||
        array_key_exists((string)$name,$this->scroll) ||
        array_key_exists((string)$name,$this->checkbox) ||
        array_key_exists((string)$name,$this->text))
            return;
        $this->append2Array($this->check_box_scroll[$this->currentColumn],(string)$name);
        $this->text[(string)$name] = (string)$default_value;
        $this->text_title[(string)$name] = (string)$title;
    }

    public function appendScrollOption($name,$opt,$value)
    {
        if(!array_key_exists((string)$name,$this->scroll))
            return;
        if(array_key_exists((string)$opt,$this->scroll[(string)$name]))
            return;
        $this->scroll[(string)$name][(string)$opt]=(string)$value;
    }
  
    public function setBoxDefault($name,$opt)
    {
        if(!array_key_exists((string)$name,$this->box))
            return;
        $this->box[(string)$name]=$opt;
    }

    public function genSelectBox()
    {
        if ($this->runScript==null)
            return "ERROR ... NO RUN SCRIPT SET FOR FORM!";
        $myBox="<form method=\"" . $this->method . "\" action=\"" . $this->runScript . "\" name=\"" . (string)$this->boxName . "\">\n\n";

        $myTable = new GenHTML_Table();
        $maxRowCount=0;
        $displayField=true;
        foreach($this->check_box_scroll as $column => $rowcont)
            {
                if (count($rowcont)>$maxRowCount)
                    $maxRowCount=count($rowcont);
                if(is_numeric($this->columnName[$column]))
                    $displayField=false;
            }

        foreach($this->check_box_scroll as $column => $rowcont)
            {
                $myTable->addField($this->columnName[$column]);
                if($displayField)
                    {
                        $myTable->setCellHeader(sprintf("<FONT SIZE=5 COLOR=\"#FF0000\"> %s </FONT>",$this->columnName[$column])," align=\"left\" ");
                        $myTable->appendElement("&nbsp;",$this->columnName[$column]);
                    }
                $numEmpty=$maxRowCount-count($rowcont);
                foreach($rowcont as $row => $item)
                    {
                        if(array_key_exists($item,$this->box))
                            {
                                $myTable->setCellHeader($this->box_title[(string)$item]," align=\"left\" ");
                                if(array_key_exists((string)$item,$this->box_tdopt))
                                    $myTable->setCellOption($this->box_tdopt[(string)$item]);
                                else
                                    $myTable->setCellOption(null);
                                $myTable->appendElement($this->genBox($item),$this->columnName[$column]);
                            }
                        else if(array_key_exists($item,$this->scroll))
                            {
                                $myTable->setCellHeader($this->scroll_title[(string)$item]," align=\"left\" ");
                                if(array_key_exists((string)$item,$this->scroll_tdopt))
                                    $myTable->setCellOption($this->scroll_tdopt[(string)$item]);
                                else
                                    $myTable->setCellOption(null);
                                $myTable->appendElement($this->genScroll($item),$this->columnName[$column]);
                            }
                        else if(array_key_exists($item,$this->checkbox))
                            {
                                $myTable->setCellHeader($this->checkbox_title[(string)$item]," align=\"left\" ");
                                if(array_key_exists((string)$item,$this->checkbox_tdopt))
                                    $myTable->setCellOption($this->checkbox_tdopt[(string)$item]);
                                else
                                    $myTable->setCellOption(null);
                                $myTable->appendElement($this->genCheckBox($item),$this->columnName[$column]);
                            }
                        else if(array_key_exists($item,$this->text))
                            {
                                $myTable->setCellHeader($this->text_title[(string)$item]," align=\"left\" ");
                                if(array_key_exists((string)$item,$this->text_tdopt))
                                    $myTable->setCellOption($this->text_tdopt[(string)$item]);
                                else
                                    $myTable->setCellOption(null);
                                //echo $this->genText($item) . "<br>\n";
                                $myTable->appendElement($this->genText($item),$this->columnName[$column]);
                            }
                    }
                if($numEmpty>0)
                    for($i=0; $i<$numEmpty; $i++)
                        {
                            $myTable->setCellHeader(" "," align=\"left\" "); 
                            $myTable->setCellOption(null);
                            $myTable->appendElement(' ',$this->columnName[$column]);
                        }

                // temporal stupid alignment...
                $empty_column_title="&nbsp;";
                for($i=0;$i<$column;$i++)
                    $empty_column_title=sprintf("%s%s",$empty_column_title,"&nbsp;");
                $myTable->addField((string)($column+0.5));
                //$myTable->addField($empty_column_title);
                $numRows=$maxRowCount;
                if($displayField)
                    $numRows++;
                for($i=0; $i<$numRows; $i++)
                    {
                        $myTable->setCellHeader(" "," align=\"left\" "); 
                        $myTable->setCellOption(null);
                        $myTable->appendElement('&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;',(string)($column+0.5));
                        //$myTable->appendElement('&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;',$empty_column_title);
                    }
            }
        //$myTable->showField($displayField);
        $myBox = $myBox . $myTable->getTable() . "\n";
        $myBox = $myBox . "<p>\n";
        $myBox = $myBox . "<input type=\"submit\" name=\"submit\" value=\"Submit\">\n";
        $myBox = $myBox . "<input type=\"reset\">\n";
        $myBox = $myBox . "</p>\n\n";
        $myBox = $myBox . "<p></p></form>\n\n";
        return $myBox;
    }

    private function genBox($name)
    {
        if(!array_key_exists((string)$name, $this->box))
            return;
        $box="<input type=\"text\" name=\"" . (string)$name . "\" ";
        if ($this->box[(string)($name)]!=null)
            $box = $box . " value=" . $this->box[(string)$name]; 
        if (array_key_exists((string)$name,$this->box_inputopt))
            $box = $box . " " . $this->box_inputopt[(string)$name];
        $box = $box . "></input>\n";
        return $box;
    }

    private function genText($name)
    {
        if(!array_key_exists((string)$name, $this->text))
            return;
        return $this->text[$name];
    }

    private function genCheckBox($name)
    {
        if(!array_key_exists((string)$name, $this->checkbox))
            return;

        $checkbox="<input type=\"checkbox\" name=\"" . (string)$name . "\" ";
        if ($this->checkbox[(string)($name)]!=null)
            {
                $checkbox = $checkbox . " value=" . $this->checkbox[(string)$name]; 
                $checkbox = $checkbox . " checked";
            }
        if (array_key_exists((string)$name,$this->checkbox_inputopt))
            $checkbox = $checkbox . " " . $this->checkbox_inputopt[(string)$name];
        $checkbox = $checkbox . "></input>\n";
        return $checkbox;
    }

    private function genScroll($name)
    {

        if(!array_key_exists((string)$name, $this->scroll))
            return "";
        $scroll="<select name=\"" . $name . "\""; 
        if (array_key_exists((string)$name,$this->scroll_inputopt))
            $scroll = $scroll . " " . $this->scroll_inputopt[(string)$name];
        $scroll = $scroll . ">\n";
        foreach($this->scroll[(string)$name] as $opt => $value)
            {
                $scroll = $scroll . "<option value=" . $value . "> " . $opt . " </option>\n"; 
            }
        $scroll= $scroll . "</select>\n";
        return $scroll;
    }

    private function append2Array(&$array,$cont)
    {
        end($array);
        $array[(int)(key($array))+1]=$cont;
    }

}
?>
