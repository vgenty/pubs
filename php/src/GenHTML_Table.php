<?php

class GenHTML_Table
{
  private $table=null;
  private $field=array();
  private $showField=false;
  private $cell_color=null;
  private $cell_class=null;
  private $cell_header=null;
  private $cell_opt=null;
  public function __construct()
  {
    $this->table="";
  }

  public function __destruct()
  {
    ;
  }

  public function setCellOption($opt)
  {
    if($opt==null)
      $this->cell_opt=null;
    else
      $this->cell_opt=" " . (string)$opt . " ";
  }

  public function setCellColor($col_def)
  {
    if ($col_def!=null)
      $this->setCellOption("bgcolor=" . (string)$col_def);
  }

  public function setCellClass($class)
  {
    if ($class==null)
      $this->cell_class=null;
    else
      $this->cell_class=(string)$class;
  }

  public function setCellHeader($header=null,$opt=null)
  {
    if((string)$header=="")
      {
	$this->cell_header=null;
	return;
      }
    if($opt!=null)
      $this->cell_header="<th" . (string)$opt .">";
    else
      $this->cell_header="<th>";
    $this->cell_header = $this->cell_header . (string)$header . "</th>\n";
  }
  
  public function showField($show=true)
  {
    $this->showField=(bool)$show;
  }

  public function addField($name)
  {
    if(!(array_key_exists($name,$this->field)))
      $this->field[$name]=array();
  }

  public function appendElement($cont,$name)
  {
    if(!(array_key_exists($name,$this->field)))
      return false;
    end($this->field[$name]);
    $row = (int)key($this->field[$name]);
    //echo $cont . "<br>\n";
    $this->field[$name][$row+1]=$this->getTd() . $cont . "</td>";
    return true;
  }

  public function modifyElement($cont,$name,$row)
  {
    if(!(array_key_exists($name,$this->field)))
      return false;
    end($this->field[$name]);
    $lastrow = (int)key($this->field);
    if($row>$lastrow)
      return false;
    $this->field[$name][$row]=$cont;
    return true;
  }
  
  public function getTable($line=false)
  {
    if((bool)$line)
      return "<table border='1'>\n" . $this->getTableContent() . "\n</table>\n";
    else
      return "<table>\n" . $this->getTableContent() . "\n</table>\n";
  }

  private function getTd()
  {
    $td=null;
    if ($this->cell_opt!=null)
      $td="<td " . $this->cell_opt . ">";
    else
      $td="<td>";
    if ($this->cell_class!=null)
      $td = $td . "<div class=\"" . $this->cell_class . "\">";

    if ($this->cell_header!=null)
      {
	$td = $this->cell_header . $td;
	$this->setCellHeader();
      }
    return $td;
  }

  private function getTableContent()
  {
    $td = $this->getTd();
    $table_cont="\n";
    $maxRow=0;

    if($this->showField)
      $table_cont = $table_cont . "<tr>\n";
    foreach($this->field as $key => $array)
      {
	if($this->showField)
	  $table_cont = $table_cont . $td . (string)$key . " </td>"; 
	end($array);
	$lastrow=(int)key($array);
	if($lastrow>$maxRow)
	  $maxRow=$lastrow;
      }
    if($this->showField)
      $table_cont = $table_cont . "</tr>\n";

    for($ctr=1; $ctr<=$maxRow; $ctr+=1)
      {
	$table_cont = $table_cont . "<tr>\n";
	foreach($this->field as $field_name => $field_rows)
	  {
	    if(array_key_exists($ctr,$field_rows))
	      $table_cont = $table_cont . $field_rows[$ctr];
	    else
	      $table_cont = $table_cont . $td . "</td> ";
	  }
	$table_cont = $table_cont . "\n</tr>\n";
      }
    return $table_cont;
  }

}
?>
