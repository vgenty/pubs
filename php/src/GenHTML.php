<?php

class GenHTML
{

  private $header=null;
  private $footer=null; 
  private $body=null;
  private $auto_reload;

  public function __construct()
  {
    $this->header='';
    $this->footer='';
    $this->body='';
    $this->auto_reload=0;
  }

  public function __destruct()
  {
    ;
  }

  public function setAutoReload($period=10)
  {
    $this->auto_reload=(int)$period;
  }

  public function genWeb($mode="QC",$title="")
  {
      return $this->genHeader() . $this->genBody($mode,$title) . $this->genFooter();
  }

  public function addBody($cont)
  {
    $this->body = $this->body . (string)$cont;
  }

  public function addHeader($cont)
  {
    $this->header = $this->header . (string)$cont;
  }

  private function genHeader()
  {
    date_default_timezone_set("America/Chicago");
    $cont="";
    $cont = $cont . "<HTML>\n<head>\n<title> MicroBooNE DB WebViewer </title>\n";
    $cont = $cont . "<meta http-equiv=\"content-type\"<content=\"text/html; charset=ISO-8859-1\">\n";
    $cont = $cont . "<link href=\"/Users/davidkaleko/Sites/src/UBStyleSheet.css\" rel=\"stylesheet\" media=\"screen\">\n";
    if($this->auto_reload>0)
      $cont = $cont . "<meta http-equiv=\"refresh\" content=\"" . (int)$this->auto_reload . "\" >\n";
    return $cont . $this->header . "</head>\n\n<body>\n\n";
  }

  private function genQCLinks()
  {
    $link="";
    $link = $link . "<table width=\"100%\">\n";
    $link = $link . "<tr>\n";
    $link = $link . "<td class=\"internal\"><a href=\"http://www.google.com\">Google</a></td>\n";
    $link = $link . "<td class=\"internal\"><a href=\"/Users/davidkaleko/Sites/pages/testpage.php\">DB Query Example</a></td>\n";
    $link = $link . "</tr><tr>\n";
    $link = $link . "</tr>\n</table>\n\n";
    return $link;
  }

  private function genBody($mode,$name)
  {
    $cont="";
    if($mode=="QC")
      $cont = $cont . $this->genQCLinks();
    $cont = $cont . "<h1> MicroBooNE " . (string)$name . " Homepage </h1>\n";
    $cont = $cont . "This page was last updated at " . (string)date("D M j G:i:s T Y") . "\n";
    $cont = $cont . "<BR> PHP version " . (string)phpversion() . "\n";
    $cont = $cont . "<hr size=\"2\" width=\"100%\">\n\n";

    $cont = $cont . "Project list table from procdb Database:<BR>\n";
    $cont = $cont . GenProjectListTable::genTable();

    return $cont . $this->body;
  }

  private function genFooter()
  {
    $this->footer = "\n\n<br><br>\n\n <HR>\n<footer>";
    $this->footer = sprintf("%s\n<FONT COLOR=\"RED\">Hosted by </FONT><B>someone</B><br>",$this->footer);
    $this->footer = sprintf("%s\n<FONT COLOR=\"BLUE\">Maintained by </FONT><B>probably me</B><br>",$this->footer);
    $this->footer = sprintf("%s\n<address> Help? Questions? ... Wake up <a href=\"mailto:kazuhiro@mit.edu\"> Sleepy Grads</a></address>",$this->footer);
    $this->footer = sprintf("%s\n</footer>",$this->footer);
    return $this->footer . "\n</body></html>\n";
  }

  
}
?>
