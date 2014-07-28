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
    date_default_timezone_set("Europe/Paris");
    $cont="";
    $cont = $cont . "<HTML>\n<head>\n<title> Double Chooz DB WebViewer </title>\n";
    $cont = $cont . "<meta http-equiv=\"content-type\"<content=\"text/html; charset=ISO-8859-1\">\n";
    $cont = $cont . "<link href=\"/Users/davidkaleko/Sites/src/DCStyleSheet.css\" rel=\"stylesheet\" media=\"screen\">\n";
    if($this->auto_reload>0)
      $cont = $cont . "<meta http-equiv=\"refresh\" content=\"" . (int)$this->auto_reload . "\" >\n";
    return $cont . $this->header . "</head>\n\n<body>\n\n";
  }

  private function genQCLinks()
  {
    $link="";
    $link = $link . "<table width=\"100%\">\n";
    $link = $link . "<tr>\n";
    $link = $link . "<td class=\"internal\"><a href=\"http://dcmonitor.in2p3.fr/QualityControl/DOGSifier\">DOGSifier Monitoring</a></td>\n";
    $link = $link . "<td class=\"internal\"><a href=\"http://dcmonitor.in2p3.fr/QualityControl/CT\">Common Trunk Monitoring</a></td>\n";
    $link = $link . "<td class=\"internal\"><a href=\"http://dcmonitor.in2p3.fr/QualityControl/DQC\">Software Monitoring</a></td>\n";
    $link = $link . "<td class=\"internal\"><a href=\"http://dcmonitor.in2p3.fr/QualityControl/DC\">DataChallenge Monitoring</a></td>\n";
    $link = $link . "</tr><tr>\n";
    $link = $link . "<td class=\"internal\"><a href=\"http://dcmonitor.in2p3.fr/QualityControl/RunInfo\">Run Information</a></td>\n";    
    $link = $link . "<td class=\"internal\"><a href=\"http://dcmonitor.in2p3.fr/QualityControl/FileDB\">Production ROOT Files</a></td>\n";    
    $link = $link . "<td class=\"internal\"><a href=\"http://dcmonitor.in2p3.fr/DataStream\">DATA Stream Monitoring</a></td>\n";
    $link = $link . "<td class=\"internal\"><a href=\"http://doublechooz.in2p3.fr/Private/private.php\">DC Web</a></td>\n";
    $link = $link . "</tr>\n</table>\n\n";
    return $link;
  }

  private function genBody($mode,$name)
  {
    $cont="";
    if($mode=="QC")
      $cont = $cont . $this->genQCLinks();
    $cont = $cont . "<h1> Double Chooz " . (string)$name . " Homepage </h1>\n";
    $cont = $cont . "This page was last updated at " . (string)date("D M j G:i:s T Y") . "\n";
    $cont = $cont . "<BR> PHP version " . (string)phpversion() . "\n";
    $cont = $cont . "<hr size=\"2\" width=\"100%\">\n\n";
    return $cont . $this->body;
  }

  private function genFooter()
  {
    $this->footer = "\n\n<br><br>\n\n <HR>\n<footer>";
    $this->footer = sprintf("%s\n<FONT COLOR=\"RED\">Hosted by </FONT><B>IN2P3</B><br>",$this->footer);
    $this->footer = sprintf("%s\n<FONT COLOR=\"BLUE\">Maintained by </FONT><B>MIT DoubleChooz Group</B><br>",$this->footer);
    $this->footer = sprintf("%s\n<address> Help? Questions? ... Wake up <a href=\"mailto:kazuhiro@mit.edu\"> Sleepy Grads</a></address>",$this->footer);
    $this->footer = sprintf("%s\n</footer>",$this->footer);
    return $this->footer . "\n</body></html>\n";
  }

  
}
?>
