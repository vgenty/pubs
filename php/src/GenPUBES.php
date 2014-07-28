<?php

class GenPUBES
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

  public function hello_world()
  {
    echo "Hello World!";
  }
}

?>