<?php

class DBInterface
{
    
    private $myURL;
    private $myUSER;
    private $myPSWD;
    private static $myDBI=null;
    private $psql_link=null;
    const   CONN_CONFIG="/Users/davidkaleko/Sites/config/sql_config_reader";
    
    private function __construct($config=null)
    {
        if ($config!=null)
            $this->readConnInfo($config);
        else
            $this->readConnInfo();
        $this->connect();
    }
    
    public function __destruct()
    {
        $this->close();
        foreach(self::$myDBI as $key=>$value)
            if($value==$this)
                unset(self::$myDBI[$key]);
        //self::$myDBI=null;
    }
    
    public static function get($config=null)
    {
        
        if ( self::$myDBI == null )
            self::$myDBI = array();
        
        if ($config==null || $config=="")
            $config=self::CONN_CONFIG;

        else
            $config=(string)$config;
        
        if(!array_key_exists($config,self::$myDBI))
            self::$myDBI[$config] = new DBInterface($config);

        return self::$myDBI[$config];
    }
    
    private function connect() {
        if($this->psql_link == null){
            $conn_info = "host=" . $this->myURL . " user=" . $this->myUSER . " password=" . $this->myPSWD . " dbname=davidkaleko";
            //          echo $conn_info . "<BR>\n";
            $this->psql_link = pg_connect($conn_info) or die(psql_error());
          }
        else if(!($this->checkConn())){
            $this->psql_link = pg_connect("host=" . $this->myURL . " user=" . $this->myUSER . " password=" . $this->myPSWD) or die(psql_error());
        }
    }
    
    public function checkConn()
    {
        if($this->psql_link == null)
            return false;
        else
            return pg_ping($this->psql_link);
    }
    
    private function close() {
        if($this->psql_link != null)
            if($this->checkConn())
                pg_close($this->psql_link) or die(psql_error());
    }
    
    private function readConnInfo($configFile=self::CONN_CONFIG)
    {
        echo "<BR>\n" . $configFile . "\n<BR>\n";
        $fh = fopen((string) $configFile,'r');
        $removeChars  = array("\n"," "); 
        $this->myURL  = str_replace($removeChars,"",(string) fgets($fh));
        $this->myUSER = str_replace($removeChars,"",(string) fgets($fh));
        $this->myPSWD = str_replace($removeChars,"",(string) fgets($fh));
        echo "\n<BR>" . $this->myURL  . " " . $this->myUSER . " " . $this->myPSWD . "<BR>\n";
        fclose($fh);
    }
    
    public function query($cmd) {
        echo "QUERY IS $cmd<BR>\n";
        $this->connect();
        $res=pg_query($this->psql_link,$cmd) or die(psql_error());
        while($row = pg_fetch_row($res)) {
            echo $row[0] . " " . $row[1] . " " . $row[2] . " " . $row[3] . "<BR>\n";
        }
        return $res;
    }
    
}
?>
