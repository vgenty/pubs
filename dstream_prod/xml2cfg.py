import sys, os, subprocess

servers = ['uboonegpvm06.fnal.gov', 'uboonegpvm05.fnal.gov', 'uboonegpvm04.fnal.gov']

template = """
PROJECT_BEGIN
NAME     PROJECT_NAME
COMMAND  python dstream_prod/production.py PROJECT_NAME
CONTACT  yuntse@slac.stanford.edu
PERIOD   120
SERVER   SERVER_NAME
SLEEP    30
RUNTABLE MainRun
RUN      1
SUBRUN   1
ENABLE   True
RESOURCE NRUNS => 3
RESOURCE XMLFILE => XML_PATH
RESOURCE STAGE_NAME => gen:g4:detsim:reco1:reco2:mergeana
RESOURCE STAGE_STATUS => 0:10:20:30:40:50
RESOURCE NRESUBMISSION => 2
RESOURCE EXPERTS => yuntse@slac.stanford.edu:kazuhiro@nevis.columbia.edu:greenlee@fnal.gov
PROJECT_END

"""

def createCFG( sample, cfgname, counter ):
   cfg = open( cfgname, "a" )

   project_name = sample.strip().split('/')[-1]
   project_name = project_name.replace(".xml", "")
   project_name = project_name.replace(".", "_")
   project_name = project_name.replace("-", "_")

   iserver = counter % len(servers)

   content = template.replace( "PROJECT_NAME", project_name )
   content = content.replace( "XML_PATH", sample )
   content = content.replace( "SERVER_NAME", servers[iserver] )

   cfg.write( content )
   cfg.close()

# def createCFG( sample, cfgname )

if __name__ == '__main__':

   xmlpath = sys.argv[1]
   cfgname = sys.argv[2]

   hctr = 0
   lctr = 0

   try:
      os.remove( cfgname )
   except OSError:
      pass

   samples = os.listdir( xmlpath )

   for sample in samples:
      if sample.find("dirt") >= 0 or sample.find("supernova") >= 0:
         continue

      if sample.find("prodgenie") >= 0 and sample.find("uniform") < 0:
         hctr += 1
         ctr = hctr
      else:
         lctr += 1
         ctr = lctr

      sample = os.path.join( xmlpath, sample )
      createCFG( sample, cfgname, ctr )

