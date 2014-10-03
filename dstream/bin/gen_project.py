#!/usr/bin/python
import sys, os

if len(sys.argv)<2:
   sys.stderr.write('Usage: %s PROJECT_NAME\n\n' % sys.argv[0])
   sys.exit(1)
if not 'PUB_TOP_DIR' in os.environ.keys():
   sys.stderr.write('$PUB_TOP_DIR not defined!\n\n')
   sys.exit(1)

name=sys.argv[1]
target_dir='%s/dstream' % os.environ['PUB_TOP_DIR']
source_dir='%s/bin/tmp' % target_dir

if not os.path.exists(target_dir) or not os.path.exists(source_dir):
   sys.stderr.write('Did not find dstream directory: %s\n' % target_dir)
   sys.stderr.write('Check if your $PUB_TOP_DIR is set right...\n')
   sys.exit(1)

infile  = '%s/project.tmp' % (source_dir)
outfile = '%s/%s.py' % (target_dir,name)

if os.path.isfile(outfile):
   sys.stderr.write('Project %s seems to exist already... aborting!\n' % name )
   sys.exit(1)


contents = open(infile,'r').read()
contents=contents.replace('$PROJECT_NAME',name.upper())
contents=contents.replace('$project_name',name.lower())
contents=contents.replace('$Project_Name',name)
contents=contents.replace('$SHELL_USER_NAME',os.environ['USER'])

fout = open(outfile,'w')
fout.write(contents)
fout.close()

print
print 'Generated your project:%s/%s.py' % (target_dir,name)
print
print 'To remove this project, simply remove the file.'
print 'To register this project, use dstream/bin/register_project.py' 
print
sys.exit(0)
