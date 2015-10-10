from dstream import ds_multiprocess,ds_api,ds_status
import sys,time,os,json,samweb_cli

flist_v = [ ( int(x.split()[0]), int(x.split()[1]), x.split()[2] ) for x in open(sys.argv[1],'r').read().split('\n') if len(x.split()) == 3 ]

reader = ds_api.ds_reader()

mp = ds_multiprocess()
samweb = samweb_cli.SAMWebClient(experiment="uboone")

good_sam_v=[]
bad_sam_v=[]
for run,subrun,fname in flist_v:
    
    print 'processing',run,subrun,fname

    json_name = fname + '.json'

    if not os.path.isfile(fname):
        print 'Error: data file not found...'
        bad_sam_v.append((run,subrun,fname))
        continue
    if not os.path.isfile(json_name):
        print 'Error: json file not found...'
        bad_sam_v.append((run,subrun,fname))
        continue

    json_dict = None
    try:
        json_dict = json.load( open( json_name ) )
    except ValueError:
        self.error('Failed loading json file: %s' % json_name)
        bad_sam_v.append((run,subrun,fname))
        continue

    try:
        in_file_base=os.path.basename(fname)
        samweb.getMetadata(filenameorid=in_file_base)
        print 'File %s Existing at SAM' % in_file_base
        continue
    except samweb_cli.exceptions.FileNotFound:
        try:
            samweb.declareFile(md=json_dict)
            #bad_sam_v.append((run,subrun,fname))
        except Exception as e:
            print "Unexpected error: samweb declareFile problem: "
            print e
            bad_sam_v.append((run,subrun,fname))
            continue

    good_sam_v.append((run,subrun,fname))

fout = open('%s_good_sam.txt' % sys.argv[1],'w')
for f in good_sam_v:
    fout.write('%07d %05d %s\n' % f)
fout.close()

fout = open('%s_bad_sam.txt' % sys.argv[1],'w')
for f in bad_sam_v:
    fout.write('%07d %05d %s\n' % f)
fout.close()

