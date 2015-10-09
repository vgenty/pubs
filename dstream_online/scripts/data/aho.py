
KEYWORD='NoiseRun'

ubdaq_list = [(int(x.split()[0]),int(x.split()[1])) for x in open('near1_%s_ubdaq.txt' % KEYWORD,'r').read().split('\n') if len(x.split())==3]

ubdaq_json_list = [(int(x.split()[0]),int(x.split()[1])) for x in open('near1_%s_ubdaq_wo_json.txt' % KEYWORD,'r').read().split('\n') if len(x.split())==3]

root_list = [(int(x.split()[0]),int(x.split()[1])) for x in open('near1_%s_root.txt' % KEYWORD,'r').read().split('\n') if len(x.split())==3]

root_json_list = [(int(x.split()[0]),int(x.split()[1])) for x in open('near1_%s_root_wo_json.txt' % KEYWORD,'r').read().split('\n') if len(x.split())==3]

ctr = 0
for x in root_list:
    if x in ubdaq_list:
        ctr+=1
print 'Found %d/%d root run list are part of %d ubdaq run list...' % (ctr,len(root_list),len(ubdaq_list))

ctr = 0
for x in ubdaq_list:
    if x in root_list:
        ctr+=1
print 'Found %d/%d ubdaq run list are part of %d root run list...' % (ctr,len(ubdaq_list),len(root_list))
