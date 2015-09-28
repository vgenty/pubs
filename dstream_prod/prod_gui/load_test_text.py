import os

#Files managed by each project have two-digit statuses like 1x, 2x, 3x, 4x
#If a status is 1x, that means it is in the first stage (the first fcl file)
#If a status is 11, that means it is in the first stage, substage 1 (IE "job submitted")

#Dictionary of { 'project name' : { 'stage name' : sample_data }
#where sample_data looks like (10, 5, 15)

#Meaning if this stage is stage #2, there are 10 files with status == 21, 5 files with status == 22, 15 with status == 23

testmode_dict = {}

def getTestData(testfile_name = 'test_mode.txt'):
    try:
        f = open(testfile_name,'r')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        print "Tried to open %s."%testfile_name
        quit()

    contents = f.read().split('\n')
    
    for line in contents:
        if line == 'END_PROJECT': 
            testmode_dict[projectname] = blah
        if 'PROJECT_NAME' in line:
            projectname = line[14:]
        if 'SAMPLE_DATA' in line:
            blah = eval(line[13:])

    return testmode_dict
