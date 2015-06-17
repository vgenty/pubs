import os

#Dictionary of project name => description string
desc_dict = {}

def getProjectDescriptions():

    infile = os.environ['PUB_TOP_DIR']+'/pub_mongui/project_descriptions.txt'

    try:
        f = open(infile,'r')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        print "Tried to open %s."%infile
        quit()

    contents = f.read().split('\n')

    reduced = [i for i in contents if i != '' and i[0] != '#']

    for line in reduced:
        proj_name = line.split()[0]
        proj_desc = line[(len(proj_name)+1):]
        desc_dict[proj_name] = proj_desc

    return desc_dict
