import os

#Dictionary of project name => (x location, ylocation) on template
params_dict = {}

def getParams(template_name):

    infile = os.environ['PUB_TOP_DIR']+'/dstream_prod/prod_gui/gui_template/'+template_name
    #Strip image extension to file, add '_params.txt'
    if infile[-4:] == '.png': infile = infile[:-4]
    myfile = infile + '_params.txt'

    try:
        f = open(myfile,'r')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        print "Tried to open %s."%myfile
        quit()

    contents = f.read().split('\n')

    for param in contents:
        if not param: continue
        proj_name, xloc, yloc, pieradius = param.split(' ')
        params_dict[proj_name] = (xloc, yloc, pieradius)

    return params_dict
