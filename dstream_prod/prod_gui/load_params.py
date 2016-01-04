import os

#Dictionary of project name => (x location, ylocation) on template
params_dict = {}

def getParams(template_name):

    infile = os.environ['PUB_TOP_DIR']+'/dstream_prod/prod_gui/gui_template/'+template_name
    #Strip image extension to file, add '_params.txt'
    if infile[-4:] == '.png': 
        infile = infile[:-4]
        myfile = infile + '_params.txt'
    else:
        myfile = infile

    try:
        f = open(myfile,'r')
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        print "Tried to open %s."%myfile
        quit()

    contents = f.read().split('\n')


    for param in contents:
        proj_name, xloc, yloc, pieradius, parents = '', '', '', '', ''
        if not param: continue
        nparams = len(param.split())
        if nparams < 4: print "wtf error"
        elif nparams == 4: proj_name, xloc, yloc, pieradius = param.split()
        elif nparams == 5: 
            proj_name, xloc, yloc, pieradius, parents = param.split()
            parents = parents.split('::')
        params_dict[proj_name] = (xloc, yloc, pieradius, parents)

    return params_dict
