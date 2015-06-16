try:
    from pyqtgraph.Qt import QtGui, QtCore
except ImportError:
    raise ImportError('Ruh roh. You need to set up pyqtgraph before you can use this GUI.')

import pyqtgraph as pg
from custom_piechart_class import PieChartItem
# catch ctrl+C to terminate the program
import signal
# exponential in piechart radius calculation
import math
# accessing PUBS environment variables
import os
# GUI parameter reader
from load_params import getParams
# dstream import
from dstream.ds_api import ds_reader
# pub_dbi import
from pub_dbi import pubdb_conn_info


_update_period = 10#in seconds
my_template = 'pubs_diagram_061515.png'

# DB interface:
global dbi
dbi = ds_reader(pubdb_conn_info.reader_info())

#Establish a connection to the database through the DBI
try:
    dbi.connect()
except:
    print "Unable to connect to database... womp womp :("

#suppress warnings temporarily:
QtCore.qInstallMsgHandler(lambda *args: None)

#Always start with this
app = QtGui.QApplication([])

#Load in the background image via pixmap
pm = QtGui.QPixmap(os.environ['PUB_TOP_DIR']+'/pub_mongui/gui_template/'+my_template)

#Make the scene the same size as the background template
scene_xmin, scene_ymin, scene_width, scene_height = 0, 0, pm.width(), pm.height()

#Make the scene the correct size
scene = QtGui.QGraphicsScene(scene_xmin,scene_ymin,scene_width,scene_height)
#Add the background pixmap to the scene
mypm = scene.addPixmap(pm)
#Set the background so it's upper-left corner matches upper-left corner of scene
mypm.setPos(scene_xmin,scene_ymin)

#Make view from the scene and show it
view = QtGui.QGraphicsView(scene)
#Enforce the view to align with upper-left corner
view.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignTop)
view.show()

#For now, zoom out a little bit so it can fit on my screen ...
view.scale(0.8,0.8)

#Get a list of all projects from the DBI
projects = dbi.list_all_projects() # [project, command, server, sleepafter .... , enabled, resource]

# Dictionary of project name --> pie chart item
proj_dict = {}

#Read in the parameters for this template into a dictionary
#These dictate, based on project name, where to draw on GUI
template_params = getParams(my_template)

for iproj in projects:

    if iproj._project not in template_params:
        print "Uh oh. Project %s doesn't have parameters to load it to the template. I will not draw this project." % iproj._project
        continue    
    
    #Initialize all piecharts as filled-in yellow circles, with radius = max radius for that project
    xloc, yloc, maxradius = template_params[iproj._project]
    xloc, yloc, maxradius = float(xloc), float(yloc), float(maxradius)
    ichart = PieChartItem((scene_xmin+scene_width*xloc, scene_ymin+scene_height*yloc, maxradius, [ (1., 'y') ]))

    #Add the piecharts to the scene (piechart location is stored in piechart object)
    scene.addItem(ichart)
  
    #Store the piechart in a dictionary to modify it later, based on project name
    proj_dict[iproj._project] = ichart

    #Add a legend to the bottom right #to do: make legend always in foreground
    mytext = QtGui.QGraphicsTextItem()
    mytext.setPos(scene_xmin+0.80*scene_width,scene_height*0.90)
    mytext.setPlainText('Legend:\nBlue: Status1\nGreen: Status2\nRed: Project Disabled')
    mytext.setDefaultTextColor(QtGui.QColor('white'))
    myfont = QtGui.QFont()
    myfont.setPointSize(10)
    mytext.setFont(myfont)
    scene.addItem(mytext)
    
def update_gui():

    #Get a list of all projects from the DBI
    #Need to repeat this because otherwise when one project gets disabled or something,
    #"projects" needs to be updated to reflect that
    projects = dbi.list_all_projects() # [project, command, server, sleepafter .... , enabled, resource]

    for iproj in projects:

        #If this project isn't in the dictionary (I.E. it wasn't ever drawn on the GUI),
        #then skip it. This can be fixed by adding the project to the GUI params
        if iproj._project not in proj_dict.keys():
            continue

        #First store the piechart x,y coordinate to re-draw it in the same place later
        ix, iy = proj_dict[iproj._project].getCenterPoint()

        #Status codes: 0 is completed (don't care about these) 1 is initiated, 2 is to be validated, >2 is error?
        #Get the number of runs with status 1 from project through the DBI
        n_status1 = len(dbi.get_runs(iproj._project,status=1))
        
        #Get the number of runs with status 2 from project through DBI
        n_status2 = len(dbi.get_runs(iproj._project,status=2))

        tot_n = n_status1+n_status2;

        #If no runs have status 1 or 2, set pie chart radius to 0 (invisible)
        if not tot_n:
            idata = (ix, iy, 0., [ (1., 'r') ])
        else:
                     
            #Compute the fraction of the total runs that are complete
            frac_1 = float(n_status1)/float(tot_n)
            frac_2 = float(n_status2)/float(tot_n)

            #Set the new data that will be used to make a new pie chart
            #If the project is disabled, make a filled-in red circle
            if iproj._enable == True:
                idata = (ix, iy, computePieChartRadius(tot_n), [ (frac_1, 'b'), (frac_2, 'g') ] )
            else:
                idata = (ix, iy, computePieChartRadius(tot_n), [ (1., 'r') ])

        #Make the replacement piechart
        ichart = PieChartItem(idata)

        #Remove the old item from the scene
        scene.removeItem(proj_dict[iproj._project])

        #Draw the new piechart in the place of the old one
        scene.addItem(ichart)

        #Save the new pie chart in the dictionary, overwriting the old
        proj_dict[iproj._project] = ichart

        #On top of the pie chart, write the number of run/subruns
        #Re-draw the text on top of the pie chart with the project name
        mytext = QtGui.QGraphicsTextItem()
        mytext.setPos(ix,iy)
        mytext.setPlainText(str(tot_n)+'\nRun/Subruns')
        mytext.setDefaultTextColor(QtGui.QColor('white'))
        #mytext.setTextWidth(50)
        myfont = QtGui.QFont()
        myfont.setBold(True)
        myfont.setPointSize(10)
        mytext.setFont(myfont)
        scene.addItem(mytext)

def computePieChartRadius(n_total_runsubruns):
    max_radius = float(template_params[iproj._project][2])
    #Right now use radius = (Rmax/2log(5))log(n_total_runsubruns)
    #unless radius > Rmax, in which case use radius = Rmax
    #This function has r = 0.5*Rmax when n = 5
    radius = (float(max_radius)/(2*math.log(5.))) * log(float(n_total_runsubruns))
    #Double check the radius isn't bigger than the max allowed
    return radius if radius <= max_radius else max_radius

#Initial drawing of GUI with real values
#This is also the function that is called to update the canvas periodically
update_gui()

timer = QtCore.QTimer()
timer.timeout.connect(update_gui)
timer.start(_update_period*1000.) #Frequency with which to update plots, in milliseconds
signal.signal(signal.SIGINT, signal.SIG_DFL)


if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
