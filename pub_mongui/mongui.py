try:
    from pyqtgraph.Qt import QtGui, QtCore
except ImportError:
    raise ImportError('Ruh roh. You need to set up pyqtgraph before you can use this GUI.')

import pyqtgraph as pg
from custom_piechart_class import PieChartItem
from custom_qgraphicsscene import CustomQGraphicsScene
from custom_qgraphicsview  import CustomQGraphicsView
from gui_utils_api import GuiUtilsAPI, GuiUtils

# catch ctrl+C to terminate the program
import signal
# exponential in piechart radius calculation
import math
# accessing PUBS environment variables
import os
# GUI parameter reader
from load_params import getParams
# Project description text-file parser
from load_proj_descriptions import getProjectDescriptions
# dstream import
from dstream.ds_api import ds_reader
# pub_dbi import
from pub_dbi import pubdb_conn_info

import time
# ==> timeprofiling: importing stuff takes 1.5 seconds

##############################################################
# ==> timeprofiling: comments like these show lines of code that take more than ~0.1 seconds to run.
##############################################################

my_template = 'pubs_diagram_092515.png'
_update_period = GuiUtils().getUpdatePeriod()#in seconds

# GUI DB interface:
gdbi = GuiUtilsAPI()
# ==> timeprofiling: creating gdbi object takes 1.3 seconds (it connects itself to database)

#suppress warnings temporarily:
QtCore.qInstallMsgHandler(lambda *args: None)

#Try using raster to make things faster
QtGui.QApplication.setGraphicsSystem('raster')

#Always start with this
app = QtGui.QApplication([])
# ==> timeprofiling: creating QApplication() instance takes 1.3 seconds

#Load in the background image via pixmap
pm = QtGui.QPixmap(os.environ['PUB_TOP_DIR']+'/pub_mongui/gui_template/'+my_template)
# ==> timeprofiling: loading in this pixmap takes 0.1 seconds

#Make the scene the same size as the background template
scene_xmin, scene_ymin, scene_width, scene_height = 0, 0, pm.width(), pm.height()

#Make the scene the correct size
scene = CustomQGraphicsScene(scene_xmin,scene_ymin,scene_width,scene_height)

#Make custom (zoomable) view from the scene and show it
view = CustomQGraphicsView(scene,pm)

view.setCacheMode(QtGui.QGraphicsView.CacheBackground)
#view.setViewportUpdateMode(QtGui.QGraphicsView.NoViewportUpdate)
#view.ensureVisible(scene.sceneRect())
#view.fitInView(scene.sceneRect(),QtCore.Qt.KeepAspectRatio)
view.setRenderHint(QtGui.QPainter.Antialiasing)
#Enforce the view to align with upper-left corner
#view.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignTop)
view.show()
# ==> timeprofiling: view.show() takes 0.2 seconds

#Get a list of all projects from the gui DBI
projectnames = gdbi.getAllProjectNames() 

# Dictionary of project name --> pie chart item
proj_dict = {}

#Read in the parameters for this template into a dictionary
#These dictate, based on project name, where to draw on GUI
template_params = getParams(my_template)

#Read in the project descriptions stored in a separate text file
proj_descripts = getProjectDescriptions()
# ==> timeprofiling: getting project descriptions takes 0.1 seconds

#Daemon text item (stored in array because that's the only way I can get it to work)
daemon_texts = []
daemon_text, daemon_warning = gdbi.genDaemonTextAndWarnings()
daemon_text.setPos(scene_xmin+0.02*scene_width,scene_height*0.95)
scene.addItem(daemon_text)
daemon_texts.append(daemon_text)
# ==> timeprofiling: generating daemon text takes a longass time, until the daemon_log(start,end) function is fixed.

for iprojname in projectnames:

    if iprojname not in template_params:
        #Commenting this so it doesn't scare shifters
        #print "Uh oh. Project %s doesn't have parameters to load it to the template. I will not draw this project." % iprojname
        continue    
    
    #Initialize all piecharts as filled-in yellow circles, with radius = max radius for that project
    xloc, yloc, maxradius = template_params[iprojname]
    xloc, yloc, maxradius = float(xloc), float(yloc), float(maxradius)

    ichart = PieChartItem((iprojname,scene_xmin+scene_width*xloc, scene_ymin+scene_height*yloc, maxradius, 0, [ (1., 'y') ]))

    #Initialize the piechart description from the stored text file
    if iprojname in proj_descripts.keys():
        ichart.setDescript(proj_descripts[iprojname])
        
    #Add the piecharts to the scene (piechart location is stored in piechart object)
    scene.addItem(ichart)

    #Store the piechart in a dictionary to modify it later, based on project name
    proj_dict[iprojname] = ichart

# ==> timeprofiling: creating all piecharts and adding them to the scene takes 0.002 seconds **************

#Add a legend to the bottom right #to do: make legend always in foreground
mytext = QtGui.QGraphicsTextItem()
mytext.setPos(scene_xmin+0.80*scene_width,scene_height*0.90)
mytext.setPlainText('Legend:\nBlue: Pending Files (Good)\nColorful: Error status.\nGray: Project Disabled')
mytext.setDefaultTextColor(QtGui.QColor('white'))
myfont = QtGui.QFont()
myfont.setPointSize(10)
mytext.setFont(myfont)
scene.addItem(mytext)

def update_gui():
    # ==> timeprofiling: entire update_gui function takes 1.2 seconds if you take out the daemon text stuff
    # ==> timeprofiling: if you include daemon text stuff, update_gui takes 3.2 seconds
    #todo: put all of this in a separate thread, perhaps

    #This is the one DB query that returns all projects and array of different statuses per project
    gdbi.update()

    #Remove the daemon text item from scene
    scene.removeItem(daemon_texts[0])
    #Remove the daemon text item from array storing it globally
    daemon_texts.pop(0)
    #re-draw the daemon text item
    daemon_text, daemon_warning = gdbi.genDaemonTextAndWarnings()
    daemon_text.setPos(scene_xmin+0.02*scene_width,scene_height*0.95)
    scene.addItem(daemon_text)
    #Store the new daemon text item
    daemon_texts.append(daemon_text)
    #If there were any warnings, open a window shouting at shifters
    if daemon_warning:
        dwarnings = scene.openDaemonWindow(daemon_warning)
    
    #Get a list of all projects from the DBI
    #Need to repeat this because otherwise when one project gets disabled or something,
    #"projects" needs to be updated to reflect that
    #This no longer does a DB query, just reads info from previous query in gdbi.update()
    projectnames = gdbi.getAllProjectNames()
    enabledprojectnames = gdbi.getEnabledProjectNames()

    for iprojname in projectnames:

        #If this project isn't in the dictionary (I.E. it wasn't ever drawn on the GUI),
        #then skip it. This can be fixed by adding the project to the GUI params
        if iprojname not in proj_dict.keys():
            continue

        #First, store the corrent data from the piechart, to check if needs to be re-drawn
        old_tot_n = proj_dict[iprojname].getTotalFiles()
        old_slices = proj_dict[iprojname].getSlices()

        #Store the piechart x,y coordinate
        ix, iy = proj_dict[iprojname].getCenterPoint()
        #Get the maximum radius of for this pie chart from the template parameters
        max_radius = float(template_params[iprojname][2])
        #Compute the number of entries in the pie chart (denominator)
        tot_n = gdbi.getTotNRunSubruns(iprojname)
        #Compute the radius if the pie chart, based on the number of entries
        ir = gdbi.computePieRadius(iprojname, max_radius, tot_n)
        #Compute the0000gn/T/s slices of the pie chart
        pie_slices = gdbi.computePieSlices(iprojname)

        #If the project is disabled, make a filled-in gray circle
        if iprojname not in enabledprojectnames: pie_slices = [ (1., 0.2) ]
    
        #Set the new data that will be used to make a new pie chart
        idata = (iprojname, ix, iy, ir, tot_n, pie_slices )

        #update the piechart item with the new data
        proj_dict[iprojname].updateData(idata)
        
        #appendHistory should take in [ (status, value), (anotherstatus, anothervalue), ...]
        proj_dict[iprojname].appendHistory(gdbi.getNRunSubruns(iprojname))

        #If nothing has changed about the pie chart, don't bother redrawing it.
        if old_tot_n == tot_n and old_slices == pie_slices: continue
        
        #Remove the old item from the scene
        scene.removeItem(proj_dict[iprojname])

        #Draw the new piechart in the place of the old one
        scene.addItem(proj_dict[iprojname])

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

#Initial drawing of GUI with real values
#This is also the function that is called to update the canvas periodically
update_gui()

timer = QtCore.QTimer()
timer.timeout.connect(update_gui)
timer.start(_update_period*1000.) #Frequency with which to update plots, in milliseconds

#Catch ctrl+C to close things
signal.signal(signal.SIGINT, signal.SIG_DFL)


if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
