try:
    from pyqtgraph.Qt import QtGui, QtCore
except ImportError:
    raise ImportError('Ruh roh. You need to set up pyqtgraph before you can use this GUI.')

import pyqtgraph as pg
from custom_piechart_class import PieChartItem
from custom_qgraphicsscene import CustomQGraphicsScene
from custom_qgraphicsview  import CustomQGraphicsView
from custom_qcombobox import CustomComboBox
from custom_project_subwindow import CustomStageSubwindow
#from gui_utils_api import GuiUtilsAPI, GuiUtils

tmpcolors = ('r','b','g','o','m','y')

# catch ctrl+C to terminate the program
import signal
# exponential in piechart radius calculation
import math
# accessing PUBS environment variables
import os
# GUI parameter reader
from load_params import getParams
# dstream import
#from dstream.ds_api import ds_reader
# pub_dbi import
#from pub_dbi import pubdb_conn_info

import time

_testmode = True
from load_test_text import getTestData

my_template = 'prod_diagram_092815.png'
_update_period = 1#GuiUtils().getUpdatePeriod()#in seconds

# GUI DB interface:
#gdbi = GuiUtilsAPI()

#suppress warnings temporarily:
QtCore.qInstallMsgHandler(lambda *args: None)

#Try using raster to make things faster
QtGui.QApplication.setGraphicsSystem('raster')

#Always start with this
app = QtGui.QApplication([])

#Load in the background image via pixmap
pm = QtGui.QPixmap(os.environ['PUB_TOP_DIR']+'/dstream_prod/prod_gui/gui_template/'+my_template)

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

if not _testmode:
    print "Right now this is all only coded for test mode!!"
    quit()

#Get a list of all projects from the gui DBI
test_dict = getTestData()
projectnames = test_dict.keys()

# Dictionary of project name --> pie chart item
proj_dict = {}

# Dictionary of project name --> combobox next to pie chart
combobox_dict = {}

# Dictionary of project name --> Legend text item
legend_dict = {}

#Read in the parameters for this template into a dictionary
#These dictate, based on project name, where to draw on GUI
template_params = getParams(my_template)

def openStageSubwindow(stagename):
    #Loop over projects and see if any have this stagename
    #note duplicate stagenames across projects are a problem
    for projectname in test_dict.keys():
        try: 
            test_dict[projectname][str(stagename)]
        except KeyError:
            continue
        newstagesubwindow = scene.openStageSubwindow(stagename,data=test_dict[projectname][str(stagename)])


for iprojname in projectnames:
    #Initialize all piecharts as filled-in yellow circles, with radius = max radius for that project
    ix, iy, max_radius = template_params[iprojname]
    ix, iy, max_radius = float(ix), float(iy), float(max_radius)
    xloc, yloc = scene_xmin+scene_width*ix, scene_ymin+scene_height*iy
    #Create and store the piechart in a dictionary to modify it later, based on project name
    proj_dict[iprojname] = PieChartItem((iprojname,xloc,yloc, max_radius, 0, [ (1., 'y') ]))

    #Fill pie chart with meaningful data:
    #Compute pie slices from test data
    stages_dict = test_dict[iprojname]
    tot_n = 0
    for stagename, mytuple in stages_dict.iteritems():
        tot_n += sum(mytuple)
    counter = 0
    pie_slices = []
    legendstring = 'PROJECT = %s\n'%iprojname
    legendstring += 'Stagename ==> Total Files\n\n'
    #stages_dict looks like {'reco_2d.fcl': (10, 5, 15), 'reco_3d.fcl': (1, 29, 100)}
    for stagename, mytuple in stages_dict.iteritems():
        ifrac = float(sum(mytuple))/float(tot_n)
        pie_slices.append( ( ifrac, tmpcolors[counter] ) )
        legendstring += '%s ==> %d\n'%(stagename,sum(mytuple))
        counter += 1        
    ir = max_radius

    data = (iprojname, xloc, yloc, ir, tot_n, pie_slices )

    #update the piechart item with the new data
    proj_dict[iprojname].updateData(data)
    
    #OLD: appendHistory should take in [ (status, value), (anotherstatus, anothervalue), ...]
    ##CURRENT appendHistory should take in {'reco_2d.fcl': (10, 5, 15), 'reco_3d.fcl': (1, 29, 100)}
    proj_dict[iprojname].appendHistory(stages_dict)
    #If nothing has changed about the pie chart, don't bother redrawing it.
    #if old_tot_n == tot_n and old_slices == pie_slices: continue
    
    # #Remove the old item from the scene
    # scene.removeItem(proj_dict[iprojname])
    # #Draw the new piechart in the place of the old one
    # scene.addItem(proj_dict[iprojname])
    # #Next to the pie chart, write the number of run/subruns for each stage name
    legend_dict[iprojname] = QtGui.QGraphicsTextItem()
    legend_dict[iprojname].setPos(xloc+max_radius,yloc-(max_radius/2))
    legend_dict[iprojname].setPlainText(str(legendstring))
    legend_dict[iprojname].setDefaultTextColor(QtGui.QColor('white'))
    #legend_dict[iprojname].setTextWidth(50)
    myfont = QtGui.QFont()
    myfont.setBold(True)
    myfont.setPointSize(10)
    legend_dict[iprojname].setFont(myfont)
    scene.addItem(legend_dict[iprojname])

    #Add the piechart to the scene (piechart location is stored in piechart object)
    scene.addItem(proj_dict[iprojname])

    #Clickable drop-down menu for each pie chart
    butt_x, butt_y = proj_dict[iprojname].getCenterPoint()
    butt_w, butt_h = 50, 30
    butt_x -= float(max_radius)
    butt_y += 1.2*float(max_radius)

    #Add (and store in a dict) one clickable menu for this project next to the pie chart
    combobox_dict[iprojname] = QtGui.QComboBox()
    combobox_dict[iprojname].setGeometry(butt_x,butt_y,butt_w,butt_h)

    #Generate the dropdown menu options (stage names)
    counter = 0
    for stagename in test_dict[iprojname]:
        combobox_dict[iprojname].addItem(stagename)
        combobox_dict[iprojname].setItemData(counter,tmpcolors[counter],QtCore.Qt.BackgroundRole)
        counter += 1

    #Resize combobox to be length of longest item, for aesthetic reasons
    width = combobox_dict[iprojname].minimumSizeHint().width()
    combobox_dict[iprojname].setMinimumWidth(width)

    #When the menu item is clicked, call the openSubwindow function with the stagename as an argument
    combobox_dict[iprojname].activated[str].connect(openStageSubwindow)

    #Add the dropdown menu button to the scene
    scene.addWidget(combobox_dict[iprojname])

def update_gui():
    #Reload data so user can change text file from underneath:
    #Get a list of all projects from the gui DBI
    test_dict = getTestData()

    for iprojname in projectnames:
        # print "Project =",iprojname
        #If this project isn't in the dictionary (I.E. it wasn't ever drawn on the GUI),
        #then skip it. This can be fixed by adding the project to the GUI params
        if iprojname not in proj_dict.keys():
            print "wtffff"
            continue

        if iprojname not in test_dict.keys():
            print "seriously wtf"
            continue

        #First, store the corrent data from the piechart, to check if needs to be re-drawn
        # old_tot_n = proj_dict[iprojname].getTotalFiles()
        # old_slices = proj_dict[iprojname].getSlices()

        #Store the piechart x,y coordinate
        ix, iy = proj_dict[iprojname].getCenterPoint()
        #Get the maximum radius of for this pie chart from the template parameters
        max_radius = float(template_params[iprojname][2])
        
        #Compute pie slices from test data
        stages_dict = test_dict[iprojname]
        tot_n = 0
        for stagename, mytuple in stages_dict.iteritems():
            tot_n += sum(mytuple)

        counter = 0
        pie_slices = []

        legendstring = 'PROJECT = %s\n'%iprojname
        legendstring += 'Stagename ==> Total Files\n\n'
        #stages_dict looks like {'reco_2d.fcl': (10, 5, 15), 'reco_3d.fcl': (1, 29, 100)}
        for stagename, mytuple in stages_dict.iteritems():
            ifrac = float(sum(mytuple))/float(tot_n)
            pie_slices.append( ( ifrac, tmpcolors[counter] ) )
            legendstring += '%s ==> %d\n'%(stagename,sum(mytuple))
            counter += 1        

        ir = max_radius

        # #Compute the number of entries in the pie chart (denominator)
        # tot_n = gdbi.getTotNRunSubruns(iprojname)
        # #Compute the radius if the pie chart, based on the number of entries
        # ir = gdbi.computePieRadius(iprojname, max_radius, tot_n)
        # #Compute the0000gn/T/s slices of the pie chart
        # pie_slices = gdbi.computePieSlices(iprojname)

        # #If the project is disabled, make a filled-in gray circle
        # if iprojname not in enabledprojectnames: pie_slices = [ (1., 0.2) ]
    
        #Set the new data that will be used to make a new pie chart
        idata = (iprojname, ix, iy, ir, tot_n, pie_slices )

        #update the piechart item with the new data
        proj_dict[iprojname].updateData(idata)
        
        #OLD: appendHistory should take in [ (status, value), (anotherstatus, anothervalue), ...]
        ##CURRENT appendHistory should take in {'reco_2d.fcl': (10, 5, 15), 'reco_3d.fcl': (1, 29, 100)}
        proj_dict[iprojname].appendHistory(stages_dict)

        #If nothing has changed about the pie chart, don't bother redrawing it.
        #if old_tot_n == tot_n and old_slices == pie_slices: continue
        
        # #Remove the old item from the scene
        # scene.removeItem(proj_dict[iprojname])

        # #Draw the new piechart in the place of the old one
        # scene.addItem(proj_dict[iprojname])

        # #Next to the pie chart, write the number of run/subruns for each stage name
        # if not legend_textitem:
        #     legend_textitem = QtGui.QGraphicsTextItem()
        #     legend_textitem.setPos(ix+max_radius,iy-(max_radius/2))
        #     legend_textitem.setPlainText(str(legendstring))
        #     legend_textitem.setDefaultTextColor(QtGui.QColor('white'))
        #     #legend_textitem.setTextWidth(50)
        #     myfont = QtGui.QFont()
        #     myfont.setBold(True)
        #     myfont.setPointSize(10)
        #     legend_textitem.setFont(myfont)
        #     scene.addItem(legend_textitem)
        legend_dict[iprojname].setPlainText(str(legendstring))

        scene.update()

#Initial drawing of GUI with real values
#This is also the function that is called to update the canvas periodically
#now this is done in the top of the script
#update_gui()

timer = QtCore.QTimer()
timer.timeout.connect(update_gui)
timer.start(_update_period*1000.) #Frequency with which to update plots, in milliseconds

#Catch ctrl+C to close things
signal.signal(signal.SIGINT, signal.SIG_DFL)


if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
