try:
    from pyqtgraph.Qt import QtGui, QtCore
except ImportError:
    raise ImportError('Ruh roh. You need to set up pyqtgraph before you can use this GUI.')

import pyqtgraph as pg
from custom_piechart_class import PieChartItem
from custom_qgraphicsscene import CustomQGraphicsScene
from custom_qgraphicsview  import CustomQGraphicsView
from custom_bar_class import ProgressBarItem
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
import sys
import time
# ==> timeprofiling: importing stuff takes 1.5 seconds

##############################################################
# ==> timeprofiling: comments like these show lines of code that take more than ~0.1 seconds to run.
##############################################################
my_template = 'pubs_diagram_BLANKv2.png'#'pubs_diagram_092515.png'
#my_params = 'pubs_diagram_111815_params.txt'
#my_params = 'pubs_diagram_010416_params.txt'
my_params = 'pubs_diagram_010516_params.txt'
_update_period = GuiUtils().getUpdatePeriod()#in seconds
global_update_counter = 0
_max_errors_before_warning = 100 # number of error statuses for a project (when in relative mode) before a warning window pops up

# GUI DB interface:
gdbi = GuiUtilsAPI()

# GUI utils interface
guiut = GuiUtils()

# ==> timeprofiling: creating gdbi object takes 1.3 seconds (it connects itself to database)

#suppress warnings temporarily:
QtCore.qInstallMsgHandler(lambda *args: None)

#Try using raster to make things faster
QtGui.QApplication.setGraphicsSystem('raster')

#Always start with this
app = QtGui.QApplication([])
# ==> timeprofiling: creating QApplication() instance takes 1.3 seconds

#Load in the background image via pixmap
pm = QtGui.QPixmap(os.environ['PUB_TOP_DIR']+'/dstream_prod/prod_gui/gui_template/'+my_template)

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
enabledprojectnames = gdbi.getEnabledProjectNames()

# Dictionary of project name --> pie chart item
proj_dict = {}

# Dictionary of project name --> text on top of project
projsupertext_dict = {}
# Dictionary of project name --> text below project
projsubtext_dict = {}

#Read in the parameters for this template into a dictionary
#These dictate, based on project name, where to draw on GUI
template_params = getParams(my_params)

#Read in the project descriptions stored in a separate text file
proj_descripts = getProjectDescriptions()
# ==> timeprofiling: getting project descriptions takes 0.1 seconds

#Brush to paint white text
text_brush = QtGui.QBrush(QtGui.QColor('white'))
#Pen to outline white text in black
outline_pen = QtGui.QPen(QtGui.QColor('darkGray'))
outline_pen.setWidth(0.01)

#Daemon text item (stored in array because that's the only way I can get it to work)
daemon_text_content, daemon_warning_content = gdbi.genDaemonTextAndWarnings()
daemon_text = QtGui.QGraphicsSimpleTextItem()
daemon_warning = QtGui.QGraphicsTextItem()
daemon_text.setBrush(text_brush)
# daemon_text.setPen(outline_pen)
# daemon_warning.setBrush(text_brush)
# daemon_warning.setPen(outline_pen)
daemon_text.setPos(scene_xmin+0.02*scene_width,scene_height*0.93)
daemon_text.setText(daemon_text_content)
# daemon_text.setDefaultTextColor(QtGui.QColor('white'))
myfont = QtGui.QFont()
myfont.setPointSize(13)
myfont.setBold(True)
daemon_text.setFont(myfont)
daemon_warning.setPlainText(daemon_warning_content)
daemon_warning.setDefaultTextColor(QtGui.QColor('white'))
warningfont = QtGui.QFont()
warningfont.setPointSize(50)
daemon_warning.setFont(warningfont)
scene.addItem(daemon_text)
# ==> timeprofiling: generating daemon text takes a longass time, until the daemon_log(start,end) function is fixed.

# last-updated timestamp:
last_update_text = QtGui.QGraphicsTextItem()
last_update_text.setPos(scene_xmin+0.02*scene_width, scene_height*0.88)
last_update_text.setPlainText("GUI Initialized, has not yet updated.")
last_update_text.setDefaultTextColor(QtGui.QColor('white'))
last_update_text.setFont(myfont)
scene.addItem(last_update_text)

def resetCounters():
    gdbi.resetCounters()

reset_button = QtGui.QPushButton()
reset_button.setText("Reset Counters")
reset_button.setMinimumSize(QtCore.QSize(0,0))
reset_button.setMaximumSize(QtCore.QSize(10000,10000))
reset_button.setGeometry(scene_xmin+0.10*scene_width, 0.10*scene_height,200,50)
# reset_button.setStyleSheet("border-style: outset; border-width: 2px; border-radius: 0px; border-color: beige; font: bold 15px; color: black; padding: 4px;")
reset_button_widget = scene.addWidget(reset_button)
reset_button_widget.setZValue(3.0)
reset_button.clicked.connect(resetCounters)

relative_counter_checkbox = QtGui.QCheckBox()
relative_counter_checkbox.setText("Use Relative Counters")
relative_counter_checkbox.setGeometry(scene_xmin+0.10*scene_width, 0.05*scene_height,200,25)
relative_counter_checkbox.setStyleSheet("color: white; background-color: transparent; font: bold 15px; min-width: 15em")
relative_counter_checkbox.setAutoFillBackground(True)
relative_counter_checkbox.setChecked(False)
relative_counter_checkbox_widget = scene.addWidget(relative_counter_checkbox)
relative_counter_checkbox_widget.setZValue(3.0)


for iprojname in projectnames:
    print iprojname
    if iprojname not in template_params:
        #Commenting this so it doesn't scare shifters
        print "Uh oh. Project %s doesn't have parameters to load it to the template. I will not draw this project." % iprojname
        continue    
    
    #Initialize all piecharts as filled-in yellow circles, with radius = max radius for that project
    xloc, yloc, maxradius, parents = template_params[iprojname]
    xloc, yloc, maxradius = float(xloc), float(yloc), float(maxradius)

    #Make a progress bar item for this project
    ichart = ProgressBarItem((iprojname,scene_xmin+scene_width*xloc, scene_ymin+scene_height*yloc, maxradius, 0, [ (1., 'y') ]))

    #Make sure progress bars are always in the front (all default z values are 0)
    ichart.setZValue(2.0)

    #Initialize the piechart description from the stored text file
    if iprojname in proj_descripts.keys():
        ichart.setDescript(proj_descripts[iprojname])
        
    #Add the piecharts to the scene (piechart location is stored in piechart object)
    scene.addItem(ichart)

    #Store the piechart in a dictionary to modify it later, based on project name
    proj_dict[iprojname] = ichart

    ###################################
    #Fill the piechart with real values
    ###################################

    #This is the one DB query that returns all projects and array of different statuses per project
    gdbi.update()

    #Store the piechart x,y coordinate
    ix, iy = proj_dict[iprojname].getCenterPoint()
    #Get the maximum radius of for this pie chart from the template parameters
    max_radius = float(template_params[iprojname][2])
    #Compute the number of entries in the pie chart (denominator)
    # tot_n = gdbi.getTotNRunSubruns(iprojname)
    tot_n = gdbi.getScaledNRunSubruns(iprojname,use_relative=relative_counter_checkbox.isChecked())
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
 
    #On top of the pie chart, write the number of run/subruns
    #Re-draw the text on top of the pie chart with the project name
    mysupertext = QtGui.QGraphicsSimpleTextItem()
    mysupertext.setBrush(text_brush)
    mysupertext.setPen(outline_pen)
    mysupertext.setZValue(2.0)
    mysupertext.setPos(ix,iy-proj_dict[iprojname].getHeight())
    mysupertext.setText(proj_dict[iprojname].getDescript())#iprojname)
    # mysupertext.setDefaultTextColor(QtGui.QColor('white'))

    myfont = QtGui.QFont()
    myfont.setBold(True)
    myfont.setPointSize(12)
    mysupertext.setFont(myfont)
    scene.addItem(mysupertext)
    #Store the text in a dictionary
    projsupertext_dict[iprojname] = mysupertext

    mysubtext = QtGui.QGraphicsSimpleTextItem()
    mysubtext.setBrush(text_brush)
    mysubtext.setPen(outline_pen)
    mysubtext.setZValue(2.0)
    mysubtext.setPos(ix,iy+proj_dict[iprojname].getHeight())
    ngood, ninit, ninter, nerr = gdbi.getScaledNGoodInterError(iprojname,use_relative=relative_counter_checkbox.isChecked())
    mysubtext.setText('%d Complete\n%d Queued\n%d Intermediate\n%d Error'%(ngood, ninit, ninter, nerr))
    # mysubtext.setDefaultTextColor(QtGui.QColor('white'))
    myfont = QtGui.QFont()
    myfont.setBold(True)
    myfont.setPointSize(12)
    mysubtext.setFont(myfont)
    scene.addItem(mysubtext)
    #Store the text in a dictionary
    projsubtext_dict[iprojname] = mysubtext


#######################################################################
############## Draw animated arrows from parent --> daughter projects
#######################################################################
arrows = {}
animations = {}

# animation_timer = QtCore.QTimeLine(5000)
# #loop infinitely
# animation_timer.setLoopCount(0)
# animation_timer.setFrameRange(0,100)

# line_pen = QtGui.QPen(QtGui.QBrush('w'),5)
line_pen = QtGui.QPen(QtGui.QColor('darkCyan'))
line_pen.setWidth(6)
for iprojname in projectnames:
    if iprojname not in template_params:
        continue    

    xloc, yloc, maxradius, parents = template_params[iprojname]

    #For each parent of this project, draw a line from it to the parent and have an animated arrow
    for parent in parents:
        if parent not in proj_dict.keys():
            continue
        endpoint = proj_dict[iprojname].getCenterPoint()
        startpoint = proj_dict[parent].getCenterPoint()
        spx, spy = startpoint[0], startpoint[1]
        epx, epy = endpoint[0], endpoint[1]
        spx += proj_dict[parent].getRadius()*0.5
        spy += proj_dict[parent].getHeight()*0.5
        epx += proj_dict[iprojname].getRadius()*0.5
        epy += proj_dict[iprojname].getHeight()*0.5

        #Let's try lines instead of arrows
        myline = scene.addLine(spx,spy,epx,epy,pen=line_pen)
        myline.setZValue(1.0)
        # if parent == 'prod_verify_binary_evb2dropbox_near1':
        #     print "starting at Binary Transfer Validation to %s"%iprojname
        #     print "(%f,%f) ==> (%f,%f)"%(spx,spy,epx,epy)
        # arrows[iprojname] = guiut.getArrowObject((spx,spy),(epx,epy))
        # arrows[iprojname].setPos(spx,spy)
        # arrows[iprojname].setPos(epx,epy)
        # animations[iprojname] = QtGui.QGraphicsItemAnimation()
        # animations[iprojname].setItem(arrows[iprojname])
        # animations[iprojname].setTimeLine(animation_timer)
        # animations[iprojname].setPosAt(0,QtCore.QPointF(spx,spy))
        # animations[iprojname].setPosAt(1,QtCore.QPointF(epx,epy))
        # scene.addItem(arrows[iprojname])
#start the animations running
# animation_timer.start()
#######################################################################
############## End draw animated arrows from parent --> daughter projects
#######################################################################

#Add a static legend to the bottom right #to do: make legend always in foreground
mytext = QtGui.QGraphicsSimpleTextItem()
mytext.setBrush(text_brush)
mytext.setPen(outline_pen)
mytext.setPos(scene_xmin+0.7*scene_width,scene_height*0.88)
mytext.setText('Legend:\nGreen (Queue): 0, 1, 2\nOrange (Running): 3, 4\nRed (Error): >=1000\nGray: Project Disabled')
# mytext.setDefaultTextColor(QtGui.QColor('white'))
myfont = QtGui.QFont()
myfont.setPointSize(12)
mytext.setFont(myfont)
scene.addItem(mytext)

#warning message that pops up if GUI cannot connect to DB (or connection to DB ever fails)
conn_warning = None

def update_gui():
    global global_update_counter
    global_update_counter += 1
    force_recreate_daemonwindow = True if not global_update_counter%1000 else False
  
    # ==> timeprofiling: entire update_gui function takes 1.2 seconds if you take out the daemon text stuff
    # ==> timeprofiling: if you include daemon text stuff, update_gui takes 3.2 seconds
    #todo: put all of this in a separate thread, perhaps

    #Hide "reset" button if reset checkbox isn't selected
    if not relative_counter_checkbox.isChecked(): reset_button_widget.hide()
    else: reset_button_widget.show()

    #This is the one DB query that returns all projects and array of different statuses per project
    gdbi.update()

    daemon_text_content, daemon_warning_content = gdbi.genDaemonTextAndWarnings()
  
    #Change the text on the already-created daemon text
    daemon_text.setText(daemon_text_content)
    daemon_warning.setPlainText(daemon_warning_content)
    #If there were any warnings, open a window shouting at shifters
    if daemon_warning_content:      
        dwarnings = scene.openDaemonWindow(daemon_warning,force_recreate = force_recreate_daemonwindow)
    
    #Check if gui query thread is successfully connected to the DB
    if not gdbi.getIsConnAlive():
        global conn_warning
        if not conn_warning:
            conn_warning_content = gdbi.genConnWarningText()
            conn_warning = QtGui.QGraphicsTextItem()
            conn_warning.setPlainText(conn_warning_content)
            conn_warning.setDefaultTextColor(QtGui.QColor('white'))
            warningfont = QtGui.QFont()
            warningfont.setPointSize(50)
            conn_warning.setFont(warningfont)
            gwarnings = scene.openDaemonWindow(conn_warning,force_recreate = force_recreate_daemonwindow)
        else:
            gwarnings = scene.openDaemonWindow(conn_warning,force_recreate = force_recreate_daemonwindow)

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

        #Store the piechart x,y coordinate
        ix, iy = proj_dict[iprojname].getCenterPoint()
        #Get the maximum radius of for this pie chart from the template parameters
        max_radius = float(template_params[iprojname][2])
        #Compute the number of entries in the pie chart (denominator)
        # tot_n = gdbi.getTotNRunSubruns(iprojname)
        tot_n = gdbi.getScaledNRunSubruns(iprojname)
        #Compute the radius if the pie chart, based on the number of entries
        ir = gdbi.computePieRadius(iprojname, max_radius, tot_n)
        #Compute the0000gn/T/s slices of the pie chart
        pie_slices = gdbi.computePieSlices(iprojname,use_relative=relative_counter_checkbox.isChecked())

        #If the project is disabled, make a filled-in gray circle
        if iprojname not in enabledprojectnames: pie_slices = [ (1., 0.2) ]
    
        #Set the new data that will be used to make a new pie chart
        idata = (iprojname, ix, iy, ir, tot_n, pie_slices )

        #update the piechart item with the new data
        proj_dict[iprojname].updateData(idata)
        
        #appendHistory should take in [ (status, value), (anotherstatus, anothervalue), ...]
        proj_dict[iprojname].appendHistory(gdbi.getNRunSubruns(iprojname))

        #Below the pie chart, update the written number of run/subruns
        ngood, ninit, ninter, nerr = gdbi.getScaledNGoodInterError(iprojname,use_relative=relative_counter_checkbox.isChecked())#proj_dict[iprojname].getHistory())    
        projsubtext_dict[iprojname].setText('%d Complete\n%d Queued\n%d Intermediate\n%d Error'%(ngood, ninit, ninter, nerr))
        #If in relative mode and more than 100 statuses for a project are error, throw a warning
        if relative_counter_checkbox.isChecked() and nerr > _max_errors_before_warning:
            warning_message_str = "Project %s has too many errors! It has %d errors (in relative mode). Tell a PUBS expert!" % (iprojname,nerr)
            warning_message = scene.openDaemonWindow(warning_message_str,force_recreate = force_recreate_daemonwindow)


    #Last updated timestamp (last time the querying thread finished a query):
    last_update_text.setPlainText("GUI Last Updated: "+gdbi.getLastUpdatedString())

    #Redraw everything in the scene. No need to create/destroy pie charts every time
    scene.update()

timer = QtCore.QTimer()
timer.timeout.connect(update_gui)
timer.start(_update_period*1000.) #Frequency with which to update plots, in milliseconds

#Catch ctrl+C to close things
signal.signal(signal.SIGINT, signal.SIG_DFL)

if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
