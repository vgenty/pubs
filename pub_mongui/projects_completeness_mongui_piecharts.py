from pyqtgraph.Qt import QtGui, QtCore
import pyqtgraph as pg
from custom_piechart_class import PieChartItem
import time
# dstream import
from dstream.ds_api import ds_reader
# pub_dbi import
from pub_dbi import pubdb_conn_info
# catch ctrl+C to terminate the program
import signal

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


#Initialize the GUI to show plots from each project
#Initialize Qt (only once per application)
qapp = QtGui.QApplication([])
view = pg.GraphicsView()
#l is a GraphicsLayoutWidget
l = pg.GraphicsLayout()#border=(100,100,100))
view.setCentralItem(l)
view.setWindowTitle('PUBS Monitoring GUI')
view.resize(600,600)
view.show()

#Get a list of all projects from the DBI
projects = dbi.list_all_projects() # [project, command, server, sleepafter .... , enabled, resource]

# Dictionary of project name --> (plotitem, gridx, gridy where it's viewer is stored)
proj_dict = {}

# Loop over projects and add a viewbox plot for each in the window
# Each viewbox is initialized with a pichart w/ hard-coded data
# For now, start with a 5x5 grid, later place them on a diagram
nrows, ncols = 5, 5
rowcount, colcount = 0, 0
for iproj in projects:
    #Initialize all piecharts as filled-in yellow circles
    init_data = (10, 20, 5, [ (1., 'y') ])
    ichart = PieChartItem(init_data)
    iplot = l.addPlot(row=rowcount, col=colcount, title=iproj._project)
    iplot.getViewBox().addItem(ichart)
    proj_dict[iproj._project] = (iplot, rowcount, colcount)
    colcount += 1
    if colcount == ncols: 
        colcount = 0
        rowcount += 1

def update_gui():

    #Get a list of all projects from the DBI
    #Need to repeat this because otherwise when one project gets disabled or something,
    #"projects" needs to be updated to reflect that
    projects = dbi.list_all_projects() # [project, command, server, sleepafter .... , enabled, resource]

    for iproj in projects:
        #Get the number of runs with status 0 from project through the DBI
        n_status0 = len(dbi.get_runs(iproj._project,status=0))
        
        #Get the number of runs with status 1 from project through DBI
        n_status1 = len(dbi.get_runs(iproj._project,status=1))

        #Compute the fraction of the total runs that are complete
        frac_0 = float(n_status0)/(float(n_status0 + n_status1))
        frac_1 = float(n_status1)/(float(n_status0 + n_status1))

        #Update the appropriate viewbox in the GUI with the new pie chart
        #If the project is disabled, make a filled-in red circle
        if iproj._enable == True:
            idata = (10, 20, 5, [ (frac_0, 'b'), (frac_1, 'g') ] )
        else:
            idata = (10, 20, 5, [ (1., 'r') ])
                
        ichart = PieChartItem(idata)
        #Make a new viewbox and replace the corresponding viewbox already in the graphicslayout
        plotitem, myrow, mycol = proj_dict[iproj._project]
        plotitem.getViewBox().addItem(ichart)

timer = QtCore.QTimer()
timer.timeout.connect(update_gui)
timer.start(1000) #once per second, update the plots
signal.signal(signal.SIGINT, signal.SIG_DFL)

if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
