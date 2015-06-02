from pyqtgraph.Qt import QtGui, QtCore
import pyqtgraph.multiprocess as mp
import pyqtgraph as pg
import time

# dstream import
from dstream.ds_api import ds_reader
# pub_dbi import
from pub_dbi import pubdb_conn_info

def main():
    
    # DB interface:
    global dbi
    dbi = ds_reader(pubdb_conn_info.reader_info())

    #Establish a connection to the database through the DBI
    try:
        dbi.connect()
    except:
        print "Unable to connect to database... womp womp :("

    #Get a list of all projects from the DBI
    projects = dbi.list_all_projects() # [project, command, server, sleepafter .... , enabled, resource]

    #Initialize the GUI to show plots from each project
    #Returns a dictionary of project name --> handles to data and curves
    datas_curves = init_gui(projects)   

    while(True):
        #Every second, we will be updating the qt plots
        time.sleep(1)
        
        #This function queries the database and adds a point to each project's plot
        update_gui(datas_curves)

def init_gui(list_of_projects):

    #Initialize Qt (only once per application)
    qapp = QtGui.QApplication([])

    # Create remote process with a plot window
    # This is how I have live-updating plots that keep a record of previous data points
    proc = mp.QtProcess()
    rpg = proc._import('pyqtgraph')
    win = rpg.GraphicsWindow(title='PUBS Monitoring GUI')
    win.resize(1000,600)
    win.setWindowTitle('PUBS Monitoring GUI')
    rpg.setConfigOptions(antialias=True)

    #This is a dictionary with key==project name, val = [data, curve]
    datas_curves = {}
    # Loop over projects and add a plot for each in the window
    # For now, I will do a 2x2 grid of projects and ignore more than 4
    # This obviously needs to be tailored to the number of production PUBS projects
    counter = 0
    nrows = 3
    ncols = 3
    for iproj in list_of_projects:
        p = win.addPlot(title=iproj._project)
        curve = p.plot(pen='y')
        # Create an empty list in the remote process
        data = proc.transfer([])
        datas_curves[iproj._project] = (data, curve)
        counter += 1
        if counter == nrows*ncols-1: break
        if (counter+1)%ncols == 0: win.nextRow()

    return datas_curves

def update_gui(datas_curves):

    for projname, data_curve in datas_curves.iteritems():
        #Get the number of runs with status 0 from project through the DBI
        n_status0 = len(dbi.get_runs(projname,status=0))
        
        #Get the number of runs with status 1 from project through DBI
        n_status1 = len(dbi.get_runs(projname,status=1))

        #Compute the fraction of the total runs that are complete
        frac_complete = float(n_status0)/(float(n_status0 + n_status1))

        # Send new data point to the remote process and plot it
        # We use the special argument _callSync='off' because we do
        # not want to wait for a return value.
        data_curve[0].extend([frac_complete], _callSync='off')
        data_curve[1].setData(y=data_curve[0], _callSync='off')

if __name__ == '__main__':
    main()
   
