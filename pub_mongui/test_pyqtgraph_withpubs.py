import pyqtgraph as pg
import psycopg2
import time

def main(xferhandle, curve):
    
    #Establish a connection to the database
    try:
        conn = psycopg2.connect("dbname='procdb' user='postgres' host='localhost' password=''")
    except:
        print "Unable to connect to database... womp womp :("

    #Define a psycopg cursor to work with (different from psql cursor)
    cur = conn.cursor()

    #Every second, we will be updating the qt graph
    while(True):
        time.sleep(1)
        #Have the cursor execute one of the predefined functions procdb allows
        cur.execute("""SELECT GetRuns('dummy_nubin_xfer',0);""")

        #Read the results from the cursor into python data
        rows = cur.fetchall()

        #Print the results
        #for row in rows:
        #    print "    ", row[0]

        #With another query, compute the fraction of run/subruns that are complete
        n_status0 = len(rows)

        cur.execute("""SELECT GetRuns('dummy_nubin_xfer',1);""")
        rows = cur.fetchall()
        n_status1 = len(rows)

        n_total = n_status0 + n_status1

        frac_status0 = float(n_status0)/float(n_total)

        update_gui(frac_status0, xferhandle, curve)


def init_gui():
    qapp = pg.mkQApp()
    import pyqtgraph.multiprocess as mp
    # Create remote process with a plot window
    proc = mp.QtProcess()
    rpg = proc._import('pyqtgraph')
    plotwin = rpg.plot()
    xaxis = plotwin.getAxis('bottom')
    xaxis.setGrid(255)
    xaxis.setLabel('Seconds since you started this script.')
    yaxis = plotwin.getAxis('left')
    yaxis.setGrid(255)
    yaxis.setLabel('Fraction of runs/subruns completed for this project.')
    yaxis.setRange(0.,1.)
    curve = plotwin.plot(pen='y')
    # Create an empty list in the remote process
    data = proc.transfer([])
    return data, curve

def update_gui(datapoint, xferhandle, curve):
    # Send new data to the remote process and plot it
    # We use the special argument _callSync='off' because we do
    # not want to wait for a return value.
    xferhandle.extend([datapoint], _callSync='off')
    curve.setData(y=xferhandle, _callSync='off')

if __name__ == '__main__':
    xferhandle, curve = init_gui()   
    main(xferhandle, curve)
   
