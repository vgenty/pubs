import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

class PieChartItem(QtGui.QGraphicsObject):
    """
    Class written by kaleko that makes a pie chart item, which inherits from pyqtgraph.GraphicsObject
    so it can be created and plotted. It takes in the x, y coordinates of the center, and the radius.
    (perhaps the x-y coordinates are not necessary... this is something I need to investigate further.
    The idea is that a project with many runs/subruns has a larger pie chart,
    and the slices of the pie chart represent the fraction of run/subruns pending/running/errorstate.
    The pie chart object itself stores its slices, a flag of whether or not it has been drawn yet,
    and a history of number of pending run/subruns since its creation (updated each time the
    mongui updates)
    The history is a dict of {status number:[number of that status at t0, # at t1, # at t2, ...]}
    """

    def __init__(self,data):
        QtGui.QGraphicsObject.__init__(self)
        #Data is in the form of: name, xcenter, ycenter, radius, [ (slice1frac, slice1 color), (slice2frac, slice2color) ...]
        self.name = data[0]
        self.x = data[1]
        self.y = data[2]
        self.r = data[3]
        self.slices = data[4]
        self.n_history_updates = 0
        self.history = {}
        #Descripton is set separately
        self.descript = ''
        self.was_updated = False
        self.generatePicture()


    def updateData(self,data):
        self.name = data[0]
        self.x = data[1]
        self.y = data[2]
        self.r = data[3]
        self.slices = data[4]
        self.was_updated = True
        self.generatePicture()

    def generatePicture(self):
        ## pre-computing a QPicture object allows paint() 
        ## to run much more quickly, rather than
        ## re-drawing the shapes every time.
        self.picture = QtGui.QPicture()
        p = QtGui.QPainter(self.picture)
        #outline color
        p.setPen(pg.mkPen('w'))
        start_angle = int(-90*16)
        for (myfrac, mycolor) in self.slices:
            p.setBrush(pg.mkBrush(mycolor))
            p.drawPie(self.boundingRect(),start_angle,int(myfrac*360*16))
            start_angle += int(myfrac*360*16)
        p.end()    

    def paint(self, p, *args):
        p.drawPicture(0,0,self.picture)
    
    def boundingRect(self):
        ## boundingRect _must_ indicate the entire area that will be drawn on
        ## or else we will get artifacts and possibly crashing.
        # This constructor is (x_topleft, y_topleft, width, height)
        topleftx = self.x-self.r
        toplefty = self.y-self.r
        return QtCore.QRectF(topleftx,toplefty,2*self.r,2*self.r)

    def getCenterPoint(self):
        return (self.x,self.y)

    def mousePressEvent(self, event):
        pass
        
    def setDescript(self, descript):
        self.descript = descript

    def getDescript(self):
        return self.descript

    def setName(self, name):
        self.name = name
        
    def getName(self):
        return self.name

    def appendHistory(self, statuses_and_values_toappend):
        self.n_history_updates = self.n_history_updates + 1

        #If too many pending run/subruns are stored, trim the list
        if self.n_history_updates > 500:
            for mykey in self.history.keys():
                self.history[mykey].pop(0)
        #statuses_and_values_toappend looks like
        #[ (1, 1234), (3, 999), (100, 14) ]
        #i think if there is initially status=1 with 1 entry, and that entry switches to status 0,
        #then the status=1 pair gets dropped out of statuses_and_values... so, will need to pad
        #an entry in its place with value = 0
        keys_to_include = self.history.keys()
        for istat_val in statuses_and_values_toappend:
            status, value = istat_val[0], istat_val[1]
            #if this status has never been added to history, back-fill it with zeros
            if status not in self.history.keys():
                self.history[status] = [0] * (self.n_history_updates-1)
            self.history[status].append(value)

        for istat_val in statuses_and_values_toappend:
            if len(self.history[status]) != self.n_history_updates:
                self.history[status].append(0)

    def getHistory(self):
        return self.history

    def doneUpdating(self):
        self.was_updated = False

    def wasUpdated(self):
        return self.was_updated

    def getRadius(self):
        return self.r
