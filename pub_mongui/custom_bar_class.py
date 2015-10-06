import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

class ProgressBarItem(QtGui.QGraphicsObject):
    """
    Class written by kaleko that makes a progress bar item, which inherits from pyqtgraph.GraphicsObject
    so it can be created and plotted. It takes in the x, y coordinates of the center, and the width.
    (perhaps the x-y coordinates are not necessary... this is something I need to investigate further.)
    The progress bar is divided into portions that are color-coded and represent the fraction of run/subruns 
    pending/running/errorstate. The progress bar object itself stores its slices, a flag of whether or not it 
    has been drawn yet, and a history of number of pending run/subruns since its creation (updated each time the
    mongui updates)
    The history is a dict of {status number:[number of that status at t0, # at t1, # at t2, ...]}
    """

    def __init__(self,data):
        QtGui.QGraphicsObject.__init__(self)
        #Data is in the form of: name, xcenter, ycenter, radius, [ (slice1frac, slice1 color), (slice2frac, slice2color) ...]
        self.name = data[0]
        self.y = data[2]
        self.w = data[3]
        self.x = data[1] - 0.5*self.w
        self.h = 40
        self.total_files = data[4]
        self.slices = data[5]

        self.n_history_updates = 0
        #History (for this given project) looks like:
        #{stagename : [ (1,5,9), (2,3,9), (4,4,4) ]}
        # the list is tuples of nfiles for each substatus 1, 2, 3 
        self.history = { }
        #Descripton is set separately
        self.descript = ''
        self.was_updated = False
        self.generatePicture()


    def updateData(self,data):
        self.name = data[0]
        self.y = data[2]
        self.w = data[3]
        self.x = data[1]
        self.total_files = data[4]
        self.slices = data[5]
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
        x_loc = self.x
        for (myfrac, mycolor) in self.slices:
            p.setBrush(pg.mkBrush(mycolor))
            p.drawRect(x_loc,self.y,myfrac*self.w,self.h)
            # p.drawPie(self.boundingRect(),start_angle,int(myfrac*360*16))
            # start_angle += int(myfrac*360*16)
            x_loc += myfrac*self.w
        p.end()    

    def paint(self, p, *args):
        p.setRenderHint(QtGui.QPainter.Antialiasing)
        p.drawPicture(0,0,self.picture)
    
    def boundingRect(self):
        ## boundingRect _must_ indicate the entire area that will be drawn on
        ## or else we will get artifacts and possibly crashing.
        # This constructor is (x_topleft, y_topleft, width, height)
        topleftx = self.x
        toplefty = self.y
        return QtCore.QRectF(topleftx,toplefty,self.w,self.h)

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

        ##CURRENT appendHistory should take in {'reco_2d.fcl': (10, 5, 15), 'reco_3d.fcl': (1, 29, 100)}

        #Increment counter of number of history updates
        self.n_history_updates = self.n_history_updates + 1

        #If history has never been updated before, create history dict.
        if self.n_history_updates == 1:
            for istat_val in statuses_and_values_toappend:
                status, value = istat_val[0], istat_val[1]
                self.history[status] = [value]
            return
 
        #keys to include are all the statuses that have been added to history at any point in time
        keys_to_include = self.history.keys()

        #loop over statuses that you currently want to add to history
        for istat_val in statuses_and_values_toappend:
            status, value = istat_val[0], istat_val[1]

            #if this status has never been added to history, back-fill it with zeros
            if status not in self.history.keys():
                self.history[status] = [0] * int(self.n_history_updates-1)

            #add this status and value to the history
            self.history[status].append(value)

        #there may be statuses that are in history, that are not being requested to be updated right now
        statuses_to_manually_include = list(set(self.history.keys())-set([x[0] for x in statuses_and_values_toappend]))
        #Keep these around, and add zeros for this update
        for istat in statuses_to_manually_include:
            self.history[istat].append(0)

        #If too many pending run/subruns are stored, trim the list
        if self.n_history_updates > 500:
            for mykey in self.history.keys():
                self.history[mykey].pop(0)

    def getHistory(self):
        return self.history

    def doneUpdating(self):
        self.was_updated = False

    def wasUpdated(self):
        return self.was_updated

    def getRadius(self):
        return self.w

    def getTotalFiles(self):
        return self.total_files

    def getSlices(self):
        return self.slices

    def getHeight(self):
        return self.h
