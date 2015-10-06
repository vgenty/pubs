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
        topleftx = self.x-self.w
        toplefty = self.y-self.w
        return QtCore.QRectF(topleftx,toplefty,2*self.w,2*self.w)

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

    def appendHistory(self, in_dict):

        ##CURRENT appendHistory should take in {'reco_2d.fcl': (10, 5, 15), 'reco_3d.fcl': (1, 29, 100)}

        #Increment counter of number of history updates
        self.n_history_updates = self.n_history_updates + 1

        #If history has never been updated before, create history dict.
        if self.n_history_updates == 1:
            for stagename, mytuple in in_dict.iteritems():
                self.history[stagename] = [ mytuple ]
            return
 
        #History (for this given project) looks like:
        #{stagename : [ (1,5,9), (2,3,9), (4,4,4) ]}
        # the list is tuples of nfiles for each substatus 1, 2, 3 


        #old comments:
        #in_dict looks like
        #[ (1, 1234), (3, 999), (100, 14) ]
        #i think if there is initially status=1 with 1 entry, and that entry switches to status 0,
        #then the status=1 pair gets dropped out of statuses_and_values... so, will need to pad
        #an entry in its place with value = 0

        #keys to include are all the statuses that have been added to history at any point in time
        stages_to_include = self.history.keys()

        #loop over statuses that you currently want to add to history
        for stagename, mytuple in in_dict.iteritems():

            #if this stage has never been added to history, back-fill it with zeros
            if stagename not in self.history.keys():
                self.history[stagename] = [0] * int(self.n_history_updates-1)

            #add this status and value to the history
            self.history[stagename].append(mytuple)

        # #there may be statuses that are in history, that are not being requested to be updated right now
        # stages_to_manually_include = list(set(self.history.keys())-set([x[0] for x in in_dict]))
        # #Keep these around, and add zeros for this update
        # for istat in stages_to_manually_include:
        #     self.history[stagename].append(0)

        #If too many pending run/subruns are stored, trim the list
        if self.n_history_updates > 50:
            for mystage in self.history.keys():
                self.history[mystage].pop(0)


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