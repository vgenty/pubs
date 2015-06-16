import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

class PieChartItem(QtGui.QGraphicsObject):
    """
    Class written by kaleko that makes a pie chart item, which inherits from pyqtgraph.GraphicsObject
    so it can be created and plotted. It takes in the x, y coordinates of the center, and the radius.
    (perhaps the x-y coordinates are not necessary... this is something I need to investigate further.
    The idea is that perhaps a project with many runs/subruns would have a larger pie chart,
    and the slices of the pie chart represent the fraction of run/subruns pending/running/errorstate.
    """

    def __init__(self,data):
        QtGui.QGraphicsObject.__init__(self)
        #Data is in the form of: xcenter, ycenter, radius, [ (slice1frac, slice1 color), (slice2frac, slice2color) ...]
        self.data = data
        self.descript = ''
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
        for (myfrac, mycolor) in self.data[3]:
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
        radius = self.data[2]
        topleftx = self.data[0]-radius
        toplefty = self.data[1]-radius
        return QtCore.QRectF(topleftx,toplefty,2*radius,2*radius)

    def getCenterPoint(self):
        return (self.data[0],self.data[1])

    def mousePressEvent(self, event):
        pass
        
    def setDescript(self, descript):
        self.descript = descript

    def getDescript(self):
        return self.descript
