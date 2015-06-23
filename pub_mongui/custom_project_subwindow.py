import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

#These are temporary imports to demonstrate a random plot
import numpy as np

class CustomProjectSubwindow():

    def __init__(self, piechartitem):

        self.piechartitem = piechartitem
        self.pname = piechartitem.getName()
        self.pdesc = piechartitem.getDescript()

        # Open an external window
        self.win = pg.GraphicsWindow(size=(500,500))
        # Window title
        self.win.setWindowTitle(self.pname+': Additional Information')
        # Portion of window that shows project description text
        self.AddTextViewbox(self.pdesc)
        # Portion of the window that draws a random plot
        self.win.nextRow()
        #self.AddRandomPlot()
        self.AddHistoryPlot()

    def AddTextViewbox(self, intext):

        #Make a text item that is the project description
        mytext = pg.TextItem(text=intext)
        mytext.setTextWidth(450)
        #Make a viewbox in the window to hold the text
        #For some reason, if invertY is false, the text appears below
        #the bottom of the graphics window. Set it to true and text will
        #appear in the upper-left corner.
        textvb = self.win.addViewBox(invertY=True,row=0,col=0)
        #Add the text to the viewbox
        textvb.addItem(mytext)

    def AddRandomPlot(self):

        #Draw a random plot to demonstrate possibilities
        p1 = self.win.addPlot(row=1,col=0)
        p1.setLabel('top','Sample plot (TBD): Project %s'%self.pname)
        data = np.random.normal(size=300)
        curve = p1.plot(data)

    def AddHistoryPlot(self):
        p1 = self.win.addPlot(row=1,col=0)
        p1.setLabel('top','# Pending Run/Subruns for Project %s'%self.pname)
        data = np.array(self.piechartitem.getHistory())
        

    def __del__(self):
        pass
