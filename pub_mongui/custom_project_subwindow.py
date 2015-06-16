import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

#These are temporary imports to demonstrate a random plot
import numpy as np

class CustomProjectSubwindow():

    def __init__(self, project_name, project_description=''):
        
        #Open an external window
        self.win = pg.GraphicsWindow(size=(500,500))
        #Window title
        self.win.setWindowTitle(project_name+': Additional Information')
        #Make a text item that is the project description
        self.mytext = pg.TextItem(text=project_description)
        self.mytext.setTextWidth(500)
        #Make a viewbox in the window to hold the text
        #For some reason, if invertY is false, the text appears below
        #the bottom of the graphics window. Set it to true and text will
        #appear in the upper-left corner.
        self.textvb = self.win.addViewBox(invertY=True,row=0,col=0)
        #Add the text to the viewbox
        self.textvb.addItem(self.mytext)
        self.win.nextRow()
        #Draw a random plot to demonstrate possibilities
        self.p1 = self.win.addPlot(row=1,col=0)
        self.p1.setLabel('top','Sample plot (TBD): Project %s'%project_name)
        self.data1 = np.random.normal(size=300)
        self.curve1 = self.p1.plot(self.data1)

    def __del__(self):
        pass
