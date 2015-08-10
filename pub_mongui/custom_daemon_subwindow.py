import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui
from gui_utils_api import GuiUtils

#subwindow that opens if any of the important daemons have warnings
#(disabled, stopped running, etc...)
#to shout at shifters in red text
class CustomDaemonSubwindow():
    
    def __init__(self,problem):

        self.problem = problem
        # Open an external window
        self.win = pg.GraphicsWindow(size=(500,500))
        # Window title
        self.win.setWindowTitle('PUBS DAEMON WARNINGS!')
        # Portion of window that shows project description text
        self.AddTextViewbox(self.problem)
        
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

    def __del__(self):
        pass
