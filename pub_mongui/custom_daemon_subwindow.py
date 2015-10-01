import pyqtgraph as pg
try:
    from pyqtgraph.Qt import QtGui, QtCore
except ImportError:
    raise ImportError('Ruh roh. You need to set up pyqtgraph before you can use this GUI.')

#subwindow that opens if any of the important daemons have warnings
#(disabled, stopped running, etc...)
#to shout at shifters in red text
class CustomDaemonSubwindow():
    
    def __init__(self,textitem):

        self.textitem = textitem
        # Open an external window
        self.win = pg.GraphicsWindow(size=(600,600))
        self.win.setBackground(QtGui.QColor('red'))
        # Window title
        self.win.setWindowTitle('PUBS DAEMON WARNINGS!')
        # Portion of window that shows project description text
        self.AddTextViewbox(self.textitem)
        
    def AddTextViewbox(self, textitem):
        self.textitem = textitem
        textitem.setDefaultTextColor(QtGui.QColor('white'))
        warningfont = QtGui.QFont()
        warningfont.setPointSize(50)
        textitem.setFont(warningfont)
        #Show the text item that is the daemon problem(s)
        textitem.setTextWidth(550)
        #Make a viewbox in the window to hold the text
        #For some reason, if invertY is false, the text appears below
        #the bottom of the graphics window. Set it to true and text will
        #appear in the upper-left corner.
        textvb = self.win.addViewBox(invertY=True,row=0,col=0)
        #Add the text to the viewbox
        textvb.addItem(textitem)
        
    def UpdateText(self, textcontent):
        self.textitem.setPlainText(textcontent)

    def __del__(self):
        pass
