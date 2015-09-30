import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui
from custom_project_subwindow import CustomProjectSubwindow
from custom_daemon_subwindow import CustomDaemonSubwindow

class CustomQGraphicsScene(QtGui.QGraphicsScene):
    """
    Custom QGraphicsScene so I can handle custom
    implementations of mouse click event inherited
    functions, etc.
    """

    def __init__(self, x, y, width, height):
        QtGui.QGraphicsScene.__init__(self,x,y,width,height)
        self.projwin = None
        self.daemwin = None

    def __del__(self):
        pass

    #custom implementation of mouse click event in the scene
    #it gets the item clicked, and if its a piechart, it gives
    #access to the piechart's info to the top-level script
    def mousePressEvent(self, event):
        item_clicked = self.itemAt(event.scenePos())
        if item_clicked is not None:
            if item_clicked.__module__ == 'custom_piechart_class':
                self.projwin = CustomProjectSubwindow(item_clicked)

    def mouseReleaseEvent(self, event):
        pass

    def openDaemonWindow(self,daemon_warning):
        #This if statement says that if the daemon warning window is already open, don't recreate one; just update text
        if not self.daemwin:
            self.daemwin = CustomDaemonSubwindow(daemon_warning)
        else:
            self.daemwin.UpdateText(daemon_warning.toPlainText())