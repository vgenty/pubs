import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui
from custom_project_subwindow import CustomProjectSubwindow, CustomStageSubwindow
# from custom_daemon_subwindow import CustomDaemonSubwindow

class CustomQGraphicsScene(QtGui.QGraphicsScene):
    """
    Custom QGraphicsScene so I can handle custom
    implementations of mouse click event inherited
    functions, etc.
    """

    def __init__(self, x, y, width, height):
        QtGui.QGraphicsScene.__init__(self,x,y,width,height)

    def __del__(self):
        pass

    #custom implementation of mouse click event in the scene
    #it gets the item clicked, and if its a piechart, it gives
    #access to the piechart's info to the top-level script
    def mousePressEvent(self, event):
        item_clicked = self.itemAt(event.scenePos())
        #print event.scenePos()
        if item_clicked is not None:
            if item_clicked.__module__ == 'custom_piechart_class':
                self.win = CustomProjectSubwindow(item_clicked)

    def openStageSubwindow(self,stagename='',data=0):
        self.win = CustomStageSubwindow(stagename,data)
    # def mouseReleaseEvent(self, event):
    #     pass

    # def openDaemonWindow(self,daemon_warning):
    #     self.win = CustomDaemonSubwindow(daemon_warning)
