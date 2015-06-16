import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

class CustomQGraphicsScene(QtGui.QGraphicsScene):
    """
    Custom QGraphicsScene so I can handle custom
    implementations of mouse click event inherited
    functions, etc.
    """

    def __init__(self, x, y, width, height):
        QtGui.QGraphicsScene.__init__(self,x,y,width,height)

    #custom implementation of mouse click event in the scene
    #it gets the item clicked, and if its a piechart, it gives
    #access to the piechart's info to the top-level script
    def mousePressEvent(self, event):
        item_clicked = self.itemAt(event.scenePos())
        if item_clicked is not None:
            if item_clicked.__module__ == 'custom_piechart_class':
                print "piechart descript = %s"%self.itemAt(event.scenePos()).getDescript()

