import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

class CustomQGraphicsView(QtGui.QGraphicsView):
    """
    Custom QGraphicsScene so I can handle custom
    implementations of mouse click event inherited
    functions, etc.
    """

    def __init__(self, scene):
        QtGui.QGraphicsView.__init__(self,scene)

    def __del__(self):
        pass

    # Custom implementation of mouse scroll event in the view
    def wheelEvent(self, event):
        
        # This adjusts how much zoom happens per mouse wheel roll amount
        zoomInFactor = 1.01
        zoomOutFactor = 1./zoomInFactor

        # Save the scene pos
        oldPos = self.mapToScene(event.pos())

        # Zoom (delta is amount mouse rotated in 8ths of degree)
        zoomFactor = zoomInFactor if event.delta() > 0 else zoomOutFactor
        self.scale(zoomFactor, zoomFactor)

        # Get the new position
        newPos = self.mapToScene(event.pos())

        # Move scene to old position
        delta = newPos - oldPos
        self.translate(delta.x(), delta.y())

