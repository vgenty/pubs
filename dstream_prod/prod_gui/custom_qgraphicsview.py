import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

class CustomQGraphicsView(QtGui.QGraphicsView):
    """
    Custom QGraphicsScene so I can handle custom
    implementations of mouse click event inherited
    functions, etc.
    """

    def __init__(self, scene, background_pixmap=''):
        QtGui.QGraphicsView.__init__(self,scene)
        self.background_pixmap = background_pixmap
        self._ignore_resize = False
        self.resize(self.background_pixmap.width(),self.background_pixmap.height())

    def __del__(self):
        pass

    # Custom implementation of mouse scroll event in the view
    def wheelEvent(self, event):
    
        # This adjusts how much zoom happens per mouse wheel roll amount
        zoomInFactor = 1.15
        zoomOutFactor = 1./zoomInFactor

        # Once the user zooms in, the always-fix-image-to-window-size functionality is disabled.
        self._ignore_resize = True

        # Save the scene pos
        oldPos = self.mapToScene(event.pos())

        # Zoom (delta is amount mouse rotated in 8ths of degree)
        zoomFactor = zoomInFactor if event.delta() > 0 else zoomOutFactor
        self.scale(zoomFactor, zoomFactor)

        # Get the new position
        newPos = self.mapToScene(event.pos())

        # Move scene to from new to old position
        delta = newPos - oldPos
        self.translate(delta.x(), delta.y())
        

    def drawBackground(self, painter, rect):
        #This gets called every time the window is resized/zoomed/etc
        #Make the scene the same size as the background template
        scene_xmin, scene_ymin, scene_width, scene_height = 0, 0, self.background_pixmap.width(), self.background_pixmap.height()
        painter.drawPixmap(QtCore.QRect(scene_xmin, scene_ymin, scene_width, scene_height),self.background_pixmap)

    def resizeEvent(self, event):
        if not self._ignore_resize:
            self.fitInView(self.sceneRect(),QtCore.Qt.KeepAspectRatio)
