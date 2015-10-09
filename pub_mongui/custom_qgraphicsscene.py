import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui
import os
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
        self.easteregg_pm = QtGui.QPixmap(os.environ['PUB_TOP_DIR']+'/pub_mongui/gui_template/kpic.png')
        self.easteregg_item = None
        self.easteregg_drawn = False
        # self.reset_button = QtGui.QPushButton()
        # self.reset_button.setText("Reset Counters")
        # self.addWidget(self.reset_button)
        # self.reset_button.clicked.connect(self.resetCounters)

    def __del__(self):
        pass

    # def resetCounters():
    #     print "clicked button woohoo"

    #custom implementation of mouse click event in the scene
    #it gets the item clicked, and if its a piechart, it gives
    #access to the piechart's info to the top-level script
    def mouseDoubleClickEvent(self, event):

        # QtGui.QGraphicsScene.mousePressEvent(self, event)
        # QtGui.QGraphicsView.mousePressEvent(self, event)
        item_clicked = self.itemAt(event.scenePos())

        # item_clicked.mousePressEvent(event)
        if item_clicked is not None:
            if item_clicked.__module__ == 'custom_piechart_class' or \
               item_clicked.__module__ == 'custom_bar_class':
                self.projwin = CustomProjectSubwindow(item_clicked)
            # else: QtGui.QGraphicsScene.mousePressEvent(self, event) 
            #     #item_clicked.mousePressEvent(event)
        else:
            if self.easteregg_drawn: 
                self.remove_easteregg()
            else: 
                self.draw_easteregg()
            self.easteregg_drawn = not self.easteregg_drawn

    # def mouseReleaseEvent(self, event):
    #     self.remove_easteregg()

    def draw_easteregg(self):
        self.easteregg_item = self.addPixmap(self.easteregg_pm)
       
    def remove_easteregg(self):
        self.removeItem(self.easteregg_item)

    def openDaemonWindow(self,daemon_warning, force_recreate = False):
        #This if statement says that if the daemon warning window is already open, don't recreate one; just update text
        #If you've never opened a daemon window to display a warning before, make one
        #OR if you've opened a daemon window previously, but force_recreate is set to true, make one
        if not self.daemwin or force_recreate:
            self.daemwin = CustomDaemonSubwindow(daemon_warning)
        #Otherwise just update the text on the previously created daemon window (which may have been closed by user)
        else:
            self.daemwin.UpdateText(daemon_warning.toPlainText())