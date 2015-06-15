from pyqtgraph.Qt import QtGui, QtCore
import pyqtgraph as pg
from custom_piechart_class import PieChartItem
import signal
import os
from load_params import getParams

my_template = 'pubs_diagram_061515.png'

#suppress warnings temporarily:
QtCore.qInstallMsgHandler(lambda *args: None)

#Always start with this
app = QtGui.QApplication([])

#These don't seem to change anything, so I'll leave them at zero
scene_xmin = 0
scene_ymin = 0

#Load in the background image via pixmap
pm = QtGui.QPixmap(os.environ['PUB_TOP_DIR']+'/pub_mongui/gui_template/'+my_template)

#Make the scene the same size as the background template
scene_width = pm.width()
scene_height = pm.height()

#Make the scene the correct size
scene = QtGui.QGraphicsScene(scene_xmin,scene_ymin,scene_width,scene_height)
#Add the background pixmap to the scene
mypm = scene.addPixmap(pm)
#Set the background so it's upper-left corner matches upper-left corner of scene
mypm.setPos(scene_xmin,scene_ymin)


#Read in the parameters for this template and draw a pie chart in each spot
template_params = getParams(my_template)

for key, value in template_params.iteritems():
    projname = key
    xloc, yloc, maxradius = float(value[0]), float(value[1]), float(value[2])
    p1 = PieChartItem((scene_xmin+scene_width*xloc, scene_ymin+scene_height*yloc, maxradius, [ (1., 'b') ]))
    #Add the pie chart to the scene
    scene.addItem(p1)

#Make view from the scene and show it
view = QtGui.QGraphicsView(scene)
#Enforce the view to align with upper-left corner
view.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignTop)
view.show()

#Temporary zoom out for debugging
#view.fitInView(scene.sceneRect(),QtCore.Qt.KeepAspectRatio)

#Temporary zoom out for debugging
#view.scale(.5,.5)



signal.signal(signal.SIGINT, signal.SIG_DFL)

if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
