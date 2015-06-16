from pyqtgraph.Qt import QtGui, QtCore
import pyqtgraph as pg
from custom_piechart_class import PieChartItem
from custom_qgraphicsscene import CustomQGraphicsScene
import signal


#suppress warnings temporarily:
QtCore.qInstallMsgHandler(lambda *args: None)

#Always start with this
app = QtGui.QApplication([])

#These don't seem to change anything, so I'll leave them at zero
scene_xmin = 0
scene_ymin = 0
scene_width = 200
scene_height = 200

scene = CustomQGraphicsScene(scene_xmin,scene_ymin,scene_width,scene_height)

p1 = PieChartItem(('first_piechart',scene_xmin+scene_width*0.5, scene_ymin+scene_height*0.5, 50, [ (0.5, 'b'), (0.25, 'r'), (0.25, 'g') ]))
p1.setDescript('this is a test string that should be a description of the project. in fact let\'s make it really long so we can test if text wraps around or not when we draw it.')
p2 = PieChartItem(('second_piechart',scene_xmin+scene_width*0.75, scene_ymin+scene_height*0.75, 10, [ (1., 'r') ]))
p2.setDescript('this is the red pie chart!!!!!!1!!!11!!!!one')

scene.addItem(p1)
scene.addItem(p2)

view = QtGui.QGraphicsView(scene)
#Enforce the view to align with upper-left corner
view.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignTop)
view.show()

signal.signal(signal.SIGINT, signal.SIG_DFL)


if __name__ == '__main__':
    import sys
    if (sys.flags.interactive != 1) or not hasattr(QtCore, 'PYQT_VERSION'):
        QtGui.QApplication.instance().exec_()
