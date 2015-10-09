import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui
from gui_utils_api import GuiUtils

#These are temporary imports to demonstrate a random plot
import numpy as np

class CustomProjectSubwindow(QtGui.QGraphicsView):

    def __init__(self, piechartitem):
        self.scene = QtGui.QGraphicsScene()
        self.scene.setSceneRect( 0., 0., 1000., 600. )
        QtGui.QGraphicsView.__init__(self,self.scene)
        self.graphics = pg.GraphicsLayoutWidget()
        self.piechartitem = piechartitem
        self.pname = piechartitem.getName()
        self.pdesc = piechartitem.getDescript()
        self.myguiutil = GuiUtils()
        self.colors = self.myguiutil.getColors()
        self.update_period = self.myguiutil.getUpdatePeriod()
        self.history_plot = None
        self.layout = QtGui.QGridLayout()
        self.setLayout(self.layout)
        self.layout.addWidget( self.graphics, 0, 0 )

        self.pause_updating = False

        # Open an external window
        # self.win = pg.GraphicsWindow(size=(800,500))
        # Window title
        # self.win.setWindowTitle(self.pname+': Additional Information')

        # Portion of window that shows project description text
        # self.AddTextViewbox(self.pdesc)
        # Portion of the window that draws a random plot
        # self.win.nextRow()
        #self.AddRandomPlot()
        self.AddHistoryPlot()

        self.pause_button = QtGui.QPushButton()
        self.pause_button.setText("Pause Live Updating")
        self.pause_button.setGeometry(10,10,200,50)
        self.pause_button_widget = self.layout.addWidget(self.pause_button)
        self.pause_button.clicked.connect(self.pauseUpdating)

        self.timer = pg.QtCore.QTimer()
        self.timer.timeout.connect(self.AddHistoryPlot)
        self.timer.start(self.update_period*1000)

        self.show()

    def pauseUpdating(self):
        new_button_text = "Pause Live Updating" if self.pause_updating else "Resume Live Updating"
        self.pause_button.setText(new_button_text)
        self.pause_updating = not self.pause_updating

    # def AddTextViewbox(self, intext):
    #     #Make a text item that is the project description
    #     mytext = pg.TextItem(text=intext)
    #     mytext.setTextWidth(450)
    #     #Make a viewbox in the window to hold the text
    #     #For some reason, if invertY is false, the text appears below
    #     #the bottom of the graphics window. Set it to true and text will
    #     #appear in the upper-left corner.
    #     textvb = self.win.addViewBox(invertY=True,row=0,col=0)
    #     #Add the text to the viewbox
    #     textvb.addItem(mytext)

    # def AddRandomPlot(self):
    #     #Draw a random plot to demonstrate possibilities
    #     p1 = self.win.addPlot(row=1,col=0)
    #     p1.setLabel('top','Sample plot (TBD): Project %s'%self.pname)
    #     data = np.random.normal(size=300)
    #     curve = p1.plot(data)

    def AddHistoryPlot(self):
        if self.pause_updating: return

        if self.history_plot: self.graphics.removeItem(self.history_plot)
        self.history_plot = pg.PlotItem()#self.win.addPlot(row=1,col=0)
        self.history_plot.setLabel('top','TOTAL # Run/Subruns for Project %s'%self.pname)
        self.history_plot.setLabel('bottom','Time Since Starting GUI [Seconds]')
        self.history_plot.showGrid(x=True,y=True)
        history = self.piechartitem.getHistory()
        leg = pg.LegendItem()#(100,60)#,offset=(70,30)) #i can't get this fucking legend to plot in the right location
        # self.history_plot.addLegend()
        
        # colorcounter = 0
        for status, values in history.iteritems():
            #ignore good statuses
            if self.myguiutil.isGoodStatus(status): continue
            elif self.myguiutil.isErrorStatus(status): mycolor = 'r'
            elif self.myguiutil.isIntermediateStatus(status): mycolor = [255, 140, 0] #dark orange
            data = np.array(values)
            xvals = np.array(range(0,len(data)*self.update_period, self.update_period))
            #add multiple plots by just calling self.history_plot.plot() a bunch of times
            # if status in self.colors.keys(): mycolor = self.colors[status]
            # elif status > 100: mycolor = 'r'
            # else: mycolor = [255, 140, 0] #dark orange
            curve = self.history_plot.plot(xvals,data,name='Status %d (%d)'%(status,data[-1]),pen=mycolor)#(colorcounter,20))
            leg.addItem(curve,'Status %d (%d Files)'%(status,data[-1]))
            # colorcounter += 1
        leg.setParentItem(self.history_plot)
        self.graphics.addItem(self.history_plot)
        # leg.anchor(itemPos=(1,0), parentPos=(1,0), offset=(-10,10))
            

    def __del__(self):
        pass
