import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

#These are temporary imports to demonstrate a random plot
import numpy as np

class CustomProjectSubwindow():

    def __init__(self, piechartitem):

        self.piechartitem = piechartitem
        self.pname = piechartitem.getName()
        self.colors = ('r','b','g','o','m','y')
        self.update_period = 1
        self.history_plot = None
        # Open an external window
        self.win = pg.GraphicsWindow(size=(800,500))
        # Window title
        self.win.setWindowTitle(self.pname+': Additional Information')
        # self.AddRandomPlot()
        # check1 = QtGui.QCheckBox(self.win)
        # check1.setText("checkbox 1")
        # self.win.addItem(check1)
        # blah.addItem(check1)
        #blah = self.win.addViewBox(invertY=True,row=0,col=0)
        #blah.addItem(self.piechartitem)
        #self.AddHistoryPlot()

        self.timer = pg.QtCore.QTimer()
        self.timer.timeout.connect(self.AddHistoryPlot)
        self.timer.start(1*1000)
        
    def AddTextViewbox(self, intext):
        #Make a text item that is the project description
        mytext = pg.TextItem(text=intext)
        mytext.setTextWidth(450)
        #Make a viewbox in the window to hold the text
        #For some reason, if invertY is false, the text appears below
        #the bottom of the graphics window. Set it to true and text will
        #appear in the upper-left corner.
        textvb = self.win.addViewBox(invertY=True,row=0,col=0)
        #Add the text to the viewbox
        textvb.addItem(mytext)

    def AddRandomPlot(self):
        #Draw a random plot to demonstrate possibilities
        p1 = self.win.addPlot(row=1,col=0)
        p1.setLabel('top','Sample plot (TBD): Project %s'%self.pname)
        data = np.random.normal(size=300)
        curve = p1.plot(data)

    def AddHistoryPlot(self):
      
        if self.history_plot: self.win.removeItem(self.history_plot)

        #History (for this given project) looks like:
        #{stagename : [ (1,5,9), (2,3,9), (4,4,4) ]}
        # the list is tuples of nfiles for each substatus 1, 2, 3 

        self.history_plot = self.win.addPlot(row=1,col=0)
        self.history_plot.setLabel('top','# Run/Subruns for Project %s, for each stage'%self.pname)
        self.history_plot.setLabel('bottom','Time Since Starting GUI [Seconds]')
        self.history_plot.showGrid(x=True,y=True)
        history = self.piechartitem.getHistory()
        leg = pg.LegendItem()#(100,60),offset=(70,30)) #i can't get this fucking legend to plot in the right location
        colorcounter = 0
        for stagename, mytuples in history.iteritems():
            #ignore stagename 0 and 1000 (0 shouldn't be in here anyway)
            if stagename in [ 0, 1000 ]: continue
            data = np.array([sum(mytuple) for mytuple in mytuples])
            xvals = np.array(range(0,len(data)*self.update_period, self.update_period))
            #add multiple plots by just calling self.history_plot.plot() a bunch of times
            #mycolor = self.colors[stagename] if stagename in self.colors.keys() else 'r'
            curve = self.history_plot.plot(xvals,data,name='stagename %s'%stagename,pen=self.colors[colorcounter])#(colorcounter,20))#,pen=mycolor)
            leg.addItem(curve,'stagename %s'%stagename)
            colorcounter += 1
        leg.setParentItem(self.history_plot)
            

    def __del__(self):
        pass
