# dstream import
from dstream.ds_api import ds_reader
# pub_dbi import
from pub_dbi import pubdb_conn_info


class GuiUtilsAPI():
  ''' Class that handles the API instance to the database (since more than
  just the main GUI window will use it, as well as a few utility functions
  that are independent of the database. '''

    def __init__():

        # DB interface:
        self.dbi = ds_reader(pubdb_conn_info.reader_info())

        #Establish a connection to the database through the DBI
        try:
            self.dbi.connect()
        except:
            print "Unable to connect to database... womp womp :("

    def getAllProjects(self):
        return self.dbi.list_all_projects()

    def computePieSlices(self,projname):

        #Status codes: 0 is completed (don't care about these) 1 is initiated, 2 is to be validated, >2 is error?
        n_status1 = len(self.dbi.get_runs(projname,status=1))
        n_status2 = len(self.dbi.get_runs(projname,status=2))
        tot_n = n_status1 + n_status2

        #If no runs have status 1 or 2, set pie chart to all red
        # (though it will probably be drawn with 0 radius)
        slices = [ (1., 'r') ]
        if tot_n:
            #Compute the fraction of the total runs that are complete
            frac_1 = float(n_status1)/float(tot_n)
            frac_2 = float(n_status2)/float(tot_n)
            slices = [ (frac_1, 'b'), (frac_2, 'g') ]

        return slices

    def computePieChartRadius(self, proj_name, n_tot, max_radius):
    
        #Right now use radius = (Rmax/2log(5))log(n_total_runsubruns)
        #unless radius > Rmax, in which case use radius = Rmax
        #This function has r = 0.5*Rmax when n = 5
        radius = (float(max_radius)/(2 * math.log(5.) )) * math.log(float(n_tot))
        #Double check the radius isn't bigger than the max allowed
        return radius if radius <= max_radius else max_radius
