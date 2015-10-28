
HTML_PRE = """<!DOCTYPE html><head>
  <!-- <meta http-equiv="refresh" content="30"> -->
</head>
<html>
  <body>
    <h1>Run Summary Page</h1>
    Brief run statistics summary page for recently taken runs.<br>
    The page is created by online PUBS and maintained by DataManagement group.
     <br><br>
    <center>"""

TABLE_BEGIN = """<table style="width:100%"><tr><td>

          <table border='1' width=1200>
            <tr>
              <th>    Run Number </th>
              <th>    Start Time </th>
              <th>      End Time </th>
              <th> Duration [hrs]</th>
              <th>       SubRuns </th>
              <th>        Events </th>
              <th> Tot ppp [E12] </th>
              <th> ppp/event [E12]</th>
              <th> Event Rate [Hz]</th>"""

HTML_POST = """</table> </td>
</center>
</body>"""

def getRunSummaryHTMLBegin():
    return HTML_PRE

def getRunSummaryHTMLEnd():
    return HTML_POST

def generateRunSummaryPage(csv='RunSummary.csv',html='RunSummary.html'):
    
    f_html = open(html,'w+')

    # add the html page beginning 
    f_html.write(getRunSummaryHTMLBegin())
    
    # a list where to keep all the table rows to be added
    all_rows = []

    # keep track of the total time, and total POT, and tota # events
    tot_time = 0.
    tot_ppp  = 0
    tot_evts = 0

    # now open the RunSummary.csv link and fill in additional info
    stats = open(csv,'r')
    for line in stats:
        words = line.split()
        table_entry = """<tr>
<td align="right"> %s </td>
<td align="center"> %s </td>
<td align="center"> %s </td>
<td align="right"> %s </td>
<td align="right"> %s </td>
<td align="right"> %s </td>
<td align="right"> %s </td>
<td align="right"> %s </td>
<td align="right"> %s </td>
</tr>"""%(words[0],'%s %s'%(words[1],words[2]),'%s %s'%(words[3],words[4]),words[5],words[6],words[7],words[8],words[9],words[10])
        all_rows.append(table_entry)

        tot_time += float(words[5])/24.
        tot_ppp  += float(words[8])/1000000.
        tot_evts += float(words[7])/1000000.

    # write summary info for all runs:
    f_html.write('<br>Total Events Recorded : %.02f E6<br>Total Run Time : %.02f days<br>Total ppp : %.02f [E18]'%(tot_evts,tot_time,tot_ppp))
    # write the table gin
    f_html.write(TABLE_BEGIN)
    # write all table rows
    for row in all_rows:
        f_html.write(row)
    # write the html page end
    f_html.write(getRunSummaryHTMLEnd())

    f_html.close()

    return


# unit test
if __name__ == '__main__' :
    
    generateRunSummaryPage()
