
HTML_PRE = """<!DOCTYPE html><head>
  <!-- <meta http-equiv="refresh" content="30"> -->
</head>
<html>
  <body>
    <h1>Run Summary Page</h1>
    Brief run statistics summary page for recently taken runs.<br>
    The page is created by online PUBS and maintained by DataManagement group.
    <h2>Run Statistics Summary</h2>
    Table below shows the list of runs that is taken during your shift period.<br>
    DAQ up-time during the current shift is shown in the next table (right).<br>
    <br><br>
    <center>
    <table style="width:100%"><tr><td>

          <table border='0' width=1400>
            <tr>
              <th> Run Number    </th>
              <th> Start Time    </th>
              <th> End Time      </th>
              <th> Duration [hrs]</th>
              <th> SubRun Counts </th>
              <th> Events        </th>
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

def generateRunSummaryPage(html = 'RunSummary.html'):
    
    f_html = open(html,'w+')

    # add the html page beginning 
    f_html.write(getRunSummaryHTMLBegin())

    # now open the RunSummary.csv link and fill in additional info
    stats = open('RunSummary.csv','r')
    for line in stats:
        words = line.split()
        table_entry = """<tr>
<td align="left"> %s </td>
<td align="center"> %s </td>
<td align="center"> %s </td>
<td align="left"> %s </td>
<td align="left"> %s </td>
<td align="left"> %s </td>
<td align="left"> %s </td>
<td align="left"> %s </td>
<td align="left"> %s </td>
</tr>"""%(words[0],'%s %s'%(words[1],words[2]),'%s %s'%(words[3],words[4]),words[5],words[6],words[7],words[8],words[9],words[10])
        f_html.write(table_entry)
        

    f_html.write(getRunSummaryHTMLEnd())

    f_html.close()

    return


# unit test
if __name__ == '__main__' :
    
    generateRunSummaryPage()
