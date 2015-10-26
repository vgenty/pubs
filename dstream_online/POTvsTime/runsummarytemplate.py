
HTML_PRE = """<!DOCTYPE html><head>
  <!-- <meta http-equiv="refresh" content="30"> -->
</head>
<html>
  <body>
    <h1>Run Summary Page</h1>
    Brief run statistics summary page for recently taken runs.<br>
    The page is created by online PUBS and maintained by DataManagement group.
     <br><br>
    <center>
    <table style="width:100%"><tr><td>

          <table border='1' width=1300>
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
        f_html.write(table_entry)
        

    f_html.write(getRunSummaryHTMLEnd())

    f_html.close()

    return


# unit test
if __name__ == '__main__' :
    
    generateRunSummaryPage()
