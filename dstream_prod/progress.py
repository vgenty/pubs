#! /usr/bin/env python

import sys, os, datetime
os.environ['PUB_LOGGER_LEVEL'] = 'kLOGGER_ERROR'
import subprocess

from dstream.ds_api import ds_reader
from pub_dbi import pubdb_conn_info

bad_runs = [5113, 5220, 5244, 5246, 5248, 5250, 5255, 5257, 5259, 5260,
            5267, 5268, 5269,
            5348, 5349, 5350, 5351, 5352,
            5677, 5678,
            5785, 5786, 5787, 5788, 5789, 5790, 5791, 5792, 5793, 5794,
            5795, 5796, 5797, 5798, 5799, 5800, 5801, 5802, 5803, 5804,
            5805, 5806, 5807, 5808, 5809, 5810, 5811, 5812, 5813, 5814,
            5815, 5816, 5817, 5818, 6512, 6515, 6516, 6741, 6742, 6743, 6745, 6746]

# DB connection.

dbi = ds_reader(pubdb_conn_info.reader_info())
try:
    dbi.connect()
    print "Connection successful."
except:
    print "Connection failed."
    sys.exit(1)

# Generate html

html = open('progress.html', 'w')

# Write html header.

html.write(
'''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
<head><title>Run 1 Data Production</title>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1" />
<link rel="StyleSheet" href="/css/ubcss.css" type="text/css"  media="screen, projection" />
<link rel="StyleSheet" href="/css/ubprint.css" type="text/css"  media="print" />
</head>
<div id="main">
<body>
<h1>Data Production Progress</h1>
''')

# Generate update time.

update_time = datetime.datetime.now().strftime('%d-%b-%Y %H:%M')
html.write('<p>Updated %s</p>\n' % update_time)

# Generate daemon status.

daemons = ('uboonegpvm01.fnal.gov',
           'uboonegpvm02.fnal.gov',
           'uboonegpvm03.fnal.gov',
           'uboonegpvm04.fnal.gov',
           'uboonegpvm05.fnal.gov',
           'uboonegpvm06.fnal.gov',
           'uboonegpvm07.fnal.gov')


html.write('<table style="width:300px; display:inline-block">')
html.write('<caption><strong>Daemons</strong></caption>')

# Loop over daemons.

for daemon in daemons:
    color = 'black'
    status = 'Unknown'
    command = ['ssh', daemon, 'ps', 'aux']
    try:
        lines = subprocess.check_output(command).splitlines()
        color = 'red'
        status = 'Stopped'
        for line in lines:
            if line.find('python') >= 0 and line.find('daemon.py') >= 0:
                status = 'Running'
                color = 'green'
    except:
        color = 'black'
        status = 'Unknown'

    html.write('<tr bgcolor=#ffffe0>\n')
    html.write('<td>%s</td>\n' % daemon)
    html.write('<td><font color="%s">%s</font></td>\n' % (color, status))
    html.write('</tr>\n')
html.write('</table>\n')

# Generate legend.

html.write(
'''<table style="width:300px; display:inline-block">
<caption><strong>Legend</strong></caption>
<tr bgcolor=#ffffe0>
<td>Complete</td>
<td nowrap align=left><img src=http://www-microboone.fnal.gov/images/bar-green2.gif height=13 width=150></td>
</tr>
<tr bgcolor=#ffffe0>
<td>Merged</td>
<td nowrap align=left><img src=http://www-microboone.fnal.gov/images/bar-green.gif height=13 width=150></td>
</tr>
<tr bgcolor=#ffffe0>
<td>Processing</td>
<td nowrap align=left><img src=http://www-microboone.fnal.gov/images/bar-yellow.gif height=13 width=50><img src=http://www-microboone.fnal.gov/images/bar-orange.gif height=13 width=50><img src=http://www-microboone.fnal.gov/images/bar-purple.gif height=13 width=50></td>
</tr>
<tr bgcolor=#ffffe0>
<td>Waiting</td>
<td nowrap align=left><img src=http://www-microboone.fnal.gov/images/bar-blue.gif height=13 width=150></td>
</tr>
<tr bgcolor=#ffffe0>
<td>Error</td>
<td nowrap align=left><img src=http://www-microboone.fnal.gov/images/bar-red.gif height=13 width=150></td>
</tr>
</table>
''')

# Generate progress for swizzling.

project = 'prod_swizzle_filter_v3'
prjdict = dbi.list_xstatus(bad_runs, project)
n1 = 0
n2 = 0
n3 = 0
n4 = 0
for status, num in prjdict[project]:
    if status == 10:
        n1 += num
    elif status >=2 and status <= 6:
        n2 += num
    elif status == 1:
        n3 += num
    elif status >= 1000:
        n4 += num
ntot = n1 + n2 + n3 + n4
s1 = int(500. * float(n1) / float(ntot))
s2 = int(500. * float(n2) / float(ntot))
s3 = int(500. * float(n3) / float(ntot))
s4 = int(500. * float(n4) / float(ntot))
stot = s1 + s2 + s3 + s4

html.write(
'''<table border bgcolor=#ffffe0>
<caption><strong>Software Trigger Swizzling</strong></caption>
<tr bgcolor=#ffffe0>
''')
html.write('<td nowrap>Swizzling</td>\n')
html.write('<td nowrap align=left>')
if s1 != 0:
    html.write('<img src=http://www-microboone.fnal.gov/images/bar-green2.gif height=13 width=%d>' % s1)
if s2 != 0:
    html.write('<img src=http://www-microboone.fnal.gov/images/bar-yellow.gif height=13 width=%d>' % s2)
if s3 != 0:
    html.write('<img src=http://www-microboone.fnal.gov/images/bar-blue.gif height=13 width=%d>' % s3)
if s4 != 0:
    html.write('<img src=http://www-microboone.fnal.gov/images/bar-red.gif height=13 width=%d>' % s4)
html.write('</td>\n')
html.write('<td nowrap>%d / %d (%6.2f%% complete, %6.2f%% error)</td>' %(n1, ntot, 
                                                                         100.*float(n1)/float(ntot),
                                                                         100.*float(n4)/float(ntot)))
html.write(
'''</td>
</tr>
''')

# Merging

streams = [('prod_swizzle_merge_bnb_v3', 'Merge BNB'),
           ('prod_swizzle_merge_ext_bnb_v3', 'Merge BNB External'),
           ('prod_swizzle_merge_bnb_unbiased_v3', 'Merge BNB Unbiased'),
           ('prod_swizzle_merge_numi_v3', 'Merge NUMI'),
           ('prod_swizzle_merge_ext_numi_v3', 'Merge NUMI External'),
           ('prod_swizzle_merge_numi_unbiased_v3', 'Merge NUMI Unbiased'),
           ('prod_swizzle_merge_ext_unbiased_v3', 'Merge External Unbiased'),
           ('prod_swizzle_merge_mucs_v3', 'Merge MuCS'),
           ('prod_swizzle_merge_notpc_v3', 'Merge NoTPC'),
           ('prod_notrig_swizzle_v3', 'Calibration')]


for stream in streams:
    project = stream[0]
    name = stream[1]
    prjdict = dbi.list_xstatus(bad_runs, project)
    n1 = 0
    n2 = 0
    n3 = 0
    n4 = 0
    n5 = 0
    for status, num in prjdict[project]:
        if status == 10:
            n1 += num
        if status == 100:
            n2 += num
        elif status >=2 and status <= 9:
            n3 += num
        elif status == 1:
            n4 += num
        elif status >= 1000:
            n5 += num
    ntot = n1 + n2 + n3 + n4 + n5
    s1 = int(500. * float(n1) / float(ntot))
    s2 = int(500. * float(n2) / float(ntot))
    s3 = int(500. * float(n3) / float(ntot))
    s4 = int(500. * float(n4) / float(ntot))
    s5 = int(500. * float(n5) / float(ntot))
    stot = s1 + s2 + s3 + s4 + s5

    html.write('<tr bgcolor=#ffffe0>\n')
    html.write('<td nowrap>%s</td>\n' % name)
    html.write('<td nowrap align=left>')
    if s1 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-green2.gif height=13 width=%d>' % s1)
    if s2 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-green.gif height=13 width=%d>' % s2)
    if s3 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-yellow.gif height=13 width=%d>' % s3)
    if s4 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-blue.gif height=13 width=%d>' % s4)
    if s5 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-red.gif height=13 width=%d>' % s5)
    html.write('</td>\n')
    html.write('<td nowrap>%d / %d / %d (%6.2f%% complete, %6.2f%% error)</td>' %(n1, n1 + n2, ntot, 
                                                                                  100.*float(n1+n2)/float(ntot),
                                                                                  100.*float(n5)/float(ntot)))
    html.write('</tr>\n')

html.write('</table>\n')


# Generate progress for reconstruction.

html.write(
'''<table border bgcolor=#ffffe0>
<caption><strong>Reconstruction</strong></caption>
''')

streams = [(['prod_reco_bnb_v5', 'prod_reco_bnb_v5_quietwires'], 'BNB'),
           (['prod_reco_ext_bnb_v5', 'prod_reco_ext_bnb_v5_quietwires'], 'BNB External'),
           (['prod_reco_bnb_unbiased_v5', 'prod_reco_bnb_unbiased_v5_quietwires'], 'BNB Unbiased'),
           (['prod_reco_numi_v5'], 'NUMI'),
           (['prod_reco_ext_numi_v5'], 'NUMI External'),
           (['prod_reco_numi_unbiased_v5'], 'NUMI Unbiased'),
           (['prod_reco_ext_unbiased_v5', 'prod_reco_ext_unbiased_v5_quietwires'], 'External Unbiased'),
           (['prod_reco_mucs_v5', 'prod_reco_mucs_v5_quietwires'], 'MuCS')]

for stream in streams:
    projects = stream[0]
    name = stream[1]
    n1 = 0
    n2 = 0
    n3 = 0
    n4 = 0
    n5 = 0
    n6 = 0
    for project in projects:
        prjdict = dbi.list_xstatus(bad_runs, project)
        #if not prjdict.has_key(project):
        #    continue
        for status, num in prjdict[project]:
            if status == 20:
                n1 += num
            elif status >=2 and status <= 9:
                n2 += num
            elif status >=10 and status <= 11:
                n3 += num
            elif status >=12 and status <= 19:
                n4 += num
            elif status == 1:
                n5 += num
            elif status >= 1000:
                n6 += num
    ntot = n1 + n2 + n3 + n4 + n5 + n6
    s1 = int(500. * float(n1) / float(ntot))
    s2 = int(500. * float(n2) / float(ntot))
    s3 = int(500. * float(n3) / float(ntot))
    s4 = int(500. * float(n4) / float(ntot))
    s5 = int(500. * float(n5) / float(ntot))
    s6 = int(500. * float(n6) / float(ntot))
    stot = s1 + s2 + s3 + s4 + s5 + s6

    html.write('<tr bgcolor=#ffffe0>\n')
    html.write('<td nowrap>%s</td>\n' % name)
    html.write('<td nowrap align=left>')
    if s1 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-green2.gif height=13 width=%d>' % s1)
    if s2 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-yellow.gif height=13 width=%d>' % s2)
    if s3 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-orange.gif height=13 width=%d>' % s3)
    if s4 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-purple.gif height=13 width=%d>' % s4)
    if s5 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-blue.gif height=13 width=%d>' % s5)
    if s6 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-red.gif height=13 width=%d>' % s6)
    html.write('</td>\n')
    html.write('<td nowrap>%d / %d (%6.2f%% complete, %6.2f%% error)</td>' %(n1, ntot,
                                                                             100.*float(n1)/float(ntot),
                                                                             100.*float(n6)/float(ntot)))
    html.write('</tr>\n')

html.write('</table>\n')


# Generate progress for analysis tree.

html.write(
'''<table border bgcolor=#ffffe0>
<caption><strong>Analysis Tree</strong></caption>
''')

streams = [(['prod_anatree_bnb_v5', 'prod_anatree_bnb_v5_quietwires'], 'BNB'),
           (['prod_anatree_ext_bnb_v5', 'prod_anatree_ext_bnb_v5_quietwires'], 'BNB External'),
           (['prod_anatree_bnb_unbiased_v5', 'prod_anatree_bnb_unbiased_v5_quietwires'], 'BNB Unbiased'),
           (['prod_anatree_numi_v5', 'prod_anatree_numi_v5_quietwires'], 'NUMI'),
           (['prod_anatree_ext_numi_v5', 'prod_anatree_ext_numi_v5_quietwires'], 'NUMI External'),
           (['prod_anatree_numi_unbiased_v5', 'prod_anatree_numi_unbiased_v5_quietwires'], 'NUMI Unbiased'),
           (['prod_anatree_ext_unbiased_v5', 'prod_anatree_ext_unbiased_v5_quietwires'], 'External Unbiased'),
           (['prod_anatree_mucs_v5', 'prod_anatree_mucs_v5_quietwires'], 'MuCS')]

for stream in streams:
    projects = stream[0]
    name = stream[1]
    n1 = 0
    n2 = 0
    n3 = 0
    n4 = 0
    for project in projects:
        prjdict = dbi.list_xstatus(bad_runs, project)
        #if not prjdict.has_key(project):
        #    continue
        for status, num in prjdict[project]:
            if status == 10:
                n1 += num
            elif status >=2 and status <= 9:
                n2 += num
            elif status == 1:
                n3 += num
            elif status >= 1000:
                n4 += num
    ntot = n1 + n2 + n3 + n4
    s1 = int(500. * float(n1) / float(max(1, ntot)))
    s2 = int(500. * float(n2) / float(max(1, ntot)))
    s3 = int(500. * float(n3) / float(max(1, ntot)))
    s4 = int(500. * float(n4) / float(max(1, ntot)))
    stot = s1 + s2 + s3 + s4

    html.write('<tr bgcolor=#ffffe0>\n')
    html.write('<td nowrap>%s</td>\n' % name)
    html.write('<td nowrap align=left>')
    if s1 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-green2.gif height=13 width=%d>' % s1)
    if s2 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-yellow.gif height=13 width=%d>' % s2)
    if s3 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-blue.gif height=13 width=%d>' % s3)
    if s4 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-red.gif height=13 width=%d>' % s4)
    html.write('</td>\n')
    html.write('<td nowrap>%d / %d (%6.2f%% complete, %6.2f%% error)</td>' %(n1, ntot,
                                                                             100.*float(n1)/float(max(1, ntot)),
                                                                             100.*float(n4)/float(max(1, ntot))))
    html.write('</tr>\n')

html.write('</table>\n')


# Generate progress for beam filter.

html.write(
'''<table border bgcolor=#ffffe0>
<caption><strong>Beam Filter</strong></caption>
''')

streams = [(['prod_beamfilter_bnb_v5'], 'BNB')]

for stream in streams:
    projects = stream[0]
    name = stream[1]
    n1 = 0
    n2 = 0
    n3 = 0
    n4 = 0
    n5 = 0
    n6 = 0
    for project in projects:
        prjdict = dbi.list_xstatus(bad_runs, project)
        #if not prjdict.has_key(project):
        #    continue
        for status, num in prjdict[project]:
            if status == 10:
                n1 += num
            elif status >=2 and status <= 4:
                n2 += num
            elif status >=5 and status <= 6:
                n3 += num
            elif status >=7 and status <= 9:
                n4 += num
            elif status == 1:
                n5 += num
            elif status >= 1000:
                n6 += num
    ntot = n1 + n2 + n3 + n4 + n5 + n6
    s1 = int(500. * float(n1) / float(ntot))
    s2 = int(500. * float(n2) / float(ntot))
    s3 = int(500. * float(n3) / float(ntot))
    s4 = int(500. * float(n4) / float(ntot))
    s5 = int(500. * float(n5) / float(ntot))
    s6 = int(500. * float(n6) / float(ntot))
    stot = s1 + s2 + s3 + s4 + s5 + s6

    html.write('<tr bgcolor=#ffffe0>\n')
    html.write('<td nowrap>%s</td>\n' % name)
    html.write('<td nowrap align=left>')
    if s1 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-green2.gif height=13 width=%d>' % s1)
    if s2 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-yellow.gif height=13 width=%d>' % s2)
    if s3 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-orange.gif height=13 width=%d>' % s3)
    if s4 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-purple.gif height=13 width=%d>' % s4)
    if s5 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-blue.gif height=13 width=%d>' % s5)
    if s6 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-red.gif height=13 width=%d>' % s6)
    html.write('</td>\n')
    html.write('<td nowrap>%d / %d (%6.2f%% complete, %6.2f%% error)</td>' %(n1, ntot,
                                                                             100.*float(n1)/float(ntot),
                                                                             100.*float(n6)/float(ntot)))
    html.write('</tr>\n')

html.write('</table>\n')


# Generate progress for beam filter analysis tree.

html.write(
'''<table border bgcolor=#ffffe0>
<caption><strong>Beam Filter Analysis Tree</strong></caption>
''')

streams = [(['prod_anatree_beamfilter_bnb_v5'], 'BNB')]

for stream in streams:
    projects = stream[0]
    name = stream[1]
    n1 = 0
    n2 = 0
    n3 = 0
    n4 = 0
    n5 = 0
    n6 = 0
    for project in projects:
        prjdict = dbi.list_xstatus(bad_runs, project)
        #if not prjdict.has_key(project):
        #    continue
        for status, num in prjdict[project]:
            if status == 10:
                n1 += num
            elif status >=2 and status <= 4:
                n2 += num
            elif status >=5 and status <= 6:
                n3 += num
            elif status >=7 and status <= 9:
                n4 += num
            elif status == 1:
                n5 += num
            elif status >= 1000:
                n6 += num
    ntot = n1 + n2 + n3 + n4 + n5 + n6
    s1 = int(500. * float(n1) / float(ntot))
    s2 = int(500. * float(n2) / float(ntot))
    s3 = int(500. * float(n3) / float(ntot))
    s4 = int(500. * float(n4) / float(ntot))
    s5 = int(500. * float(n5) / float(ntot))
    s6 = int(500. * float(n6) / float(ntot))
    stot = s1 + s2 + s3 + s4 + s5 + s6

    html.write('<tr bgcolor=#ffffe0>\n')
    html.write('<td nowrap>%s</td>\n' % name)
    html.write('<td nowrap align=left>')
    if s1 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-green2.gif height=13 width=%d>' % s1)
    if s2 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-yellow.gif height=13 width=%d>' % s2)
    if s3 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-orange.gif height=13 width=%d>' % s3)
    if s4 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-purple.gif height=13 width=%d>' % s4)
    if s5 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-blue.gif height=13 width=%d>' % s5)
    if s6 != 0:
        html.write('<img src=http://www-microboone.fnal.gov/images/bar-red.gif height=13 width=%d>' % s6)
    html.write('</td>\n')
    html.write('<td nowrap>%d / %d (%6.2f%% complete, %6.2f%% error)</td>' %(n1, ntot,
                                                                             100.*float(n1)/float(ntot),
                                                                             100.*float(n6)/float(ntot)))
    html.write('</tr>\n')

html.write('</table>\n')


# Write html trailer.

html.write(
'''</div>
</body>
</html>''')


sys.exit(0)
