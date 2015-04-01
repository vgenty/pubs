########################################
#                                      #
# README for scripts under dstream/bin #
#                                      #
########################################

This is a README for dstream/bin directory.
Here I briefly describe each script you can find here.
I trust users... so there are some wild scripts here ;)
Watch out when you use my scripts!
Below, I introduce dangerous scripts first to keep your
attention to those. Finally, though I call it "dangerous",
those scripts to clean up your DB is useful. So use it
whenever you think appropriate (but just be responsible).

#########################################################
#                                                       #
# Scripts to re-initialize a part or a whole process DB #
#                                                       #
#########################################################

- initialize_db.py

  This script initialize dstream database. 
  In particular it drops ALL project tables, ProcessTable,
  and also MainRun table. Then it recreates ProcessTable and 
  MainRun (both empty). It is meant to reset your process DB 
  after lots of playing and messing aroung!

- remove_all_projects.py

  This script removes ALL project tables and ProcessTable,
  then re-create an empty ProcessTable. Note the MainRun table
  remains untouched.

- recreate_test_mainrun.py

  Like initialize_db.py, this script drops all relevant tables
  and re-create MainRun and ProcessTable. But then, in addition,
  this script fills MainRun table with a specified # of run/subruns
  that you can specify through option flags. Use this when you
  wish to initialize + fill MainRun table with some numbers.

###########################################
#                                         #
# Scripts to interact 1 project at a time #
#                                         #
###########################################

- register_project.py

  This script allows you to register one project at a time.
  Try --help flag to see available options. Basically you can
  set project name, command, latency of execution, start run/subrun,
  email contact address. You cannot specify, unfortunatley, a resource
  at the moment (ignore if you don't know about a resource). Kazu
  is just lazy so far to implement multi-argument option flag. Remind
  him if you need it. It shouldn't take 10 minutes...

- update_project.py  

  This script allows you to alter registered project parameters.
  You cannot change the name, run, or subrun. But you can change
  everything else (again except for "resource"). You can use this
  script to disable a project from execution by a daemon instead
  of removing it.

- remove_project.py 

  This script allows you to clearnly remove a specified project
  from the process DB.  Hey, by the way, to avoid your project
  from execution, you do not have to remove your project but 
  instead just change "enabled" value. You can use update_project.py.

########################################################
#                                                      #
# Scripts to interact with multiple projects at a time #
#                                                      #
########################################################

- list_project.py

  This script lists all projects currently registered and enabled
  in the database.

- batch_register_project.py

  This is probably the most useful project in combination with
  initialize_db.py script. This script can take a formatted text
  file (see sample.txt) to define/update multiple projects at once.
  In the end, we will have several projects in dstream. During your
  development, it would be very tedious to change/add/remove one
  project at a time using scripts I introduced above. Instead you
  want to configure all projects from a single configuration file.
  This script allows you to do that. Again, look at sample.txt 
  as an example configuration file.


