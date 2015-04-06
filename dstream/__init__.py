## @defgroup dstream dstream
#  @brief PUBS module dstream is for managing the MicroBooNE online data process stream
#  @details
#  dstream uses database for process management.\n
#  Each process is called as a "project" and defined by an execution of a script.\n
#  Typically each process is executed per (run,sub-run) combination although it is\n
#  possible to sub-divide into one more layer of granularity if needed.\n
#  Each project has a dedicated database table, and its status is logged in the table\n
#  for a each (run,sub-run) combination.\n
#  The execution of projects are done through a daemon process defined in ds_daemon\n
#  sub module. ds_daemon simply query the database for registered projects for an\n
#  execution, and capable of executing multiple projects in parallel indefinitely.\n
#  For the details, see docdb documentation (in preparation...).\n
#  dstream uses some basic class/function defined in pub_dbi and pub_util modules.\n
#  The database interface is defined in ds_api sub module which, in turn, depends on\n
#  more basic database interface defined in pub_dbi module. In addition, (pretty much)\n
#  all dstream classes uses a simple logger defined in pub_util module.

## @addtogroup dstream
#  @namespace dstream
#  @brief
#  Package dstream is a simple framework for running UB data processing.
#  See each class description for details.

from daemon       import proc_daemon
from ds_data      import ds_status, ds_project, ds_daemon, ds_daemon_log
from ds_exception import DSException
from ds_proc_base import ds_project_base
