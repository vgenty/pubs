from ds_proc_base import ds_base
import subprocess
from ds_exception import DSException
import time,os,signal

class finished_process:

    _return_value = -9

    def __init__(self,arg=-9):
        self._return_value = arg

    def poll(self): return self._return_value

## @class ds_multiprocess
#  @brief A class that handles parallelization of command execution
#  @details Someone should replace this w/ multiprocessing
class ds_multiprocess(ds_base):

    _proc_v    = []
    _isgroup_v = []
    _cout_v    = []
    _cerr_v    = []

    def __init__(self,arg=None):

        if arg is None: arg = self.__class__.__name__
        super(ds_multiprocess,self).__init__(arg)
        self._proc_v=[]
        self._cout_v=[]
        self._cerr_v=[]

    def execute(self,cmd,group_session=True):
        active_ctr = self.active_count()
        session=bool(group_session)
        try:
            shell = type(cmd) == type(str())
            if group_session:
                p = subprocess.Popen(cmd, shell=shell,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     preexec_fn=os.setsid)
            else:
                p = subprocess.Popen(cmd, shell=shell,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
            self._isgroup_v.append(session)
            self._proc_v.append(p)
            self._cerr_v.append(None)
            self._cout_v.append(None)
        except Exception:
            self.error('Failed to execute: %s' % cmd)
            self._cout_v.append('')
            self._cerr_v.append('Failed to execute: %s' % cmd)
            self._proc_v.append(finished_process(-9))
            self._isgroup_v.append(False)
        return (len(self._proc_v)-1,active_ctr+1)

    def finished(self,index=None):
        if index is None:
            for p in self._proc_v:
                if p.poll() is None:
                    return False
        elif index < len(self._proc_v):
            return (not self._proc_v[index].poll() is None)        
        else:
            raise DSException('Invalid process index: %d' % index)
        return True

    def poll(self,index):
        return self._proc_v[index].poll()

    def communicate(self,index):

        if self._cout_v[index] is None:

            self._cout_v[index], self._cerr_v[index] = self._proc_v[index].communicate()
            self._proc_v[index] = finished_process(self._proc_v[index].poll())

        return (self._cout_v[index],self._cerr_v[index])

    def kill(self):
        self.info('All-process termination requested...')
        for i in xrange(len(self._proc_v)):
            p = self._proc_v[i]
            if not p.poll() is None: continue
            self.info('Found active process (index=%d,pid=%d) ... terminating... @ %s' % (i,p.pid,time.strftime('%Y-%m-%d %H:%M:%S')))
            if self._isgroup_v[i]:
                os.killpg(p.pid,signal.SIGINT)
            else:
                p.kill()
            self.debug('Termination call made for process (index=%d,pid=%d)' % (i,p.pid))
            slept_time = 0
            while slept_time < 10:
                if not p.poll() is None: break
                time.sleep(0.2)
                slept_time += 0.2
                if int(slept_time*10)%20 == 0:
                    self.info('Waiting for process to be killed... (%s/10)' % slept_time)
            if p.poll() is None:
                self.warning('Force-killing the process (index=%d,pid=%d) @ %s' % (i,p.pid,time.strftime('%Y-%m-%d %H:%M:%S')))
                if self._isgroup_v[i]:
                    os.killpg(p.pid,signal.SIGTERM)
                else:
                    os.kill(p.pid,signal.SIGTERM)

        if self.active_count():
            self.kill()

    def active_count(self):
        ctr=0
        for i in xrange(len(self._proc_v)):
            p = self._proc_v[i]
            if p.poll() is None:
                ctr+=1
            elif not isinstance(p,finished_process):
                self.debug('New finished process! communicating @ %s...' % time.strftime('%Y-%m-%d %H:%M:%S'))
                self._cout_v[i], self._cerr_v[i] = self._proc_v[i].communicate()
                self.debug('...done!')
                self._proc_v[i] = finished_process(p.poll())
                del p

        return ctr

    









        
