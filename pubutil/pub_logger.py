import logging, logging.handlers, sys

class _MSG_FORMAT(logging.Formatter):

    _fmt_DEBUG    = logging.Formatter("\033[1;34;40m[ %(levelname)-8s]\033[00m %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    _fmt_INFO     = logging.Formatter("\033[1;35;40m[ %(levelname)-8s]\033[00m %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    _fmt_WARNING  = logging.Formatter("\033[5;1;33;40m[ %(levelname)-8s]\033[00m %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    _fmt_ERROR    = logging.Formatter("\033[5;1;31;40m[ %(levelname)-8s]\033[00m %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    _fmt_CRITICAL = logging.Formatter("\033[5;1;33;41m[ %(levelname)-8s]\033[00m %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    def format(self,record):
        if record.levelno <= 10:
            return self._fmt_DEBUG.format(record)
        elif record.levelno <= 20:
            return self._fmt_INFO.format(record)
        elif record.levelno <= 30:
            return self._fmt_WARNING.format(record)
        elif record.levelno <= 40:
            return self._fmt_ERROR.format(record)
        else:
            return self._fmt_CRITICAL.format(record)

class pub_logger:

    _Loggers={}
    _FileHandlers={}
    _StreamHandlers={}
    _globalLevel=0
    _LogFormat=_MSG_FORMAT()

    # Attach a logger for itself.
    _Logger=logging.getLogger(__name__)
    _StreamHandler = logging.StreamHandler(sys.stdout)
    _StreamHandler.setFormatter(_LogFormat)
    _Logger.addHandler(_StreamHandler)
    
    @classmethod
    def GetME(cls,name,fname='',fCounts=0):
        name=cls._CorrectName(name)
        cls._Logger.info('Requested to add a logger for: %s' % name)

        if not name in cls._Loggers.keys():
            cls._AddLogger(name,fname,fCounts)
        return cls._Loggers[name]

    @classmethod
    def _AddLogger(cls,name,fname='',fCounts=0):
        if not name in cls._Loggers.keys():
            cls._Logger.info('Adding a Logger: %s' % name)
            cls._Loggers[name]=logging.getLogger(str(name))
            if fname:
                cls.OpenFile(str(name),fname,fCounts)
            else:
                cls._OpenStream(str(name))
            cls._Loggers[str(name)].setLevel((cls._globalLevel)*10)
            cls._Loggers[name].info("OPENED LOGGER %s" % name)

    @classmethod
    def _OpenStream(cls,name):
        cls._Logger.info('Adding a stream: %s' % name)
        if not str(name) in cls._Loggers.keys():
            return False
        cls._StreamHandlers[name] = logging.StreamHandler(sys.stdout)
        cls._StreamHandlers[name].setFormatter(cls._LogFormat)
        cls._Loggers[name].addHandler(cls._StreamHandlers[name])    

    @classmethod
    def OpenFile(cls, name, fname, size=1e8, fCounts=0):
        name=cls._CorrectName(name)
        if not str(name) in cls._Loggers.keys():
            return False
        if str(name) in cls._FileHandlers.keys():
            return False
        cls._FileHandlers[name] = logging.handlers.RotatingFileHandler(filename = '%s.log' % fname, 
                                                                       maxBytes = size, 
                                                                       backupCount = fCounts)
        cls._FileHandlers[name].setFormatter(cls._LogFormat)
        cls._Loggers[name].addHandler(cls._FileHandlers[name])
        cls._Loggers[name].info('OPENED LOGFILE name = %s.log' % str(fname))
        cls._Loggers[name].info('OPENED LOGFILE size = %s' % str(size))
        cls._Loggers[name].info('OPENED LOGFILE file count = %s' % str(fCounts))
        return True

    @classmethod
    def CloseFile(cls, name):
        name=cls._CorrectName(name)
        if not (name in cls._Loggers.keys() and name in cls._FileHandlers.keys()):
            return False
        else:
            cls._Loggers[name].removeHandler(cls._FileHandlers[name])
            cls._FileHandlers[name].close()
            cls._Loggers[name].info('CLOSED LOGFILE')
            del cls._FileHandlers[name]

    @classmethod
    def SetLevelMSG(cls, name='', level=0):
        try:
            level=int(name)
            name=''
        except ValueError:
            pass

        name=cls._CorrectName(name)

        if len(name)==0:
            cls._globalLevel=level
            for name in cls._Loggers.keys():
                cls._Loggers[name].setLevel((level+1)*10)
        else:
            if not name in cls._Loggers.keys():
                cls._AddLogger(name,level)
            cls._Loggers[name].setLevel((level+1)*10)


    @classmethod
    def _CorrectName(cls,name):
        if len(name.split('.'))>1:
            name=name.split('.')[len(name.split('.'))-1]      
        return name
