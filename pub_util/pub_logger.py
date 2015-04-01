from pub_env import *
import logging, logging.handlers, sys

class _MSG_FORMAT_SCREEN(logging.Formatter):

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

class _MSG_FORMAT_FILE(logging.Formatter):

    _fmt_DEBUG    = logging.Formatter("[ %(levelname)-8s] %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    _fmt_INFO     = logging.Formatter("[ %(levelname)-8s] %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    _fmt_WARNING  = logging.Formatter("[ %(levelname)-8s] %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    _fmt_ERROR    = logging.Formatter("[ %(levelname)-8s] %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")
    _fmt_CRITICAL = logging.Formatter("[ %(levelname)-8s] %(module)s (L: %(lineno)-3d) >> {%(funcName)s} %(message)s")

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

    _loggers={}
    _fileHandlers={}
    _streamHandlers={}
    _globalLevel=kLOGGER_LEVEL
    _logFormatScreen=_MSG_FORMAT_SCREEN()
    _logFormatFile=_MSG_FORMAT_FILE()

    # Attach a logger for itself.
    _logger=logging.getLogger(__name__)
    _streamHandler = logging.StreamHandler(sys.stdout)
    _streamHandler.setFormatter(_logFormatScreen)
    _logger.addHandler(_streamHandler)
    
    @classmethod
    def get_logger(cls,name,dest=kLOGGER_DRAIN,fCounts=0):
        name=cls._correctName(name)
        cls._logger.info('Requested to add a logger for: %s' % name)

        if not name in cls._loggers.keys():
            cls._add_logger(name,dest,fCounts)
        return cls._loggers[name]

    @classmethod
    def _add_logger(cls,name,dest,fCounts):
        if not name in cls._loggers.keys():
            cls._logger.info('Adding a Logger: %s' % name)
            cls._loggers[name]=logging.getLogger(str(name))
            if dest == kLOGGER_FILE:
                cls._openFile(str(name),
                              '%s/%s' % (kLOGGER_FILE_LOCATION,str(name)),
                              1e8,
                              fCounts)
            else:
                cls._openStream(str(name))
            cls._loggers[str(name)].setLevel(cls._globalLevel)
            cls._loggers[name].info("OPENED LOGGER %s" % name)

    @classmethod
    def _openStream(cls,name):
        cls._logger.info('Adding a stream: %s' % name)
        if not str(name) in cls._loggers.keys():
            return False
        cls._streamHandlers[name] = logging.StreamHandler(sys.stdout)
        cls._streamHandlers[name].setFormatter(cls._logFormatScreen)
        cls._loggers[name].addHandler(cls._streamHandlers[name])    

    @classmethod
    def _openFile(cls, name, fname, size=1e8, fCounts=0):
        name=cls._correctName(name)
        if not str(name) in cls._loggers.keys():
            return False
        if str(name) in cls._fileHandlers.keys():
            return False
        cls._fileHandlers[name] = logging.handlers.RotatingFileHandler(filename = '%s.log' % fname, 
                                                                       maxBytes = size, 
                                                                       backupCount = fCounts)
        cls._fileHandlers[name].setFormatter(cls._logFormatFile)
        cls._loggers[name].addHandler(cls._fileHandlers[name])
        cls._loggers[name].info('OPENED LOGFILE name = %s.log' % str(fname))
        cls._loggers[name].info('OPENED LOGFILE size = %s' % str(size))
        cls._loggers[name].info('OPENED LOGFILE file count = %s' % str(fCounts))
        return True

    @classmethod
    def _closeFile(cls, name):
        name=cls._correctName(name)
        if not (name in cls._loggers.keys() and name in cls._fileHandlers.keys()):
            return False
        else:
            cls._loggers[name].removeHandler(cls._fileHandlers[name])
            cls._fileHandlers[name].close()
            cls._loggers[name].info('CLOSED LOGFILE')
            del cls._fileHandlers[name]

    @classmethod
    def _correctName(cls,name):
        if len(name.split('.'))>1:
            name=name.split('.')[len(name.split('.'))-1]      
        return name
