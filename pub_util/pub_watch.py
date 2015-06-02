import time

class pub_watch:

    _clocks = {}

    @classmethod
    def start(cls,name):
        cls._clocks[name]=time.time()
        
    @classmethod
    def time(cls,name):
        if not name in cls._clocks:
            return 0
        return time.time() - cls._clocks[name]


    
