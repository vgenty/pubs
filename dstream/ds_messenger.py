from pub_util     import pub_smtp, BaseException
from ds_exception import DSException

class daemon_messenger:

    _address = {}
    @classmethod
    def set_address(cls,owner,addr):
        cls._address[str(owner)] = str(addr)
            
    @classmethod
    def email(cls,owner,subject,text):
        if not owner in cls._address:
            return False
        try:
            res = pub_smtp(receiver = cls._address[owner],
                           subject  = subject,
                           text = text)
            if not res: res=False
            return res
        except BaseException as e:
            print e
            return False

    def __init__(self):
        self.email = self.__class__.email
        


