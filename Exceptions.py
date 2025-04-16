class UnknownReptHandle(Exception):
    def __init__(self,msg=None):
        self.msg=msg


class DuplicateKey(Exception):
    def __init__(self,msg=None):
        self.msg=msg


class NullabilityError(Exception):
    def __init__(self,msg=None):
        self.msg=msg


class KeyError(Exception):
    def __init__(self,msg=None):
        self.msg=msg



