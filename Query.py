import pyngres as py
import iitypes as ii
import ctypes
import xxhash

class Query():
    '''base class for Actian OpenAPI queries'''

    ##  this is essentially an abstract class; there is no good reason
    ##  to ever instantiate it. The useful subclasses are RepeatedQuery()
    ##  and PreparedQuery()
    
    ##  parameterized queries always expect the REPEATED query placeholder
    _placeholder = '${} = ~V '


    def __init__(self,sql,name=None):
#        if self._placeholder in sql:
#            raise RuntimeError(
#                'sql contains placeholder(s). '
#                'Use RepeatedQuery() or PreparedQuery()')
        self._parmCount = 0
        self._name = name
        self._queryText = sql.encode()

    ##  expose immutable attributes

    @property
    def name(self):
        return self._name

    @property
    def queryText(self):
        return self._queryText


    @property
    def parmCount(self):
        return self._parmCount


    def __str__(self):
        return self._queryText.decode()


class PreparedQuery(Query):
    
    def __init__(self,sql,name=None):
        super().__init__(sql,name)
        raise NotImplementedError        


class RepeatedQuery(Query):
    '''repeated SQL query details'''

    def __init__(self,sql,name=None):
        '''make query repeatable and publishable'''
        super().__init__(sql,name)

        ##  number the placeholders
        n = self._parmCount = sql.count(self._placeholder) 
        ns = [i for i in range(n)]
        _sql = sql.format(*ns)
        self._queryText = _sql.encode()

        self._reptHandle = None
        self._queryHandle = ii.IIAPI_HNDL_TYPE(
            ctypes.c_void_p(self._reptHandle))
        self._hisig = None
        self._losig = None
        self._name = None

        ##  repeated queries can be published or not; published queries are 
        ##  named and have a signature

        publish = True if name else False
        if publish:
            if not name.isascii():
                raise ValueError('name contains non-ASCII character(s)')
            try:
                _name = name.encode()
            except AttributeError:
                if type(name) is bytes:
                    _name = name
                else:
                    raise
            self._queryName = ii.Char(_name,64)

            ##  use xxhash to generate a stable cross-platform signature
            signature = xxhash.xxh64(_sql).intdigest()
            _hisig = signature >> 32 
            _losig = signature & 0xFFFFFFFF    
            self._hisig = ii.Integer(ctypes.c_int32(_hisig).value)
            self._losig = ii.Integer(ctypes.c_int32(_losig).value)


    @property
    def reptHandle(self):
        return self._reptHandle


    @reptHandle.setter
    def reptHandle(self, handle):
        '''intercept new reptHandle values'''
        self._reptHandle = handle
        ##  create an IIAPI_HNDL_TYPE for the repeated query handle
        self._queryHandle = ii.IIAPI_HNDL_TYPE(
            ctypes.c_void_p(self._reptHandle))


    ##  expose immutable attributes

    @property
    def isPublished(self):
        return True if self._name else False


    @property
    def hisig(self):
        '''return the high-order half of the query ID as iitypes.Integer'''

        return self._hisig


    @property
    def losig(self):
        '''return the low-order half of the query ID as iitypes.Integer'''

        return self._losig


    @property
    def queryName(self):
        '''return the query name as iitypes.Char'''

        return self._queryName


    @property
    def queryHandle(self):
        '''return the repeatQueryHandle as iitypes.IIAPI_HNDL_TYPE'''

        return self._queryHandle
