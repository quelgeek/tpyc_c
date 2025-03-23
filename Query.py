import xxhash

class Query():
    '''base class for Actian OpenAPI queries'''

    ##  this is essentially an abstract class; there is no good reason
    ##  to ever instantiate it. The useful subclasses are RepeatedQuery()
    ##  and PreparedQuery()
    
    ##  parameterized queries always use the repeated query placeholder.
    ##  If the query is a PreparedQuery we replace it with "? "
    _placeholder = '${} = ~V '


    def __init__(self,sql,name=None):
        if self._placeholder in sql:
            raise RuntimeError(
                'sql contains placeholder(s).'
                'Use RepeatedQuery() or PreparedQuery()')
        self._parmCount = 0
        self._queryText = _sql.encode()


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
        raise NotImpementedError        


class RepeatedQuery(Query):
    '''repeated SQL query details'''

    def __init__(self,sql,name=None):
        '''make query repeatable and publishable'''

        ##  number the placeholders
        n = self._parmCount = sql.count(self._placeholder) 
        ns = [i for i in range(n)]
        _sql = sql.format(*ns)
        self._queryText = _sql.encode()

        self.reptHandle = None
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
            ##  pad name to full extent with blanks
            self._name = _name.ljust(64,b' ')

            ##  use xxhash to generate a stable cross-platform signature
            signature = xxhash.xxh64(_sql).intdigest()
            self._hisig = signature >> 32 
            self._losig = signature & 0xFFFFFFFF    


    ##  expose immutable attributes

    @property
    def isPublished(self):
        return True if self._name else False


    @property
    def hisig(self):
        return self._hisig


    @property
    def losig(self):
        return self._losig



