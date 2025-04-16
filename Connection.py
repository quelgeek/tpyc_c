import asyncio
import random
import pyngres.asyncio as py
from loguru import logger
from ErrorHandler import errorCheck

class Connection():

    def __init__(self,dbname):
        '''initialize a DBMS session manager'''
    
        try:
            self.envHandle = py.envHandle
        except AttributeError:
            ##  the OpenAPI has not been intialized; do it now
            inp = py.IIAPI_INITPARM()
            for attempt_version in range(py.IIAPI_VERSION,0,-1):
                inp.in_version = attempt_version
                inp.in_timeout = -1
                py.IIapi_initialize(inp)
                if inp.in_status == py.IIAPI_ST_SUCCESS:
                    envHandle = inp.in_envHandle
                    break
            if not envHandle:
                print(f'Can\'t initialize the Actian OpenAPI')
                quit()
            ##  inject the handle into the Pyngres namespace
            py.envHandle = envHandle            

        self.dbname = dbname
        self.envHandle = py.envHandle
        self.connHandle = None
        self.tranHandle = None


    async def connect(self):
        '''start a DBMS session'''

        ##  disperse the thundering herd
        ms_delay = random.randint(0,10) * .001
        await asyncio.sleep(ms_delay)

        ##  connect to the DBMS
        target = self.dbname.encode()
        cop = py.IIAPI_CONNPARM()
        cop.co_target = target
        cop.co_type = py.IIAPI_CT_SQL
        cop.co_connHandle = self.envHandle
        cop.co_timeout = -1
        await py.IIapi_connect(cop)
        if cop.co_genParm.gp_status != py.IIAPI_ST_SUCCESS:
            print(f'Can\'t open {self.dbname}')
            quit()

        self.connHandle = cop.co_connHandle
        self.tranHandle = None


    async def commit(self):
        '''COMMIT the current transaction'''

        cmp = py.IIAPI_COMMITPARM()
        cmp.cm_tranHandle = self.tranHandle
        await py.IIapi_commit(cmp)

        self.stmtHandle = None
        self.tranHandle = None 
   

    async def rollback(self):
        '''ROLLBACK the current transaction'''
        rbp = py.IIAPI_ROLLBACKPARM()
        rbp.rb_tranHandle = self.tranHandle
        await py.IIapi_rollback(rbp)

        self.stmtHandle = None
        self.tranHandle = None    


    async def disconnect(self):
        '''terminate the DBMS session'''

        dcp = py.IIAPI_DISCONNPARM()
        dcp.dc_connHandle = self.connHandle
        await py.IIapi_disconnect(dcp)
        self.connHandle = None


    async def execute(self, queryText):
        '''execute a non-parameterized SQL statement'''
        qyp = py.IIAPI_QUERYPARM()
        qyp.qy_connHandle = self.connHandle
        qyp.qy_queryType = py.IIAPI_QT_QUERY
        qyp.qy_queryText = queryText
        qyp.qy_parameters = False
        qyp.qy_tranHandle = self.tranHandle
        await py.IIapi_query( qyp )
        errorCheck(qyp.qy_genParm)
        self.tranHandle = qyp.qy_tranHandle
        self.stmtHandle = qyp.qy_stmtHandle

        gqp = py.IIAPI_GETQINFOPARM()
        gqp.gq_stmtHandle = self.stmtHandle
        await py.IIapi_getQueryInfo( gqp )
        errorCheck(gqp.gq_genParm)

        clp = py.IIAPI_CLOSEPARM()
        clp.cl_stmtHandle = self.stmtHandle
        await py.IIapi_close( clp )
        errorCheck(clp.cl_genParm)

        
    def handles(self):
        '''return the session handles'''

        return (self.envHandle, self.connHandle, self.tranHandle)
