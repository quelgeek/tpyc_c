import asyncio
import ctypes
import random
import pyngres.asyncio as py
import iitypes as ii

import TPCC_random as tpc
from Exceptions import *
from loguru import logger
import Query as qy
from ErrorHandler import errorCheck


class Work():
    '''abstract class for workload'''

    def __init__(self, terminal):
        self.terminal = terminal
        self.session = terminal.session
        self.warehouse = ii.Integer(terminal.warehouse)
        self.district = ii.Integer(terminal.district)


    async def simulate_work(self):
        '''pretend to perform the work actions'''
        
        ms_delay = random.randint(0,10) * .001
        await asyncio.sleep(ms_delay)


    async def using_repeated(self):
        '''pretend to perform stock level processing using repeated queries'''

        await self.simulate_work()


    async def using_prepared(self):
        '''pretend to perform stock level processing using prepared queries'''

        await self.simulate_work()


    async def _invoke_repeated_sql(self, query, *parms):
        '''execute a repeated query, defining it if necessary'''

        ##  make sure enough arguments have been supplied
        parmCount =  query.parmCount
        argCount = len(parms)
        if argCount != parmCount:
            raise RuntimeError(
                f'expected {parmCount} arguments; got {argCount}')

        ##  run the repeated query; define it if it's not known
        try:
            attempted_handle = query.reptHandle
            result_set = await self._attempt_repeated_sql(query, *parms)
        except UnknownReptHandle:
            ##  if the query_handle is unchanged (re-)define the query
            if query.reptHandle == attempted_handle:
                await self._define_repeated_sql(query, *parms)
                ##  if it fails again, let it
                result_set = await self._attempt_repeated_sql(query, *parms)

        return result_set


    async def _define_repeated_sql(self, query, *query_parms):
        '''publish a query for repeated execution'''

        ##  initiate the repeated query definition protocol
        session = self.session
        connHandle = session.connHandle
        tranHandle = session.tranHandle
        queryText = query.queryText
        queryName = query.queryName.value.strip()
        logger.info(f'{self.terminal.name}: '
            f'defining repeated query {queryName}')
        qyp = py.IIAPI_QUERYPARM()
        qyp.qy_connHandle = connHandle
        qyp.qy_queryType = py.IIAPI_QT_DEF_REPEAT_QUERY
        qyp.qy_queryText = queryText
        qyp.qy_parameters = True
        qyp.qy_tranHandle = tranHandle
        qyp.qy_stmtHandle = None
        await py.IIapi_query( qyp )
        errorCheck(qyp.qy_genParm)
        tranHandle = qyp.qy_tranHandle
        stmtHandle = qyp.qy_stmtHandle

        parms = (query.hisig, query.losig, query.queryName) + query_parms
        ##  send the parameter descriptors
        descriptorCount = len(parms)
        descriptors = (py.IIAPI_DESCRIPTOR * descriptorCount)()
        sdp = py.IIAPI_SETDESCRPARM()
        sdp.sd_stmtHandle = stmtHandle
        sdp.sd_descriptorCount = descriptorCount
        sdp.sd_descriptor = descriptors
        for index, parm in enumerate(parms):
            if index in (0,1,2):
                parm.descriptor.ds_columnType = py.IIAPI_COL_SVCPARM
            else:
                parm.descriptor.ds_columnType = py.IIAPI_COL_QPARM
            sdp.sd_descriptor[index] = parm.descriptor 
        await py.IIapi_setDescriptor( sdp )
        errorCheck(sdp.sd_genParm)

        ##  send the parm datavalues
        datavalues = (py.IIAPI_DATAVALUE * descriptorCount)()
        ppp = py.IIAPI_PUTPARMPARM()
        ppp.pp_stmtHandle = stmtHandle
        ppp.pp_parmCount = descriptorCount
        ppp.pp_parmData = datavalues
        for index, parm in enumerate(parms):
            ppp.pp_parmData[index] = parm.datavalue
        await py.IIapi_putParms( ppp )
        errorCheck(ppp.pp_genParm)

        ##  get repeat results
        gqp = py.IIAPI_GETQINFOPARM()
        gqp.gq_stmtHandle = stmtHandle
        await py.IIapi_getQueryInfo( gqp )
        errorCheck(gqp.gq_genParm)
        if gqp.gq_mask & py.IIAPI_GQ_REPEAT_QUERY_ID:
            ##  save the repeated query handle for reuse
            query.reptHandle = gqp.gq_repeatQueryHandle
            logger.debug(f'{self.terminal.name}: {query.reptHandle=}')

        ##  free resources
        clp = py.IIAPI_CLOSEPARM()
        clp.cl_stmtHandle = stmtHandle
        await py.IIapi_close( clp )
        errorCheck(clp.cl_genParm)

        session.tranHandle = tranHandle


    async def _attempt_repeated_sql(self, query, *query_parms):
        '''try to execute a repeated query'''

        ##  the query handle may not be known yet
        if not query.reptHandle:
            raise UnknownReptHandle

        ##  initiate the repeated query execution protocol
        session = self.session
        connHandle = session.connHandle
        tranHandle = session.tranHandle
        queryName = query.queryName.value.strip()
        logger.info(f'{self.terminal.name}: ' 
            f'invoking repeated query {queryName}')
        qyp = py.IIAPI_QUERYPARM()
        qyp.qy_connHandle = connHandle
        qyp.qy_queryType = py.IIAPI_QT_EXEC_REPEAT_QUERY
        qyp.qy_queryText = None
        qyp.qy_parameters = True    ##  we send at least the query handle 
        qyp.qy_tranHandle = tranHandle
        qyp.qy_stmtHandle = None
        await py.IIapi_query( qyp );
        errorCheck(qyp.qy_genParm)
        tranHandle = qyp.qy_tranHandle
        session.tranHandle = tranHandle
        stmtHandle = qyp.qy_stmtHandle

        parms = (query.queryHandle,) + query_parms
        ##  send the parameter descriptors
        descriptorCount = len(parms)
        descriptors = (py.IIAPI_DESCRIPTOR * descriptorCount)()
        sdp = py.IIAPI_SETDESCRPARM()
        sdp.sd_stmtHandle = stmtHandle
        sdp.sd_descriptorCount = descriptorCount
        sdp.sd_descriptor = descriptors        
        ##  the query handle is the first parameter
        for index, parm in enumerate(parms):
            if index == 0:
                parm.descriptor.ds_columnType = py.IIAPI_COL_SVCPARM
            else:
                parm.descriptor.ds_columnType = py.IIAPI_COL_QPARM
            sdp.sd_descriptor[index] = parm.descriptor
        await py.IIapi_setDescriptor( sdp )
        errorCheck(sdp.sd_genParm)

        ##  send the arguments
        datavalues = (py.IIAPI_DATAVALUE * descriptorCount)()
        ppp = py.IIAPI_PUTPARMPARM()
        ppp.pp_stmtHandle = stmtHandle
        ppp.pp_parmCount = descriptorCount
        ppp.pp_parmData = datavalues        
        for index, parm in enumerate(parms):
            ppp.pp_parmData[index] = parm.datavalue
        await py.IIapi_putParms( ppp )
        errorCheck(ppp.pp_genParm)

        ##  get the result/tuple descriptor
        gdp = py.IIAPI_GETDESCRPARM()
        gdp.gd_stmtHandle = qyp.qy_stmtHandle
        await py.IIapi_getDescriptor(gdp)
        errorCheck(gdp.gd_genParm)
        if gdp.gd_genParm.gp_status != py.IIAPI_ST_SUCCESS:
            ##  see if the repeated query handle is invalidated
            gqp = py.IIAPI_GETQINFOPARM()
            gqp.gq_stmtHandle = qyp.qy_stmtHandle;
            await py.IIapi_getQueryInfo( gqp )
            errorCheck(gqp.gq_genParm)
            flags = gqp.gq_flags
            if flags & py.IIAPI_GQF_UNKNOWN_REPEAT_QUERY:
                ##  end this attempt and raise UnknownReptHandle
                cnp = py.IIAPI_CANCELPARM()
                cnp.cn_stmtHandle = stmtHandle
                await py.IIapi_cancel(cnp)
                ##  unusually, we want to see IIAPI_ST_FAILURE here
                logger.info(f'CANCELLING ({cnp.cn_genParm.gp_status=})')
                ##  close the query
                clp = py.IIAPI_CLOSEPARM()
                clp.cl_stmtHandle = stmtHandle
                await py.IIapi_close(clp)
                errorCheck(clp.cl_genParm)
                raise UnknownReptHandle

        ##  fetch the result set, if any
        body = None
        if gdp.gd_descriptorCount > 0:

            ##  set up the tuple buffer list
            tuple = {}
            columnData = (py.IIAPI_DATAVALUE * gdp.gd_descriptorCount)()
            for column_index in range(gdp.gd_descriptorCount):
                descriptor = gdp.gd_descriptor[column_index]
                clone = type(descriptor).from_buffer_copy(descriptor)
                descriptor = clone
                buffer_allocator = ii.allocator_for_type(descriptor)
                buffer = buffer_allocator(descriptor=descriptor)
                columnData[column_index] = buffer.datavalue
                columnName = descriptor.ds_columnName.decode()
                tuple[columnName] = buffer        

            ##  construct the result set header
            header = [attrName for attrName in tuple]

            ##  fetch all the rows into the result set body
            ##  (Note: increasing readahead_count could speed this up a bit)
            body = []
            readahead_count = 1
            gcp = py.IIAPI_GETCOLPARM()
            gcp.gc_rowCount = readahead_count
            gcp.gc_columnCount = gdp.gd_descriptorCount
            gcp.gc_columnData = columnData
            gcp.gc_stmtHandle = qyp.qy_stmtHandle
            while True:
                await py.IIapi_getColumns(gcp)
                errorCheck(gcp.gc_genParm)
                if gcp.gc_genParm.gp_status != py.IIAPI_ST_SUCCESS:
                    break
                row = tuple.copy()
                body.append(row)
                
        ##  check for query information
        gqp = py.IIAPI_GETQINFOPARM()
        gqp.gq_stmtHandle = qyp.qy_stmtHandle;
        await py.IIapi_getQueryInfo( gqp )
        errorCheck(gqp.gq_genParm)

        ##  free resources
        clp = py.IIAPI_CLOSEPARM()
        clp.cl_stmtHandle = stmtHandle
        await py.IIapi_close( clp )
        errorCheck(clp.cl_genParm)

        return body


class Delivery(Work):
    pass



