import asyncio
import pyngres.asyncio as py
import ctypes
import struct
import ingtypes as ii
from loguru import logger
import random
import time
import xxhash

##  we use xxhash to generate cross-platform stable "compile-time" 
##  repeated query IDs

CONFIGWHSECOUNT    = 10
CONFIGITEMCOUNT    = 100000     ## tpc-c std = 100,000
CONFIGDISTPERWHSE  = 10         ## tpc-c std = 10
CONFIGCUSTPERDIST  = 3000       ## tpc-c std = 3,000

ADHOC = 'ADHOC'                 ##  indication to use literal query
PREPARED = 'PREPARED'           ##  indication to use prepared query
REPEATED = 'REPEATED'           ##  indication to use repeated query
LEARNED = 'LEARNED'             ##  indication to decide based on use


class UnknownReptHandle(Exception):
    def __init__(self,msg):
        self.msg=msg


class NullabilityError(Exception):
    def __init__(self,msg):
        self.msg=msg


class KeyError(Exception):
    def __init__(self,msg):
        self.msg=msg


class RepeatedQuery():
    '''repeated SQL query details'''

    def __init__(self,sql,name=None):
        '''initialize a RepeatedQuery description'''

        ##  number the placeholders
        placeholder = '${} = ~V'
        self._parmCount = sql.count(placeholder) 
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
    def queryText(self):
        return self._queryText


    @property
    def parmCount(self):
        return self._parmCount


    @property
    def isPublished(self):
        return True if self._name else False


    @property
    def hisig(self):
        return self._hisig


    @property
    def losig(self):
        return self._losig


    @property
    def name(self):
        return self._name


stmtGetCustWhse = RepeatedQuery(
    'SELECT c.discount, c.last, c.credit, w.tax '
    'FROM customer c, warehouse w '
    'WHERE w.warehouse = ${} ~V '
    'AND w.warehouse = c.warehouse '
    'AND c.district = ${} ~V '
    'AND c.customer = ${} ~V ', name='stmtGetCustWhse' )

stmtGetDist = RepeatedQuery(
    'SELECT d.next_o_id, float4(d.tax) as tax '
    'FROM district d '
    'WHERE d.district = ${} ~V '
    'AND d.warehouse = ${} ~V '
    'FOR UPDATE', name = 'stmtGetDist' )

stmtInsertNewOrder = RepeatedQuery(
    'INSERT INTO new_order (order, district, warehouse) ' 
    'VALUES ( ${} ~V , ${} ~V , ${} ~V )', name = 'stmtInsertNewOrder' )

stmtUpdateDist = RepeatedQuery(
    'UPDATE district d '
    'SET next_o_id = d.next_o_id+1 ' 
    'WHERE d.district = ${} = ~V '
    'AND d.warehouse = ${} = ~V ', name = 'stmtUpdateDist' )

stmtInsertOrder = RepeatedQuery(
    'INSERT INTO order ' 
    '(order, district, warehouse, customer, entry_d, ol_cnt, all_local) '
    'VALUES (${} = ~V , ${} = ~V , ${} = ~V , ${} = ~V , '
    'CURRENT_TIME , ${} = ~V , ${} = ~V )', name = 'stmtInsertOrder' )

stmtGetItem = RepeatedQuery(
    'SELECT i.price, i.name, i.data '
    'FROM item i '
    'WHERE i.item = ${} = ~V ', name = 'stmtGetItem' )

stmtGetStock = RepeatedQuery(
    'SELECT s.quantity, s.data, s.dist_01, s.dist_02, s.dist_03, '
    's.dist_04, s.dist_05,  s.dist_06, s.dist_07, s.dist_08, '
    's.dist_09, s.dist_10 '
    'FROM stock s '
    'WHERE s.item = ${} = ~V '
    'AND s.warehouse = ${} = ~V '
    'FOR UPDATE', name = 'stmtGetStock' )

stmtUpdateStock = RepeatedQuery(
    'UPDATE stock s 
    'SET s.quantity = ${} = ~V , s.ytd = s.ytd +${} = ~V , '
    s.remote_cnt = s.remote_cnt + ${} = ~V '
    'WHERE s.item = ${} = ~V '
    'AND s.warehouse = ${} = ~V ', name = 'stmtUpdateStock' )

stmtInsertOrderLine = RepeatedQuery(
    'INSERT INTO order_line (order, district, warehouse, ol_number, item, '
    'supply_warehouse, quantity, amount, dist_info) '
    'VALUES (${} = ~V ,${} = ~V ,${} = ~V ,${} = ~V , ${} = ~V , '
    '${} = ~V ,${} = ~V ,${} = ~V ,${} = ~V )', name = 'stmtInsertOrderLine' )

payUpdateWhse = RepeatedQuery(
    'UPDATE warehouse w '
    'SET ytd = ytd+${} = ~V ' 
    'WHERE w.warehouse = ${} = ~V ', name = 'payUpdateWhse' )

payGetWhse = RepeatedQuery(
    'SELECT w.street_1, w.street_2, w.city, w.state, w.zip, w.name '
    'FROM warehouse w '
    'WHERE w.warehouse = ${} = ~V ', name = 'payGetWhse' )

payUpdateDist = RepeatedQuery(
    'UPDATE district d '
    'SET ytd = ytd +${} = ~V ' 
    'WHERE d.warehouse = ${} = ~V '
    'AND d.district = ${} = ~V ', name = 'payUpdateDist' )

payGetDist = RepeatedQuery(
    'SELECT d.street_1, d.street_2, d.city, d.state, d.zip, d.name '
    'FROM district d '
    'WHERE d.warehouse = ${} = ~V '
    'AND d.district = ${} = ~V ', name = 'payGetDist' )

payCountCust = RepeatedQuery(
    'SELECT count(*) AS namecnt '
    'FROM customer c ' 
    'WHERE c.last = ${} = ~V '
    'AND c.district = ${} = ~V '
    'AND c.warehouse = ${} = ~V ', name = 'payCountCust' )

payCursorCustByName = RepeatedQuery(
    'SELECT c.first, c.middle, c.customer, c.street_1, c.street_2, '
    'c.city, c.state, c.zip, c.phone, c.credit, c.credit_lim, '
    'c.discount, c.balance, c.since ' 
    'FROM customer c '
    'WHERE c.warehouse = ${} = ~V '
    'AND c.district = ${} = ~V '
    'AND c.last = ${} = ~V '
    'ORDER BY c.warehouse, c.district, c.last, c.first',
    name = 'payCursorCustByName' )

payGetCust = RepeatedQuery(
    'SELECT c.first, c.middle, c.last, c.street_1, c.street_2, '
    'c.city, c.state, c.zip, c.phone, c.credit, c.credit_lim, '
    'c.discount, c.balance, c.since '
    'FROM customer c '
    'WHERE c.warehouse = ${} = ~V '
    'AND c.district = ${} = ~V ' 
    'AND c.customer = ${} = ~V ', name = 'payGetCust' )

payGetCustCdata = RepeatedQuery(
    'SELECT c.data '
    'FROM customer c '
    'WHERE c.warehouse = ${} = ~V '
    'AND c.district = ${} = ~V ' 
    'AND c.customer = ${} = ~V ', name = 'payGetCustCdata' )

payUpdateCustBalCdata = RepeatedQuery(
    'UPDATE customer c '
    'SET balance = ${} = ~V , data = ${} = ~V '  
    'WHERE c.warehouse = ${} = ~V '
    'AND c.district = ${} = ~V ' 
    'AND c.customer = ${} = ~V ', name = 'payUpdateCustBalCdata' )

payUpdateCustBal = RepeatedQuery(
    'UPDATE customer c '
    'SET balance = ${} = ~V ' 
    'WHERE c.warehouse = ${} = ~V '
    'AND c.district = ${} = ~V ' 
    'AND c.customer = ${} = ~V ', name = 'payUpdateCustBal' )

payInsertHist = RepeatedQuery(
    'INSERT INTO history (customer_district, customer_warehouse, '
    'customer, district, warehouse, date, amount, data) ' 
    'VALUES (${} = ~V ,${} = ~V ,${} = ~V ,${} = ~V , '
    '${} = ~V ,CURRENT_TIME ,${} = ~V ,${} = ~V )',
    name = 'payInsertHist' )

ordStatCountCust = RepeatedQuery(
    'SELECT count(*) AS namecnt '
    'FROM customer c '
    'WHERE c.last = ${} = ~V '
    'AND c.district = ${} = ~V ' 
    'AND c.warehouse = ${} = ~V ', name = 'ordStatCountCust' )

ordStatGetCust = RepeatedQuery(
    'SELECT c.balance, c.first, c.middle, customer '
    'FROM customer c '
    'WHERE c.last = ${} = ~V ' 
    'AND c.district = ${} = ~V ' 
    'AND c.warehouse = ${} = ~V ' 
    'ORDER BY warehouse, district, last, first', name = 'ordStatGetCust' )

ordStatGetNewestOrd = RepeatedQuery(
    'SELECT MAX(order) AS maxorderid '
    'FROM order o '
    'WHERE o.warehouse = ${} = ~V ' 
    'AND o.district = ${} = ~V ' 
    'AND o.customer = ${} = ~V ', name = 'ordStatGetNewestOrd' )

ordStatGetCustBal = RepeatedQuery(
    'SELECT c.balance, c.first, c.middle, c.last '
    'FROM customer c '
    'WHERE c.customer = ${} = ~V ' 
    'AND c.district = ${} = ~V ' 
    'AND c.warehouse = ${} = ~V ', name = 'ordStatGetCustBal' )

ordStatGetOrder = RepeatedQuery(
    'SELECT o.carrier_id, o.entry_d '
    'FROM order o '
    'WHERE o.warehouse = ${} = ~V ' 
    'AND o.district = ${} = ~V ' 
    'AND o.customer = ${} = ~V ' 
    'AND o.order = ${} = ~V ', name = 'ordStatGetOrder' )

ordStatGetOrderLines = RepeatedQuery(
    'SELECT ol.item, ol.supply_warehouse, ol.quantity, '
    'ol.amount, ol.delivery_d '
    'FROM order_line ol '
    'WHERE ol.order = ${} = ~V ' 
    'AND ol.district =${} = ~V ' 
    'AND ol.warehouse = ${} = ~V ', name = 'ordStatGetOrderLines' )

delivGetOrderId = RepeatedQuery(
    'SELECT no.order '
    'FROM new_order no '
    'WHERE no.district = ${} = ~V ' 
    'AND no.warehouse = ${} = ~V ' 
    'ORDER BY order ASC', name = 'delivGetOrderId' )

delivDeleteNewOrder = RepeatedQuery(
    'DELETE FROM new_order no '
    'WHERE no.district = ${} = ~V ' 
    'AND no.warehouse = ${} = ~V ' 
    'AND no.order = ${} = ~V ', name = 'delivDeleteNewOrder' )

delivGetCustId = RepeatedQuery(
    'SELECT o.customer '
    'FROM order o '
    'WHERE o.order = ${} = ~V ' 
    'AND o.district = ${} = ~V ' 
    'AND o.warehouse = ${} = ~V ', name = 'delivGetCustId' )

delivUpdateCarrierId = RepeatedQuery(
    'UPDATE order o '
    'SET carrier_id = ${} = ~V ' 
    'WHERE o.order = ${} = ~V ' 
    'AND o.district = ${} = ~V ' 
    'AND o.warehouse = ${} = ~V ', name = 'delivUpdateCarrierId' )

delivUpdateDeliveryDate = RepeatedQuery(
    'UPDATE order_line ol '
    'SET delivery_d = CURRENT_TIME ' 
    'WHERE ol.order = ${} = ~V ' 
    'AND ol.district = ${} = ~V ' 
    'AND ol.warehouse = ${} = ~V ', name = 'delivUpdateDeliveryDate' )

delivSumOrderAmount = RepeatedQuery(
    'SELECT SUM(amount) AS total '
    'FROM order_line ol '
    'WHERE ol.order = ${} = ~V ' 
    'AND ol.district = ${} = ~V ' 
    'AND ol.warehouse = ${} = ~V ', name = 'delivSumOrderAmount' )

delivUpdateCustBalDelivCnt = RepeatedQuery(
    'UPDATE customer c '
    'SET balance = balance+${} = ~V , delivery_cnt = delivery_cnt+1 '
    'WHERE c.customer = ${} = ~V ' 
    'AND c.district = ${} = ~V ' 
    'AND c.warehouse = ${} = ~V ', name = 'delivUpdateCustBalDelivCnt' )


##  alias random.randint to make reference to the TPC-C spec obvious
TPCC_random = random.randint

def TPCC_NU_random(x, min, max):
    '''return a non-uniform random number using the TPC-C algorithm'''

    value = ((TPCC_random(0,x) | TPCC_random(min,max)) + 
        TPCC_random(0,x)) % (max-min+1) + min
    return value


def get_itemID():
    ID = TPCC_NU_random(8191,1,100000)
    return ID


def get_customerID():
    ID = TPCC_NU_random(1023,1,3000)
    return ID


def get_lastname():
    fragment = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE',
        'ANTI', 'CALLY', 'ATION', 'EING']
    num = TPCC_NU_random(255,0,999)
    lastname = fragment[num//100] + fragment[(num//10)%10] + fragment[num%10]
    return lastname


def _log_rowcount(rows):
    rowcount = 0 if not rows else len(rows)
    msg = f'({rowcount} rows)'
    logger.success(msg)


class Terminal():

    def __init__(self,
            name,
            event_lock,
            run_event,
            ready_event, 
            ack_event,
            halt_event,
            commits_queue,
            jobs_queue):

        self.name = name
        self.warehouse = TPCC_random(1,CONFIGWHSECOUNT)
        self.district = TPCC_random(1,CONFIGDISTPERWHSE)

        self.event_lock = event_lock 
        self.run_event = run_event 
        self.ready_event = ready_event  
        self.ack_event = ack_event 
        self.halt_event = halt_event 
        self.commits_queue = commits_queue 
        self.jobs_queue = jobs_queue

        self.connHandle = None
        self.tranHandle = None
        self.stmtHandle = None
        self.envHandle = None

        logger.success(f'started {name=} {self.warehouse=} {self.district=}')


    async def connect(self,target):
        target = target.encode()
        inp = py.IIAPI_INITPARM()
        inp.in_version = py.IIAPI_VERSION_11
        inp.in_timeout = -1
        py.IIapi_initialize(inp)

        envHandle = inp.in_envHandle
        self.envHandle = envHandle

        ##  disperse the thundering herd
        ms_delay = random.randint(0,10) * .001
        await asyncio.sleep(ms_delay)

        cop = py.IIAPI_CONNPARM()
        cop.co_target = target
        cop.co_connHandle = self.envHandle
        cop.co_type = py.IIAPI_CT_SQL
        cop.co_timeout = -1
        await py.IIapi_connect(cop)

        connHandle = cop.co_connHandle
        self.connHandle = connHandle

    
    async def disconnect(self):
        dcp = py.IIAPI_DISCONNPARM()
        dcp.dc_connHandle = self.connHandle
        await py.IIapi_disconnect(dcp)
        self.connHandle = None
        
        rep = py.IIAPI_RELENVPARM()
        rep.re_envHandle = self.envHandle
        py.IIapi_releaseEnv(rep)
        self.envHandle = None

        tmp = py.IIAPI_TERMPARM()
        py.IIapi_terminate(tmp)


    async def commit(self):
        cmp = py.IIAPI_COMMITPARM()
        cmp.cm_tranHandle = self.tranHandle

        await py.IIapi_commit(cmp)

        self.stmtHandle = None
        self.tranHandle = None    


    async def rollback(self):
        rbp = IIAPI_ROLLBACKPARM()
        rbp.rb_tranHandle = self.tranHandle
        
        await py.IIapi_rollback(rbp)

        self.stmtHandle = None
        self.tranHandle = None    


#    async def execute(query):
#        if type(query) is RepeatedQuery:
#            rows = await self._repeated_exec(query)
#        else:
#            rows = await self._prepared_exec(query)
#        return rows


    async def _receive_result_rows():
        '''receive result rows from standard, prepared, and repeated queries'''
        result_rows = []
        ...
        return result_rows


    async def _complete_sql(stmtHandle):
        '''complete processing of standard, prepared, and repeated queries'''
        gqp = IIAPI_GETQINFOPARM()
        gqp.gq_stmtHandle = stmtHandle
        await IIapi_getQueryInfo( gqp )
        if gqp.gq_genParm.gp_status != py.IIAPI_ST_SUCCESS:
            breakpoint()
            raise RuntimeError
        clp = IIAPI_CLOSEPARM()
        clp.cl_stmtHandle = stmtHandle
        await IIapi_close( clp )


    async def _invoke_repeated_sql(query, *parms):
        '''execute a repeated query'''

        ##  preempt query execution if there is no reptHandle
        if not query.reptHandle:
            raise UnknownReptHandle

        ##  make sure enough arguments have been supplied
        parmCount =  query.parmCount
        argCount = len(parms)
        if argCount != parmCount:
            raise RuntimeError(f'expected {parmCount} arguments; got {argCount}')

        ##  initiate the repeated query execution protocol
        logger.info('invoking repeated query')
        qyp = py.IIAPI_QUERYPARM()
        qyp.qy_connHandle = self.connHandle
        qyp.qy_queryType = IIAPI_QT_EXEC_REPEAT_QUERY
        qyp.qy_parameters = True
        qyp.qy_tranHandle = self.tranHandle
        qyp.qy_stmtHandle = None
        await py.IIapi_query( qyp );
        if qyp.qy_genParm.gp_status != py.IIAPI_ST_SUCCESS:
            breakpoint()
            raise RuntimeError
        stmtHandle = qyp.qy_stmtHandle

        ##  send the parameter descriptors; a repeated query will always send
        ##  its own handle plus any parms used in the query
        parmCount = len(parms)
        descriptorCount = 1 + parmCount
        descriptor = (py.IIAPI_DESCRIPTOR * descriptor_count)()
        descriptor[0].ds_dataType = IIAPI_HNDL_TYPE
        descriptor[0].ds_length = ctypes.sizeof(II_PTR)
        descriptor[0].ds_nullable = False
        descriptor[0].ds_precision = 0
        descriptor[0].ds_scale = 0
        descriptor[0].ds_columnType = IIAPI_COL_SVCPARM
        for i in range(parmCount):
            descriptor[i+1] = parms[i].descriptor
        sdp = IIAPI_SETDESCRPARM()
        sdp.sd_stmtHandle = stmtHandle
        sdp.sd_descriptorCount = descriptorCount
        sdp.sd_descriptor = descriptor
        await py.IIapi_setDescriptor( sdp )

        ##  send the parameters, including the repeated query handle
        reptHandle = query.reptHandle
        parmData = (py.IIAPI_DATAVALUE * descriptor_count)()
        parmData[0].dv_null = False
        parmData[0].dv_length = ctypes.sizeof(II_PTR)
        parmData[0].dv_value = ctypes.addressof(reptHandle)
        for i in range(parmCount):
            parmData[i+1] = parms[i].datavalue
        ppp = py.IIAPI_PUTPARMPARM()
        ppp.pp_stmtHandle = stmtHandle
        ppp.pp_parmCount = descriptorCount
        ppp.pp_parmData = parmData
        await py.IIapi_putParms( ppp )
            breakpoint()
            ##  check for py.E_AP0014_INVALID_REPEAT_ID here...
            # fix me; get error code
            if error == py.E_AP0014_INVALID_REPEAT_ID:
                raise UnknownReptHandle
            else:
                raise RuntimeException
                
        ##  complete the repeated query execution protocol
        await _complete_sql(stmtHandle)


    async def _register_repeated_sql(sql, parms):
        ...


    async def _execute_repeated_sql():
        ...

    
    async def _execute_repeated_sql(sql, *parms, expect_rows=False):
        try:
            await _invoke_repeated_sql(sql, *parms)
        except UnknownReptException:
            await sql.reptHandle = _register_repeated_sql(sql, parms)
            await  _invoke_repeated_sql(sql, parms)
        if expect_rows:
            result_set = await _receive_result_rows()
        else:
            result_set = None
        await _complete_sql()
        return result_set


    async def _execute_repeated_select(sql, *parms):
        rows = await _execute_repeated_sql(sql, *parms, expect_rows=True)
        _log_rowcount(rows)
        return rows

    
    async def _execute_repeated_non_select():
        ...

    
    execute_repeated_select = _execute_repeated_select
    execute_repeated_insert = _execute_repeated_non_select
    execute_repeated_update = _execute_repeated_non_select
    execute_repeated_delete = _execute_repeated_non_select


#    async def prepare_query(self, query):
#        qyp = py.IIAPI_QUERYPARM()
#        queryText = query.encode()
#        qyp.qy_connHandle = self.connHandle
#        qyp.qy_queryType = py.IIAPI_QT_QUERY
#        qyp.qy_queryText = queryText
#        qyp.qy_parameters = False
#        qyp.qy_tranHandle = self.tranHandle
#
#        await py.IIapi_query(qyp)
#
#        self.stmtHandle = qyp.qy_stmtHandle
#        self.tranHandle = qyp.qy_tranHandle
#
#        gqp = py.IIAPI_GETQINFOPARM()               
#        gqp.gq_stmtHandle = self.stmtHandle
#
#        await py.IIapi_getQueryInfo(gqp)                  
#
#        clp = py.IIAPI_CLOSEPARM()                  
#        clp.cl_stmtHandle = self.stmtHandle
#
#        await py.IIapi_close(clp)
#
#
#    async def exec_prepared_SELECT(self, query_name, *args):
#
#        nargs = len(args)
#        rows = []
#
#        queryText = query_name.encode()
#        qyp = py.IIAPI_QUERYPARM()
#        qyp.qy_connHandle = self.connHandle
#        qyp.qy_queryType = py.IIAPI_QT_OPEN
#        qyp.qy_queryText = queryText
#        qyp.qy_parameters = True
#        qyp.qy_tranHandle = self.tranHandle
#
#        await py.IIapi_query(qyp)
#
#        self.stmtHandle = qyp.qy_stmtHandle
#        self.tranHandle = qyp.qy_tranHandle
#        
#        ##  assume the arguments map to the SQL placeholders left-to-right
#        descriptor = (py.IIAPI_DESCRIPTOR * nargs)()
#        for i in range(nargs):
#            descriptor[i].ds_dataType = py.IIAPI_INT_TYPE
#            descriptor[i].ds_nullable = False
#            descriptor[i].ds_length = ctypes.sizeof(ctypes.c_int)
#            descriptor[i].ds_precision = 0
#            descriptor[i].ds_scale = 0
#            descriptor[i].ds_columnType = py.IIAPI_COL_QPARM
#            descriptor[i].ds_columnName = None
#
#        sdp = py.IIAPI_SETDESCRPARM()
#        sdp.sd_stmtHandle = self.stmtHandle
#        sdp.sd_descriptorCount = nargs
#        sdp.sd_descriptor = descriptor
#
#        await py.IIapi_setDescriptor(sdp)
#
#        value = []
#        for arg in args:
#            carg = ctypes.c_int(arg)
#            value.append(carg)
#
#        parmData = (py.IIAPI_DATAVALUE * nargs)()
#        for i in range(nargs):
#            parmData[i].dv_null = False
#            parmData[i].dv_length = ctypes.sizeof(ctypes.c_int)
#            parmData[i].dv_value = ctypes.addressof(value[i])
#
#        ppp = py.IIAPI_PUTPARMPARM()
#        ppp.pp_stmtHandle = self.stmtHandle
#        ppp.pp_parmCount = nargs
#        ppp.pp_parmData = parmData
#
#        await py.IIapi_putParms(ppp)
#
#        gdp = py.IIAPI_GETDESCRPARM()
#        gdp.gd_stmtHandle = self.stmtHandle
#
#        await py.IIapi_getDescriptor(gdp)
#
#        ncols = gdp.gd_descriptorCount
#        column_types = [None] * ncols
#        column_nullabilities = [None] * ncols
#        column_lengths = [None] * ncols
#        column_precisions = [None] * ncols
#        column_scales = [None] * ncols
#        column_names = [None] * ncols
#        column_offsets = [0] * ncols
#        column_addresses = [0] * ncols
#        template = ''
#
#        offset = 0
#        for i in range(ncols):
#            descriptor = gdp.gd_descriptor[i]
#            column_types[i] = ingtype = descriptor.ds_dataType
#            column_nullabilities[i] = descriptor.ds_nullable
#            column_lengths[i] = length = descriptor.ds_length
#            column_precisions[i] = descriptor.ds_precision
#            column_scales[i] = descriptor.ds_scale
#            column_names[i] = descriptor.ds_columnName
#            column_offsets[i] = offset
#            offset = offset + _rounded_up_to_4n(length)
#
#            pattern = _pattern_for_ingtype(ingtype,length)
#            template = template + pattern
#
#        buffer_size = struct.calcsize(template)
#        buffer = ctypes.c_buffer(buffer_size)
#        buffer_address = ctypes.addressof(buffer)
#        column_addresses = [buffer_address+offset
#            for offset in column_offsets]
#
#        columnData = (py.IIAPI_DATAVALUE * ncols)()
#        for i in range(ncols):
#            columnData[i].dv_value = column_addresses[i]
#
#        gcp = py.IIAPI_GETCOLPARM()
#        gcp.gc_rowCount = 1
#        gcp.gc_columnCount = ncols
#        gcp.gc_columnData = columnData
#        gcp.gc_stmtHandle = self.stmtHandle
#
#        while True:
#            await py.IIapi_getColumns(gcp)
#
#            if gcp.gc_genParm.gp_status == py.IIAPI_ST_NO_DATA:
#                break
#
#            column_values = struct.unpack(template,buffer)
#            column_values = [_pythonize(value,type)
#                for value,type in zip(column_values,column_types)] 
#            
#            column_names = [name.decode()
#                for name in column_names]
#            row = dict(zip(column_names,column_values))
#            rows.append(row)
#
#        gqp = py.IIAPI_GETQINFOPARM()
#        gqp.gq_stmtHandle = self.stmtHandle
#
#        await py.IIapi_getQueryInfo(gqp)
#
#        #cnp = py.IIAPI_CANCELPARM()
#        #cnp.cn_stmtHandle = self.stmtHandle
#
#        #await py.IIapi_cancel(cnp)
#
#        clp = py.IIAPI_CLOSEPARM()
#        clp.cl_stmtHandle = self.stmtHandle
#
#        await py.IIapi_close(clp)
#
#        return rows


    async def order(self):
        logger.info(f'{self.name} starting work (order)')

        district = TPCC_random(1,CONFIGDISTPERWHSE)
        customer = get_customerID()
        num_items = TPCC_random(5,15)
        logger.info(f'{num_items=}')
        items = []
        supplier_warehouses = []
        order_quantities = []
        poison_pill = -1

        all_local = True
        for _ in range(num_items):
            items.append(get_itemID())
            ##  initially assume supply from home warehouse
            warehouse = self.warehouse
            if TPCC_random(1,100) == 1:
                ##  supply 1% of items from a randomly chosen remote warehouse 
                ##  (unless there is only one warehouse)
                if CONFIGWHSECOUNT > 1:
                    while warehouse == self.warehouse:
                        warehouse = TPCC_random(1,CONFIGWHSECOUNT)
                    all_local = False
            supplier_warehouses.append(warehouse)
            order_quantities.append(TPCC_random(1,10))

        ##  make 1% of transactions roll back by poisoning the last item
        if TPCC_random(1,100) == 1:
            items[-1] = poison_pill

        #  interact with the database
        await self.process_order(warehouse,customer,district,
            all_local,items,supplier_warehouses,order_quantities)


    async def process_order(self,warehouse,customer,district,
            all_local,items,supplier_warehouses,order_quantities):
        logger.info(f'{warehouse=} {customer=} {district=} {all_local=} {items=} {supplier_warehouses=} {order_quantities=}')


#        stmtGetCustWhse = SQL_by_name['stmtGetCustWhse']
#        stmtGetDist = SQL_by_name['stmtGetDist']
#        stmtInsertNewOrder = SQL_by_name['stmtInsertNewOrder']
#
#        if strategy == REPEATED:
#            try:
#                rqh = repeated_query_handles[stmtGetCustWhse]
#            except KeyError:
#
#
#            try:
#                execute_repeated_query(rqh,...)
#            except QueryCacheMiss:
#            ...            
#
#
#        elif strategy == PREPARED:
#            await self.prepare_query(stmtGetCustWhse)
#            await self.prepare_query(stmtGetDist)
#            await self.prepare_query(stmtInsertNewOrder)
#        else:
#            raise RuntimeError
#        
#            rows = await self.exec_prepared_SELECT(
#                'stmtGetCustWhse', warehouse, district, customer) 


        rows = await self.execute(stmtGetCustWhse)
        if rows:
            logger.success(rows[0])
        else:
            logger.error('??')

        row = rows[0]
        discount = row['discount']
        last = row['last']
        credit = row['credit']

        new_order_inserted = False
        while not new_order_inserted:
            rows = await self.exec_prepared_SELECT(
                'stmtGetDist', 
                district, warehouse)
            
            if rows:
                logger.success(row)
            else:
                logger.error('??')

            row = rows[0]
            order = next_o_id = row['next_o_id']
            tax = row['tax']
            

            new_order_inserted = True




        await self.commit()










    async def payment(self):
        logger.info(f'{self.name} starting work (payment)')

        district = TPCC_random(1,CONFIGDISTPERWHSE)
        customer_lastname = None
        customerID = None

        x = TPCC_random(1,100)
        if x <= 85:
            customer_district = district
            customer_warehouse = self.warehouse
        else:
            customer_district = TPCC_random(1,10)
            customer_warehouse = self.warehouse
            if CONFIGWHSECOUNT > 1:
                while customer_warehouse == self.warehouse:
                    customer_warehouse = TPCC_random(1,CONFIGWHSECOUNT)

        y = TPCC_random(1,100)
        if y <= 60:
            customer_lastname = get_lastname() 
        else:
            customerID = get_customerID()

        payment_amount = TPCC_random(100,500000) / 100.

        ##  interact with the database
        await self.process_payment(customer_warehouse, payment_amount, district,
            customer_district, customerID, customer_lastname)


    async def process_payment(self,customer_warehouse, payment_amount, district,
            customer_district, customerID, customer_lastname):
        logger.info(f'{customer_warehouse=} {payment_amount=} {district=} {customer_district=} {customerID=} {customer_lastname=}')
        await asyncio.sleep(0.1)


    async def level(self):
        logger.info(f'{self.name} starting work (level)')
        threshold = TPCC_random(10,20)
        await self.process_level(self.warehouse, self.district, threshold)


    async def process_level(self,warehouse,district,threshold):
        logger.info(f'{warehouse=} {district=} {threshold=}')

        stockGetDistOrderId = '''
        PREPARE stockGetDistOrderId FROM
        SELECT d.next_o_id
        FROM district d
        WHERE d.warehouse = ? 
        AND d.district = ?'''

        stockGetCountStock = '''
        PREPARE stockGetCountStock FROM
        SELECT COUNT(DISTINCT (s.item)) AS stock_count 
        FROM order_line ol, stock s 
        WHERE ol.warehouse = ? 
        AND ol.district = ? 
        AND ol.order < ?  AND ol.order >= ? - 20 
        AND s.warehouse = ? 
        AND s.item = ol.item 
        AND s.quantity < ?'''

        await self.prepare_query(stockGetDistOrderId)
        await self.prepare_query(stockGetCountStock)

        rows = await self.exec_prepared_SELECT(
            'stockGetDistOrderId', 
            warehouse, district) 

        next_o_id = rows[0]['next_o_id']

        rows = await self.exec_prepared_SELECT(
            'stockGetCountStock', 
            warehouse, district, 
            next_o_id, next_o_id,
            warehouse, threshold) 

        logger.success(rows[0]['stock_count'])

        await self.commit()


    async def status(self):
        logger.info(f'{self.name} starting work (status)')

        customerID = None
        customer_lastname = None
        district = TPCC_random(1,10)

        y = TPCC_random(1,100)
        if y <= 60:
            customer_lastname = get_lastname() 
        else:
            customerID = get_customerID()

        await self.process_status(self.warehouse,district,
            customerID,customer_lastname)


    async def process_status(self,warehouse,district,
        customerID,customer_lastname): 
        logger.info(f'{warehouse=} {district=} {customerID=} {customer_lastname=}')

        await asyncio.sleep(0.1)
        return

        ##  get customer balance
        if customer_lastname:
            ##  look up by customer name
            row = exec_prepared_SELECT('ordStatCountCust',
                customer_lastname, district, warehouse).singleton()
            count = row.namecnt
            if count == 0:
                msg = (f'{name}: ' + 
                    f'{last=} not found for {district=} {warehouse=}')
                logger.warning(f'{self.name}')
                return
            middle = (count+1) // 2
            rows = exec('ordStatGetCust',
                last, district, warehouse)
            customer_row = rows[middle]
        elif customerID:
            ##  look up by customer number
            row = exec_prepared_SELECT('ordStatGetCustBal',
                customer, district, warehouse ).singleton()
            if not row:    
                msg = (f'{name}: ' + 
                    f'{customer=} not found for {district=} {warehouse=}')
                logger.warning(f'{self.name}')
                return
            customer_row = row
        else:
            logger.critical(f'no customer identifier given')
            return

        ##  get the customer's most recent order
        pass

        ##  signal transaction ended
        await commits.put('tx ended')


    async def delivery(self):
        logger.info(f'{self.name} starting work (delivery)')

        carrier_id = TPCC_random(1,10)

        await self.process_delivery(self.warehouse, carrier_id)


    async def process_delivery(self,warehouse,carrier_id):
        logger.info(f'{warehouse=} {carrier_id=}')
        await asyncio.sleep(0.1)


    async def worker(self, dbname):

        event_lock = self.event_lock
        ack_event = self.ack_event
        ready_event = self.ready_event
        run_event = self.run_event
        halt_event = self.halt_event
        commits_queue = self.commits_queue
        jobs_queue = self.jobs_queue
        logger.info(f'{self.name=} {event_lock=} {dbname=}')
        
        await self.connect(dbname)
        
        ##  wait until all the other workers are ready; it would be tidier
        ##  if I used asyncio.Barrier()...but that would require Python 3.11+
        async with event_lock:
            logger.info(f'{self.name=} acquired event_lock')
            ready_event.set()
            await ack_event.wait()
            logger.info(f'{self.name=} got handshake')
            ack_event.clear()
        logger.info(f'{self.name=} waiting to run free')
        await run_event.wait()
        logger.info(f'{self.name=} running free!')

        processor_lookup = {
            'order': self.order,
            'payment': self.payment,
            'status': self.status,
            'delivery': self.delivery,
            'level': self.level }
            
        # run until told to stop
        while not halt_event.is_set():
            job = await self.jobs_queue.get()
            processor = processor_lookup[job]
            await processor()
            #await asyncio.sleep(random.randint(0,3))   # <---- REMOVE ME
            logger.info(f'{self.name} finished work')
            await commits_queue.put('tx ended')
            await asyncio.sleep(0)
        await self.disconnect()
        logger.info(f'{self.name=} FINISHED')


async def driver(dbname, jobs_queue, halt_event):
    '''drive the workers by queueing work_items'''
    logger.info('driver() started')

    n_orders = 45
    n_payments = 43
    n_statuses = 4 
    n_deliveries = 4
    n_levels = 4

    orders = ['order' for i in range(n_orders)]
    payments = ['payment' for i in range(n_payments)]
    statuses = ['status' for i in range(n_statuses)] 
    deliveries = ['delivery' for i in range(n_deliveries)]
    levels = ['level' for i in range(n_levels)]
    job_pool = orders + payments + statuses + deliveries + levels
    assert len(job_pool) == 100 # percent

    while not halt_event.is_set():
        if jobs_queue.full():
            await asyncio.sleep(0)
        else:
            selection = random.randint(1,100)-1
            job = job_pool[selection] 
            logger.info(job)
            await jobs_queue.put(job)


async def starter(n_terminals, event_lock, ack_event, ready_event, run_event):
    '''signal start once all terminals are ready'''
    logger.info('starter() started')

    #event_lock = controls.event_lock
    #ack_event = controls.ack_event
    #ready_event = controls.ready_event
    #run_event = controls.run_event
    
    ready_count = 0
    while ready_count < n_terminals:
        await ready_event.wait()
        logger.info('got ready_event')
        ready_count = ready_count + 1
        ready_event.clear()
        logger.info('cleared ready_event')
        ack_event.set()
        logger.info('sent handshake')
    logger.info('signalling free running')
    run_event.set()
    logger.info('FINISHED')


async def tx_counter(tx_limit, commits_queue, halt_event):
    '''count transactions; signal stop if transaction limit is reached'''

    logger.info('tx_counter() started')
    tx_count = 0
    while not halt_event.is_set():
        tx = await commits_queue.get()
        tx_count = tx_count + 1
        logger.info('counted tx')
        if tx_limit and tx_count >= tx_limit:
            logger.info('signalling halt')
            halt_event.set()
    logger.info('FINISHED')


async def timer(time_limit, halt_event):
    '''signal stop when time limit is reached'''

    if time_limit:
        logger.info('time_terminator() started')
        await asyncio.sleep(time_limit)
        logger.info('signalling halt')
        halt_event.set()
    logger.info('FINISHED')


async def workload(dbname, n_teminals, tx_limit, time_limit):
    '''start all the tasks'''

    ##  set up the synchronization primitives
    event_lock = asyncio.Lock()
    run_event = asyncio.Event()
    ready_event = asyncio.Event()
    ack_event = asyncio.Event()
    halt_event = asyncio.Event()
    commits_queue = asyncio.Queue()
    jobs_queue = asyncio.Queue(maxsize=100)

    ##  initialize
    terminals = []
    for n in range(n_terminals):
        name = f'term{n}'
        terminal = Terminal(
            name,
            event_lock,
            run_event,
            ready_event, 
            ack_event,
            halt_event,
            commits_queue,
            jobs_queue) 
        terminals.append(terminal)
   
    ##  absent any specified halting condition run for 30 seconds
    if not (tx_limit or time_limit):
        time_limit = 30

    tasks = []
    tasks.append(driver(dbname, jobs_queue, halt_event))
    tasks.append(starter(n_terminals, event_lock,
        ack_event, ready_event, run_event))
    tasks.append(tx_counter(tx_limit,  commits_queue,  halt_event))
    tasks.append(timer(time_limit,  halt_event))
    tasks = tasks + [t.worker(dbname) for t in terminals]
    await asyncio.gather(*tasks)
   

##  run the benchmark (such as it is)

import argparse
parser = argparse.ArgumentParser(description='Run some Actian workload.')
parser.add_argument('dbname',
    help = 'target vnode, database, and server class' )
parser.add_argument('-n', type=int,
    help = 'number (<= 10) of warehouse terminals to run' )    
parser.add_argument('-c', type=int,
    help = 'count of transactions to execute' )    
parser.add_argument('-d', type=int,
    help = 'duration of execution in seconds')

args = parser.parse_args()
dbname = args.dbname
n_terminals = args.n or 10
tx_limit = args.c
time_limit = args.d
logger.info(f'{dbname=},{n_terminals=},{tx_limit=},{time_limit=}')
asyncio.run(workload(dbname, n_terminals, tx_limit, time_limit))
