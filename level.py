from Executor import Work
import asyncio
import pyngres.asyncio as py
import iitypes as ii
from loguru import logger
import Query as qy
import TPCC_random as tpc

class Level(Work):
    '''Stock level activity'''

    def __init__(self, terminal):
        super().__init__(terminal)

        name = 'stockGetDistOrderId'
        query = (

            'SELECT d.next_o_id '
            'FROM district d '
            'WHERE d.warehouse = ${} = ~V ' 
            'AND d.district = ${} = ~V ')

        self.repeated_stockGetDistOrderId = qy.RepeatedQuery( query, name )

#           self.prepared_stockGetDistOrderId = qy.PreparedQuery(
#               stockGetDistOrderId_text,
#               name = 'stockGetDistOrderId' )


        name = 'stockGetCountStock'
        query= (

            'SELECT COUNT(DISTINCT (s.item)) AS stock_count ' 
            'FROM order_line ol, stock s '
            'WHERE ol.warehouse = ${} = ~V '
            'AND ol.district = ${} = ~V ' 
            'AND ol.order < ${} = ~V AND ol.order >= ${} = ~V - 20 ' 
            'AND s.warehouse = ${} = ~V ' 
            'AND s.item = ol.item '
            'AND s.quantity < ${} = ~V ')

        self.repeated_stockGetCountStock = qy.RepeatedQuery( query, name )

#            self.prepared_stockGetCountStock = qy.PreparedQuery(
#                stockGetCountStock_text,
#                name = 'stockGetCountStock' )


    async def using_repeated(self):
        '''perform stock level processing using repeated queries'''

        logger.info(f'({self.terminal.name}): processing stock level')

        msg = 'no data returned'

        parms = (self.warehouse, self.district)
        result_set = await self._invoke_repeated_sql(
            self.repeated_stockGetDistOrderId, *parms)
        if result_set:
            next_o_id = result_set[0]['next_o_id']
            value = next_o_id.value
            msg = f'next_o_id={value}, ' 

        threshold = ii.Integer(tpc.TPCC_random(10,20))
        parms = (self.warehouse, self.district,
            next_o_id, next_o_id, self.warehouse, threshold)
        result_set = await self._invoke_repeated_sql(
            self.repeated_stockGetCountStock, *parms)
        if result_set:
            stock_count = result_set[0]['stock_count']
            value = stock_count.value
            msg = msg + f'stock_count={value}'

        msg = f'({self.terminal.name}): ' + msg
        logger.success(msg)

        await self.session.commit()
