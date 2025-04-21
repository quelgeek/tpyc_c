from Executor import Work
import asyncio
import pyngres.asyncio as py
import iitypes as ii
from loguru import logger
import Query as qy
import TPCC_random as tpc
from config import *
from Exceptions import *

ii.publish_envHandle()


class Delivery(Work):
    '''delivery activity'''

    def __init__(self,terminal):
        super().__init__(terminal)
    
        name = 'delivGetOrderId'
        query = (
            'SELECT FIRST 1 order '
            'FROM new_order no '
            'WHERE no.district = ${} = ~V ' 
            'AND no.warehouse = ${} = ~V ' 
            'ORDER BY order ASC' )

        self.repeated_delivGetOrderId = qy.RepeatedQuery(query, name)
    
        name = 'delivDeleteNewOrder'
        query = (
            'DELETE FROM new_order no '
            'WHERE no.district = ${} = ~V ' 
            'AND no.warehouse = ${} = ~V ' 
            'AND no.order = ${} = ~V ' )

        self.repeated_delivDeleteNewOrder = qy.RepeatedQuery(query, name)
    
        name = 'delivGetCustId'
        query = (
            'SELECT o.customer '
            'FROM order o '
            'WHERE o.order = ${} = ~V ' 
            'AND o.district = ${} = ~V ' 
            'AND o.warehouse = ${} = ~V ' )

        self.repeated_delivGetCustId = qy.RepeatedQuery(query, name)
    
        name = 'delivUpdateCarrierId'
        query = (
            'UPDATE order o '
            'SET carrier_id = ${} = ~V ' 
            'WHERE o.order = ${} = ~V ' 
            'AND o.district = ${} = ~V ' 
            'AND o.warehouse = ${} = ~V ' )

        self.repeated_delivUpdateCarrierId = qy.RepeatedQuery(query, name)
    
        name = 'delivUpdateDeliveryDate'
        query = (
            'UPDATE order_line ol '
            'SET delivery_d = CURRENT_TIME ' 
            'WHERE ol.order = ${} = ~V ' 
            'AND ol.district = ${} = ~V ' 
            'AND ol.warehouse = ${} = ~V ')

        self.repeated_delivUpdateDeliveryDate = qy.RepeatedQuery(query, name)
    
        name = 'delivSumOrderAmount'
        query = (
            'SELECT SUM(amount) AS total '
            'FROM order_line ol '
            'WHERE ol.order = ${} = ~V ' 
            'AND ol.district = ${} = ~V ' 
            'AND ol.warehouse = ${} = ~V ' )

        self.repeated_delivSumOrderAmount = qy.RepeatedQuery(query, name)
    
        name = 'delivUpdateCustBalDelivCnt'
        query = (
            'UPDATE customer c '
            'SET balance = balance+${} = ~V , delivery_cnt = delivery_cnt+1 '
            'WHERE c.customer = ${} = ~V ' 
            'AND c.district = ${} = ~V ' 
            'AND c.warehouse = ${} = ~V ' )

        self.repeated_delivUpdateCustBalDelivCnt = qy.RepeatedQuery(query, name)


    async def using_repeated(self):

        logger.info(f'{self.terminal.name} processing delivery')

        random_carrier = tpc.TPCC_random(1,10)
        carrier = ii.Integer(random_carrier)

        await self.process_delivery(self.warehouse, carrier)


    async def process_delivery(self, warehouse, carrier):
    
        logger.info(f'{warehouse=} {carrier=}')

        district = ii.Integer(0)
        for next_district in range(CONFIGDISTPERWHSE):
            district.value = next_district+1
            parms = (district, warehouse)
            result_set = await self._invoke_repeated_sql(
                self.repeated_delivGetOrderId, *parms)
            if not result_set:
                logger.warning('no rows; delivGetOrderId ' +
                    f'{warehouse.value=} {district.value=}' )
            else:
                row = result_set[0]
                order = row['order']

            parms = (district, warehouse, order)
            await self._invoke_repeated_sql(
                self.repeated_delivDeleteNewOrder, *parms)

            parms = (order, district, warehouse)                    
            result_set = await self._invoke_repeated_sql(
                self.repeated_delivGetCustId, *parms)
            if not result_set:
                logger.warning('no rows; delivGetCustId ' +
                    f'{order.value=} {warehouse.value=} {district.value=}' )
            else:
                row = result_set[0]
                customer = row['customer']                
                    
            parms = (carrier, order, district, warehouse)
            await self._invoke_repeated_sql(
                self.repeated_delivUpdateCarrierId, *parms)

            parms = (order, district, warehouse)
            await self._invoke_repeated_sql(
                self.repeated_delivUpdateDeliveryDate, *parms)

            parms = (order, district, warehouse)                    
            result_set = await self._invoke_repeated_sql(
                self.repeated_delivSumOrderAmount, *parms)
            if not result_set:
                logger.warning('no rows; delivSumOrderAmount ' +
                    f'{order.value=} {warehouse.value=} {district.value=}' )
            else:
                row = result_set[0]
                total = row['total']                
                    
            parms = (total, customer, district, warehouse)
            await self._invoke_repeated_sql(
                self.repeated_delivUpdateCustBalDelivCnt, *parms)

        order_ref = f'{warehouse.value}.{district.value}.{order.value}'
        msg = f'delivered order {order_ref}'
        msg = f'({self.terminal.name}): ' + msg
        logger.success(msg)

        await self.session.commit()

