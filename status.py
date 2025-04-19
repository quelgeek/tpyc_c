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


class Status(Work):
    '''query status of customer's last order'''

    def __init__(self,terminal):
        super().__init__(terminal)

        name='ordStatCountCust'
        query = (

            'SELECT count(*) AS namecnt '
            'FROM customer c '
            'WHERE c.last = ${} = ~V '
            'AND c.district = ${} = ~V ' 
            'AND c.warehouse = ${} = ~V ' )

        self.repeated_ordStatCountCust = qy.RepeatedQuery(query, name)

        name = 'ordStatGetCust'
        query = (

            'SELECT c.balance, c.first, c.middle, customer '
            'FROM customer c '
            'WHERE c.last = ${} = ~V ' 
            'AND c.district = ${} = ~V ' 
            'AND c.warehouse = ${} = ~V ' 
            'ORDER BY warehouse, district, last, first'  )

        self.repeated_ordStatGetCust = qy.RepeatedQuery(query, name)

        name = 'ordStatGetNewestOrd'
        query = (

            'SELECT MAX(order) AS maxorderid '
            'FROM order o '
            'WHERE o.warehouse = ${} = ~V ' 
            'AND o.district = ${} = ~V ' 
            'AND o.customer = ${} = ~V '  )

        self.repeated_ordStatGetNewestOrd = qy.RepeatedQuery(query, name)

        name = 'ordStatGetCustBal'
        query = (

            'SELECT c.balance, c.first, c.middle, c.last '
            'FROM customer c '
            'WHERE c.customer = ${} = ~V ' 
            'AND c.district = ${} = ~V ' 
            'AND c.warehouse = ${} = ~V '  )

        self.repeated_ordStatGetCustBal = qy.RepeatedQuery(query, name)

        name = 'ordStatGetOrder'
        query = (

            'SELECT o.carrier_id, o.entry_d '
            'FROM order o '
            'WHERE o.warehouse = ${} = ~V ' 
            'AND o.district = ${} = ~V ' 
            'AND o.customer = ${} = ~V ' 
            'AND o.order = ${} = ~V ' )

        self.repeated_ordStatGetOrder = qy.RepeatedQuery(query, name)

        name = 'ordStatGetOrderLines'
        query = (

            'SELECT ol.item, ol.supply_warehouse, ol.quantity, '
            'ol.amount, ol.delivery_d '
            'FROM order_line ol '
            'WHERE ol.order = ${} = ~V ' 
            'AND ol.district =${} = ~V ' 
            'AND ol.warehouse = ${} = ~V ' )
            
        self.repeated_ordStatGetOrderLines = qy.RepeatedQuery(query, name)


    async def using_repeated(self):
        logger.info(f'{self.terminal.name}: processing order-status')

        random_district = tpc.TPCC_random(1,CONFIGDISTPERWHSE)
        customer_district = ii.Integer(random_district)

        customerID = None
        customer_lastname = None
        y = tpc.TPCC_random(1,100)
        if y <= 60:
            random_lastname = tpc.get_lastname() 
            customer_lastname = ii.Varchar(random_lastname) 
        else:
            random_customerID = tpc.get_customerID()
            customerID = ii.Integer(random_customerID)

        await self.process_status(self.warehouse, customer_district,
            customerID, customer_lastname)


    async def process_status(self,warehouse,district,
        customerID,customer_lastname): 
        logger.debug(f'{warehouse=} {district=} ' +
            f'{customerID=} {customer_lastname=}')

        if customer_lastname:
            ##  look up by customer name
            parms = ( customer_lastname, district, warehouse )
            result_set = await self._invoke_repeated_sql(
                self.repeated_ordStatCountCust, *parms )
            if not result_set:
                logger.warning(f'no rows; ordStatCountCust ' +
                    f'{customer_lastname.value=} ' 
                    f'{warehouse.value=} {district.value=}')
            else:
                row = result_set[0]
                namecnt = row['namecnt']

            parms = ( customer_lastname, district, warehouse )
            result_set = await self._invoke_repeated_sql(
                self.repeated_ordStatGetCust, *parms )
            if not result_set:
                logger.warning(f'no rows; ordStatGetCust ' +
                    f'{customer_lastname.value=} ' +
                    f'{warehouse.value=} {district.value=}')
                ##  knock this work item on the head
                await self.session.rollback()
                return
            else:
                ##  choose the middle customer from the sorted list
                middle_customer_index = namecnt.value // 2
                row = result_set[middle_customer_index]
                customer = row['customer']
                first = row['first']
                middle = row['middle']
                balance = row['balance']
        elif customerID:
            ##  look up by customer number
            customer = customerID
            parms = (customer, district, warehouse)
            result_set = await self._invoke_repeated_sql(
                self.repeated_ordStatGetCustBal, *parms )
            if not result_set:
                logger.warning(f'no rows; ordStatGetCustBal ' +
                    f'{customer.value=} {warehouse.value=} {district.value=}')
                ##  knock this work item on the head
                await self.session.rollback()
                return
            else:
                row = result_set[0]
                last = row['last']
                first = row['first']
                middle = row['middle']
                balance = row['balance']

        ##  find the newest order for the customer
        parms = (customer, district, warehouse)
        result_set = await self._invoke_repeated_sql(
            self.repeated_ordStatGetNewestOrd, *parms )
        if result_set:
            row = result_set[0]
            order = row['maxorderid']

            ##  get the carrier and order date for the newest order
            parms = (warehouse,district,customer,order)
            result_set = await self._invoke_repeated_sql(
                self.repeated_ordStatGetOrder, *parms )
            if result_set:
                row = result_set[0]
                carrier = row['carrier_id']
                entry_d = row['entry_d']

            ##  get the order lines for the newest order
            parms = (order,district,warehouse)
            result_set = await self._invoke_repeated_sql(
                self.repeated_ordStatGetOrderLines, *parms )
            for row in result_set:
                item = row['item']
                supply_warehouse = row['supply_warehouse']
                quantity = row['quantity']
                amount = row['amount']
                delivery_d = row['delivery_d']

        msg = f'fetched customer {customer.value} order status'
        msg = f'({self.terminal.name}): ' + msg
        logger.success(msg)

        await self.session.commit()
