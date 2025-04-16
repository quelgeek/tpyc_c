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


class Order(Work):
    '''New order activity'''

    def __init__(self,terminal):
        super().__init__(terminal)

        self.POISON_PILL = ii.Integer(-1)

        name='stmtGetCustWhse'
        query = (

            'SELECT c.discount, c.last, c.credit, w.tax '
            'FROM customer c, warehouse w '
            'WHERE w.warehouse = ${} = ~V '
            'AND w.warehouse = c.warehouse '
            'AND c.district = ${} = ~V '
            'AND c.customer = ${} = ~V ' )

        self.repeated_stmtGetCustWhse = qy.RepeatedQuery(query, name )

        name = 'stmtGetDist'
        query = (

            'SELECT d.next_o_id, float4(d.tax) as tax '
            'FROM district d '
            'WHERE d.district = ${} = ~V '
            'AND d.warehouse = ${} = ~V ' )

        self.repeated_stmtGetDist = qy.RepeatedQuery( query, name )
            
        name = 'stmtInsertNewOrder'
        query = (

            'INSERT INTO new_order (order, district, warehouse) ' 
            'VALUES ( ${} = ~V , ${} = ~V , ${} = ~V )' )

        self.repeated_stmtInsertNewOrder = qy.RepeatedQuery( query, name )
                
        name = 'stmtUpdateDist'
        query = (

            'UPDATE district d '
            'SET next_o_id = d.next_o_id+1 ' 
            'WHERE d.district = ${} = ~V '
            'AND d.warehouse = ${} = ~V ' )

        self.repeated_stmtUpdateDist = qy.RepeatedQuery( query, name )

        name = 'stmtInsertOrder'
        query = (

            'INSERT INTO order ' 
            '(order, district, warehouse, customer, '
                'entry_d, ol_cnt, all_local) '
            'VALUES (${} = ~V , ${} = ~V , ${} = ~V , ${} = ~V , '
                'CURRENT_TIME, ${} = ~V , ${} = ~V )' )            

        self.repeated_stmtInsertOrder = qy.RepeatedQuery( query, name )

        name = 'stmtGetItem'
        query = (

            'SELECT i.price, i.name, i.data '
            'FROM item i '
            'WHERE i.item = ${} = ~V ' )

        self.repeated_stmtGetItem = qy.RepeatedQuery( query, name )

        name = 'stmtGetStock'
        query = (

            'SELECT s.quantity, s.data, s.dist_01, s.dist_02, s.dist_03, '
            's.dist_04, s.dist_05,  s.dist_06, s.dist_07, s.dist_08, '
            's.dist_09, s.dist_10 '
            'FROM stock s '
            'WHERE s.item = ${} = ~V '
            'AND s.warehouse = ${} = ~V ' )

        self.repeated_stmtGetStock = qy.RepeatedQuery( query, name )

        name = 'stmtUpdateStock'
        query = (

            'UPDATE stock s ' 
            'SET quantity = ${} = ~V , s.ytd = s.ytd + ${} = ~V , '
            's.remote_cnt = s.remote_cnt + ${} = ~V '
            'WHERE s.item = ${} = ~V '
            'AND s.warehouse = ${} = ~V ' )

        self.repeated_stmtUpdateStock = qy.RepeatedQuery( query, name )

        name = 'stmtInsertOrderLine'
        query = (

            'INSERT INTO order_line (order, district, warehouse, '
               'ol_number, item, supply_warehouse, quantity, amount, '
               'dist_info) '
            'VALUES (${} = ~V ,${} = ~V ,${} = ~V , '
                '${} = ~V , ${} = ~V , ${} = ~V ,${} = ~V ,${} = ~V , '
                '${} = ~V )' )

        self.repeated_stmtInsertOrderLine = qy.RepeatedQuery( query, name )

    
    async def using_repeated(self):
        '''process new order using repeated queries'''

        logger.info(f'({self.terminal.name}): processing new order')

        district = ii.Integer(tpc.TPCC_random(1,CONFIGDISTPERWHSE))
        customer = ii.Integer(tpc.get_customerID())
        num_items = ii.Integer(tpc.TPCC_random(5,15))
        logger.info(f'num_items={num_items.value}')
        items = []
        supplier_warehouses = []
        order_quantities = []

        all_local = ii.Integer(1)
        for _ in range(num_items.value):
            items.append(ii.Integer(tpc.get_itemID()))
            ##  initially assume supply from home warehouse
            warehouse = self.warehouse
            ##  supply 1% of items from a randomly chosen remote warehouse 
            ##  (unless there is only one warehouse)
            if tpc.TPCC_random(1,100) == 1:
                if CONFIGWHSECOUNT > 1:
                    while warehouse.value == self.warehouse.value:
                        random_warehouse = tpc.TPCC_random(1,CONFIGWHSECOUNT)
                        warehouse = ii.Integer(random_warehouse)
                    all_local.value = 0
            supplier_warehouses.append(warehouse)
            random_qty = tpc.TPCC_random(1,10)
            order_quantities.append(ii.Integer(random_qty))

        ##  make 1% of transactions roll back by poisoning the last item
        if tpc.TPCC_random(1,100) == 1:
            items[-1] = self.POISON_PILL

        #  interact with the database
        await self.process_order(warehouse,customer,district, num_items,
            all_local,items,supplier_warehouses,order_quantities)


    async def process_order(self,warehouse,customer,district, num_items,
        all_local,items,supplier_warehouses,order_quantities):

        logger.debug(f'{warehouse=} {customer=} {district=} ' +
            f'{all_local=} {items=} {supplier_warehouses=} ' +
            f'{order_quantities=}')

        parms = (warehouse, district, customer)
        result_set = await self._invoke_repeated_sql(
            self.repeated_stmtGetCustWhse, *parms)
        if not result_set:
            logger.warning('no rows; stmtGetCustWhse ' +
                f'{warehouse.value=} {district.value=} {customer.value=}' )
        else:
            row = result_set[0]
            discount = row['discount']
            last = row['last']
            credit = row['credit']
            w_tax = row['tax']
        
        new_order_inserted = False
        while not new_order_inserted:
            parms = (district, warehouse)
            result_set = await self._invoke_repeated_sql(
                self.repeated_stmtGetDist, *parms )
            if not result_set:
                logger.warning('no rows; stmtGetDist ' +
                    f'{warehouse.value=} {district.value=}' )
            else:
                row = result_set[0]
                order = row['next_o_id']
                d_tax = row['tax']

            parms = (order, district, warehouse)
            try:
                await self._invoke_repeated_sql(
                    self.repeated_stmtInsertNewOrder, *parms )
                new_order_inserted = True
            except DuplicateKey:
                logger.warning('duplicate key; stmtInsertNewOrder, re-trying')
                pass

        parms = (district, warehouse)
        await self._invoke_repeated_sql(
            self.repeated_stmtUpdateDist, *parms )

        parms = (order, district, warehouse, customer, num_items, all_local)
        await self._invoke_repeated_sql(
            self.repeated_stmtInsertOrder, *parms )
         
        prices = []
        names = []
        for i in range(num_items.value):
            supplier_warehouse = supplier_warehouses[i] 
            item_id = items[i]
            order_quantity = order_quantities[i]
            ol_number = ii.Integer(i+1)
        
            if item_id == self.POISON_PILL:
                ##  this is supposed to affect about 1% of orders; rollback
                await self.session.rollback()
                return

            parms = (item_id,)
            result_set = await self._invoke_repeated_sql(
                self.repeated_stmtGetItem, *parms )
            if not result_set:
                logger.warning('no rows, stmtGetItem ' +
                    f'{item_id.value=}' )
            else:
                row = result_set[0]
                price = row['price']
                name = row['name']
                data = row['data']
                prices.append(price)
                names.append(name)

            parms = (item_id, supplier_warehouse)
            result_set = await self._invoke_repeated_sql(
                self.repeated_stmtGetStock, *parms )
            if not result_set:
                logger.warning('no rows, stmtGetStock ' +
                    f'{item_id.value=} {supplier_warehouse.value=}' )
            else:
                row = result_set[0]
                quantity = row['quantity']
                data = row['data']
                dist_01 = row['dist_01']
                dist_02 = row['dist_02']
                dist_03 = row['dist_03']
                dist_04 = row['dist_04']
                dist_05 = row['dist_05']
                dist_06 = row['dist_06']
                dist_07 = row['dist_07']
                dist_08 = row['dist_08']
                dist_09 = row['dist_09']
                dist_10 = row['dist_10']

            if quantity.value - order_quantity.value >= 10:
                quantity.value = quantity.value - order_quantity.value
            else:
                quantity.value = quantity.value - (91 + order_quantity.value)

            if supplier_warehouse.value == self.warehouse.value:
                increment = ii.Integer(0)
            else:
                increment = ii.Integer(1)

            calculated_amount = quantity.value * price.value
            amount = ii.Float(calculated_amount)
                
            parms = (quantity, amount, increment, item_id, supplier_warehouse)
            await self._invoke_repeated_sql(
                self.repeated_stmtUpdateStock, *parms )
                
            index = district.value - 1
            dist_info = [ dist_01, dist_02, dist_03, dist_04, dist_05,
                dist_06, dist_07, dist_08, dist_09, dist_10][index]

            parms = (
                order, district, warehouse,
                ol_number, item_id, supplier_warehouse, order_quantity,
                amount, dist_info)
            await self._invoke_repeated_sql(
                self.repeated_stmtInsertOrderLine, *parms )

        order_ref = f'{warehouse.value}.{district.value}.{order.value} '
        msg = f'inserted {order_ref=} with {num_items.value} items'
        msg = f'({self.terminal.name}): ' + msg
        logger.success(msg)

        await self.session.commit()


