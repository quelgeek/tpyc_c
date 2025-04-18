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


class Payment(Work):
    '''payment activity'''

    def __init__(self,terminal):
        super().__init__(terminal)

        name = 'payUpdateWhse'
        query = (
            
            'UPDATE warehouse w '
            'SET ytd = ytd+${} = ~V ' 
            'WHERE w.warehouse = ${} = ~V ' )

        self.repeated_payUpdateWhse = qy.RepeatedQuery(query, name)

        name = 'payGetWhse'
        query = (

            'SELECT w.street_1, w.street_2, w.city, w.state, w.zip, w.name '
            'FROM warehouse w '
            'WHERE w.warehouse = ${} = ~V ' )

        self.repeated_payGetWhse = qy.RepeatedQuery(query, name)

        name = 'payUpdateDist' 
        query = (

            'UPDATE district d '
            'SET ytd = ytd +${} = ~V ' 
            'WHERE d.warehouse = ${} = ~V '
            'AND d.district = ${} = ~V ' )

        self.repeated_payUpdateDist = qy.RepeatedQuery(query, name)

        name = 'payGetDist'  
        query = (

            'SELECT d.street_1, d.street_2, d.city, d.state, d.zip, d.name '
            'FROM district d '
            'WHERE d.warehouse = ${} = ~V '
            'AND d.district = ${} = ~V ' )

        self.repeated_payGetDist = qy.RepeatedQuery(query, name)

        name = 'payCountCust'  
        query = (

            'SELECT count(*) AS namecnt '
            'FROM customer c ' 
            'WHERE c.last = ${} = ~V '
            'AND c.district = ${} = ~V '
            'AND c.warehouse = ${} = ~V ' )

        self.repeated_payCountCust = qy.RepeatedQuery(query, name)

        name = 'payCustByName'
        query = (

            'SELECT c.first, c.middle, c.customer, c.street_1, c.street_2, '
            'c.city, c.state, c.zip, c.phone, c.credit, c.credit_lim, '
            'c.discount, c.balance, c.since ' 
            'FROM customer c '
            'WHERE c.warehouse = ${} = ~V '
            'AND c.district = ${} = ~V '
            'AND c.last = ${} = ~V '
            'ORDER BY c.warehouse, c.district, c.last, c.first' )

        self.repeated_payCustByName = qy.RepeatedQuery(query, name)

        name = 'payGetCust' 
        query = (

            'SELECT c.first, c.middle, c.last, c.street_1, c.street_2, '
            'c.city, c.state, c.zip, c.phone, c.credit, c.credit_lim, '
            'c.discount, c.balance, c.since '
            'FROM customer c '
            'WHERE c.warehouse = ${} = ~V '
            'AND c.district = ${} = ~V ' 
            'AND c.customer = ${} = ~V ' )

        self.repeated_payGetCust = qy.RepeatedQuery(query, name)

        name = 'payGetCustCdata' 
        query = (

            'SELECT c.data '
            'FROM customer c '
            'WHERE c.warehouse = ${} = ~V '
            'AND c.district = ${} = ~V ' 
            'AND c.customer = ${} = ~V ' )

        self.repeated_payGetCustCdata = qy.RepeatedQuery(query, name)

        name = 'payUpdateCustBalCdata' 
        query = (

            'UPDATE customer c '
            'SET balance = ${} = ~V , data = ${} = ~V '  
            'WHERE c.warehouse = ${} = ~V '
            'AND c.district = ${} = ~V ' 
            'AND c.customer = ${} = ~V ' )

        self.repeated_payUpdateCustBalCdata = qy.RepeatedQuery(query, name)

        name = 'payUpdateCustBal'  
        query = (

            'UPDATE customer c '
            'SET balance = ${} = ~V ' 
            'WHERE c.warehouse = ${} = ~V '
            'AND c.district = ${} = ~V ' 
            'AND c.customer = ${} = ~V ' )

        self.repeated_payUpdateCustBal = qy.RepeatedQuery(query, name)

        name = 'payInsertHist' 
        query = (

            'INSERT INTO history (customer_district, customer_warehouse, '
            'customer, district, warehouse, date, amount, data) ' 
            'VALUES (${} = ~V ,${} = ~V ,${} = ~V ,${} = ~V , '
            '${} = ~V ,CURRENT_TIME ,${} = ~V ,${} = ~V )' )
                
        self.repeated_payInsertHist = qy.RepeatedQuery(query, name)


    async def using_repeated(self):
        '''process payment using repeated queries'''

        logger.info(f'({self.terminal.name}): processing payment')

        random_district = tpc.TPCC_random(1,CONFIGDISTPERWHSE)
        customer_district = ii.Integer(random_district)

        x = tpc.TPCC_random(1,100)
        if x <= 85:
            customer_warehouse = self.warehouse
        else:
            customer_warehouse = self.warehouse.value
            if CONFIGWHSECOUNT > 1:
                while customer_warehouse == self.warehouse.value:
                    customer_warehouse = tpc.TPCC_random(1,CONFIGWHSECOUNT)
            customer_warehouse = ii.Integer(customer_warehouse)

        customer_lastname = None
        customerID = None
        y = tpc.TPCC_random(1,100)
        if y <= 60:
            random_lastname = tpc.get_lastname() 
            customer_lastname = ii.Varchar(random_lastname)
        else:
            random_customerID = tpc.get_customerID()
            customerID = ii.Integer(random_customerID)

        random_amount = tpc.TPCC_random(100,500000) / 100.
        payment_amount = ii.Float(random_amount)

        ##  interact with the database
        await self.process_payment(self.warehouse, customer_warehouse,
            payment_amount, self.district, customer_district, customerID,
            customer_lastname)


    async def process_payment(self, warehouse, customer_warehouse, 
        payment_amount, district, customer_district, customerID,
        customer_lastname):

        logger.debug(f'{customer_warehouse=} {payment_amount=} {district=} {customer_district=} {customerID=} {customer_lastname=}')
        await asyncio.sleep(0.1)

        parms = (payment_amount, warehouse)
        await self._invoke_repeated_sql(
            self.repeated_payUpdateWhse, *parms )

        parms = (warehouse,)
        result_set = await self._invoke_repeated_sql(
            self.repeated_payGetWhse, *parms )
        if not result_set:
            logger.warning(f'no rows; payGetWhse {warehouse.value=}')
        else:
            row = result_set[0]
            warehouse_name = row['name']
            street_1 = row['street_1']
            street_2 = row['street_2']
            city = row['city']
            state = row['state']
            zip = row['zip']

        parms = ( payment_amount, warehouse, district )
        await self._invoke_repeated_sql(
            self.repeated_payUpdateDist, *parms )

        parms = ( warehouse, district )
        result_set = await self._invoke_repeated_sql(
            self.repeated_payGetDist, *parms )
        if not result_set:
            logger.warning(f'no rows; payGetDist ' +
                f'{warehouse.value=} {district.value=}')
        else:
            row = result_set[0]
            district_name = row['name']
            street_1 = row['street_1']
            street_2 = row['street_2']
            city = row['city']
            state = row['state']
            zip = row['zip']

        if customer_lastname:
            ##  payment is by customer last name
            parms = ( customer_lastname, district, warehouse )
            result_set = await self._invoke_repeated_sql(
                self.repeated_payCountCust, *parms )
            if not result_set:
                logger.warning(f'no rows; payCountCust ' +
                    f'{customer_lastname.value=} ' +
                    f'{warehouse.value=} {district.value=}')
            else:
                row = result_set[0]
                namecnt = row['namecnt']

            parms = (warehouse, district, customer_lastname)
            result_set = await self._invoke_repeated_sql(
                self.repeated_payCustByName, *parms )
            if not result_set:
                logger.warning(f'no rows; payCustByName ' +
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
                street_1 = row['street_1']
                street_2 = row['street_2']
                city = row['city']
                state = row['state']
                zip = row['zip']
                phone = row['phone']
                credit = row['credit']
                credit_lim = row['credit_lim']
                discount = row['discount']
                balance = row['balance']
                since  = row['since']
        else:
            ##  payment is by customer ID
            customer = customerID
            parms = (warehouse, district, customer )
            result_set = await self._invoke_repeated_sql(
                self.repeated_payGetCust, *parms )
            if not result_set:
                logger.warning(f'no rows; payGetCust ' +
                    f'{warehouse.value=} {district.value=}' +
                    f'{customerID.value=}')
            else:
                row = result_set[0]
                first = row['first']
                middle = row['middle']
                last = row['last']
                street_1 = row['street_1']
                street_2 = row['street_2']
                city = row['city']
                state = row['state']
                zip = row['zip']
                phone = row['phone']
                credit = row['credit']
                credit_lim = row['credit_lim']
                discount = row['discount']
                balance = row['balance']
                since = row['since']

        new_balance = balance.value + payment_amount.value
        balance.value = new_balance

        if credit.value == 'BC':
            ##  bad credit process
            parms = ( customer_warehouse, customer_district, customer )
            result_set = await self._invoke_repeated_sql(
                self.repeated_payGetCustCdata, *parms )
            if not result_set:
                logger.warning(f'no rows; payGetCust ' +
                    f'{customer_warehouse.value=} ' +
                    f'{customer_district.value=} {customer.value=}')
            else:
                row = result_set[0]
                data = row['data']

            new_data = (
                f'{customer.value} {customer_district.value} '
                f'{customer_warehouse.value} {district.value} '
                f'{warehouse.value} {payment_amount.value} |' ) + data.value
            data.value = new_data[:500]

            parms = ( balance, data, 
                customer_warehouse, customer_district, customer )
            await self._invoke_repeated_sql(
                self.repeated_payUpdateCustBalCdata, *parms )
        else:
            ##  good credit process
            parms = ( balance, 
                customer_warehouse, customer_district, customer)
            await self._invoke_repeated_sql(
                self.repeated_payUpdateCustBal, *parms )

        new_data = f'{warehouse_name.value}    {district_name.value}' 
        data = ii.Varchar(new_data)
        parms = ( customer_district, customer_warehouse, customer,
            district, warehouse, payment_amount, data )
        await self._invoke_repeated_sql(
            self.repeated_payInsertHist, *parms )

        msg = f'updated payment with {payment_amount.value}'
        msg = f'({self.terminal.name}): ' + msg
        logger.success(msg)

        await self.session.commit()
