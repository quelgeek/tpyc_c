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
        await self.process_payment(customer_warehouse, payment_amount,
            district, customer_district, customerID, customer_lastname)


    async def process_payment(self,customer_warehouse, payment_amount, district,
            customer_district, customerID, customer_lastname):
        logger.info(f'{customer_warehouse=} {payment_amount=} {district=} {customer_district=} {customerID=} {customer_lastname=}')
        await asyncio.sleep(0.1)



