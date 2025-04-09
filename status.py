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



