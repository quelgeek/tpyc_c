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


