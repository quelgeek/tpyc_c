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



