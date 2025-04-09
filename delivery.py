    async def delivery(self):
        logger.info(f'{self.name} starting work (delivery)')

        carrier_id = TPCC_random(1,10)

        await self.process_delivery(self.warehouse, carrier_id)


    async def process_delivery(self,warehouse,carrier_id):
        logger.info(f'{warehouse=} {carrier_id=}')
        await asyncio.sleep(0.1)



