import argparse
import asyncio
import ctypes
from loguru import logger
import pyngres.asyncio as py
import random
import struct
import time
import xxhash

import Exceptions
import TPCC_random as tpc
from Query import RepeatedQuery, PreparedQuery
from Connection import Connection
from config import *
#import order, payment, level, status, delivery
from level import Level
from order import Order
import Executor

parser = argparse.ArgumentParser(description='Run Actian workload.')
parser.add_argument('dbname',
    help = 'target database, of form [vnode::]dbname[/server_class]' )
parser.add_argument('-n', type=int,
    help = 'number (<= 10) of warehouse terminals to run' )    
parser.add_argument('-c', type=int,
    help = 'count of transactions to execute' )    
parser.add_argument('-d', type=int,
    help = 'duration of execution in seconds')
group = parser.add_mutually_exclusive_group()
group.add_argument('-r', '--repeated', action='store_true',
    help = 'use REPEATED queries (this is the default)' )
group.add_argument('-p', '--prepared', action='store_true',
    help = 'use PREPARED queries (not yet implemented)' )

args = parser.parse_args()
dbname = args.dbname
n_terminals = args.n or 10
tx_limit = args.c
time_limit = args.d
##  define Query according to the supplied flag
if args.repeated:
    query_protocol = 'repeated'
else:
    query_protocol = 'prepared'
logger.info(
    f'{dbname=},{n_terminals=},{tx_limit=},{time_limit=},'
    f'{args.repeated=},{args.prepared=}')


def _log_rowcount(rows):
    rowcount = 0 if not rows else len(rows)
    msg = f'({rowcount} rows)'
    logger.success(msg)


class Terminal():

    def __init__(self,
        name, dbname, query_protocol,
        event_lock, run_event, ready_event, ack_event, halt_event,
        commits_queue, jobs_queue):

        self.name = name
        self.dbname = dbname
        self.query_protocol = query_protocol
        self.warehouse = tpc.TPCC_random(1,CONFIGWHSECOUNT)
        self.district = tpc.TPCC_random(1,CONFIGDISTPERWHSE)

        self.event_lock = event_lock 
        self.run_event = run_event 
        self.ready_event = ready_event  
        self.ack_event = ack_event 
        self.halt_event = halt_event 
        self.commits_queue = commits_queue 
        self.jobs_queue = jobs_queue

        self.session = None


    async def task(self):
        '''execute jobs read from the queue'''

        event_lock = self.event_lock
        ack_event = self.ack_event
        ready_event = self.ready_event
        run_event = self.run_event
        halt_event = self.halt_event
        commits_queue = self.commits_queue
        jobs_queue = self.jobs_queue
        logger.info(f'{self.name=} {event_lock=} {dbname=}')
        
        ##  connect to the database
        session = Connection(self.dbname)
        await session.connect()
        self.session = session
        logger.success(
            f'started {self.name} {self.warehouse=} {self.district=}')
        
        processor_lookup = {
            'order': Order(self),
            'payment': Executor.Payment(self),
            'status': Executor.Status(self),
            'delivery': Executor.Delivery(self),
            'level': Level(self) }
            
        ##  wait until all the other workers are ready; this would be tidier
        ##  if I used asyncio.Barrier()...but that would require Python 3.11+
        async with event_lock:
            logger.info(f'{self.name=} acquired event_lock')
            ready_event.set()
            await ack_event.wait()
            logger.info(f'{self.name=} got handshake')
            ack_event.clear()
        logger.info(f'{self.name=} waiting to run free')
        await run_event.wait()
        logger.info(f'{self.name=} running free!')

        ##  run jobs from the queue until told to stop
        while not halt_event.is_set():
            job = await self.jobs_queue.get()
            processor = processor_lookup[job]
            if self.query_protocol == 'repeated':
                await processor.using_repeated()
            else:
                await processor.using_prepared()
            logger.info(f'{self.name} job/tx ENDED')
            await commits_queue.put('tx ended')
            await asyncio.sleep(0)

        ##  disconnect from the database
        await session.disconnect()
        self.session = None
        logger.info(f'{self.name} DISCONNECTED')


async def driver(dbname, jobs_queue, halt_event):
    '''drive the workers by queueing randomly chosen work_items'''

    logger.info('driver() started')

    ##  construct the pool of work items from which to choose
    n_orders = 45
    n_payments = 43
    n_statuses = 4 
    n_deliveries = 4
    n_levels = 4
    orders = ['order'] * n_orders
    payments = ['payment'] * n_payments
    statuses = ['status'] * n_statuses 
    deliveries = ['delivery'] * n_deliveries
    levels = ['level'] * n_levels
    job_pool = orders + payments + statuses + deliveries + levels
    assert len(job_pool) == 100 # percent

    ##  add work to the queue until signalled to halt
    while not halt_event.is_set():
        if jobs_queue.full():
            await asyncio.sleep(0)
        else:
            selection = random.randint(1,100)-1
            job = job_pool[selection] 
            logger.info(job)
            await jobs_queue.put(job)


async def starter(n_terminals, event_lock, ack_event, ready_event, run_event):
    '''signal start once all terminals are ready'''

    logger.info('starter() started')

    #event_lock = controls.event_lock
    #ack_event = controls.ack_event
    #ready_event = controls.ready_event
    #run_event = controls.run_event
    
    ready_count = 0
    while ready_count < n_terminals:
        await ready_event.wait()
        logger.info('got ready_event')
        ready_count = ready_count + 1
        ready_event.clear()
        logger.info('cleared ready_event')
        ack_event.set()
        logger.info('sent handshake')
    logger.info('signalling free running')
    run_event.set()
    logger.info('starter COMPLETED')


async def tx_counter(tx_limit, commits_queue, halt_event):
    '''count transactions; signal stop if transaction limit is reached'''

    logger.info('tx_counter() started')
    tx_count = 0
    while not halt_event.is_set():
        tx = await commits_queue.get()
        tx_count = tx_count + 1
        logger.info('counted tx')
        if tx_limit and tx_count >= tx_limit:
            logger.info('signalling halt')
            halt_event.set()
    logger.info('tx_counter FINISHED')


async def timer(time_limit, halt_event):
    '''signal stop when time limit is reached'''

    if time_limit:
        logger.info('time_terminator() started')
        await asyncio.sleep(time_limit)
        logger.info('signalling halt')
        halt_event.set()
    logger.info('timer EXHAUSTED')


async def workload(dbname, n_teminals, tx_limit, time_limit):
    '''start all the tasks'''

    ##  set up the synchronization primitives
    event_lock = asyncio.Lock()
    run_event = asyncio.Event()
    ready_event = asyncio.Event()
    ack_event = asyncio.Event()
    halt_event = asyncio.Event()
    commits_queue = asyncio.Queue()
    jobs_queue = asyncio.Queue(maxsize=100)

    ##  initialize
    terminals = []
    for n in range(n_terminals):
        name = f'term{n}'
        terminal = Terminal(
            name, dbname, query_protocol,
            event_lock, run_event, ready_event, ack_event, halt_event,
            commits_queue, jobs_queue) 
        terminals.append(terminal)
   
    ##  absent any specified halting condition, run for 30 seconds
    if not (tx_limit or time_limit):
        time_limit = 30

    tasks = []
    tasks.append(driver(dbname, jobs_queue, halt_event))
    tasks.append(starter(n_terminals, event_lock,
        ack_event, ready_event, run_event))
    tasks.append(tx_counter(tx_limit,  commits_queue,  halt_event))
    tasks.append(timer(time_limit,  halt_event))
    tasks = tasks + [terminal.task() for terminal in terminals]
    await asyncio.gather(*tasks)
   

##  run the benchmark (such as it is)

asyncio.run(workload(dbname, n_terminals, tx_limit, time_limit))
