#!/usr/bin/env python

##  Copyright (c) 2025 Roy Hann

##  Name: tpyc_c.py
##
##  Description:
##  	Demonstrates the capabilities of pyngres.asyncio to implement
##      multiple concurrent Ingres sessions, running TPC-C **inspired**
##      queries, without the use of multithreading. It also incidentally
##      illustrates the use of repeated queries.
##
##      This IS NOT a benchmark. It is completely incapable of driving
##      an Ingres DBMS to anywhere near its performance limits. It is
##      just an illustration of the use of asyncio with Pyngres.
##
##  Command syntax:
##      usage: tpyc_c.py [-h] [-n N] [-c C] [-d D] [-r | -p] dbname
##
##      dbname    target database, of form [vnode::]dbname[/server_class]
##
##      optional arguments:
##        -h, --help      show help message and exit
##        -n N            number (<= 10) of warehouse terminals to run
##        -c C            count of transactions to execute
##        -d D            duration of execution in seconds
##        -r, --repeated  use REPEATED queries (this is the default)
##        -p, --prepared  use PREPARED queries (NOT YET IMPLEMENTED)
##
##  Notes:
##  1.  tpyc_c.py does not produce any output. Enable logging to see
##      visible activity. On Windows: set LOGURU_LEVEL=INFO.
##      On Linux/Darwin: export LOGURU_LEVEL=INFO.
##  2.  I have made next to no effort to ensure the workload implemented
##      by tpyc_c.py is faithful to the official TPC-C standard. I am
##      content that it just does "stuff".
##  3.  It is a good idea to roll the database forward between runs,
##      unless you particularly want to see lots of "duplicate key on
##      insert" errors, etc.
##  4.  This code just "grew". It is poorly structured, badly annotated,
##      unpythonic in many places, and has provisions for features that
##      I probably won't ever implement (for example, the option to run
##      prepared queries instead of repeated queries). Also it is dog-slow,  
##      and the Loguru logging overhead doesn't help.
## 
##  Good luck...  :-|

import argparse
import asyncio
import ctypes
import random
import struct
import time

from loguru import logger
import pyngres.asyncio as py

import Exceptions
import TPCC_random as tpc
from Query import RepeatedQuery, PreparedQuery
from Connection import Connection
from config import *
from level import Level
from order import Order
from payment import Payment
from status import Status
from delivery import Delivery
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
n_terminals = args.n or 2
tx_limit = args.c
time_limit = args.d
##  absent any specified halting condition, set time_limit
if not (tx_limit or time_limit):
    time_limit = 5

##  define Query according to the supplied flag
if args.repeated:
    query_protocol = 'repeated'
else:
    query_protocol = 'prepared'
logger.info(
    f'{dbname=}, {n_terminals=}, {tx_limit=}, {time_limit=}, '
    f'{args.repeated=}, {args.prepared=}')


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
        logger.debug(f'{self.name=} {event_lock=} {dbname=}')
        
        ##  connect to the database
        session = Connection(self.dbname)
        await session.connect()
        self.session = session
        logger.info(f'started {self.name}: '
            f'warehouse={self.warehouse} district={self.district}')

        ##  set session description
        description = f'\'{self.name} in tpyc_c\''
        sql = 'SET SESSION WITH DESCRIPTION = ' + description
        sql = sql.encode()
        await session.execute(sql)

        ##  specify MVCC as the isolation mechanism
        sql = 'SET LOCKMODE SESSION WHERE LEVEL = MVCC'
        sql = sql.encode()
        await session.execute(sql)

        ##  choose REPEATABLE READ isolation
        sql = 'SET SESSION ISOLATION LEVEL REPEATABLE READ'
        sql = sql.encode()
        await session.execute(sql)
            
        ##  instantiate workload processors
        processor_lookup = {
            'order': Order(self),
            'payment': Payment(self),
            'status': Status(self),
            'delivery': Delivery(self),
            'level': Level(self) }
            
        ##  wait until all the other workers are ready; this would be tidier
        ##  if I used asyncio.Barrier()...but that would require Python 3.11+
        async with event_lock:
            logger.debug(f'{self.name=} acquired event_lock')
            ready_event.set()
            await ack_event.wait()
            logger.debug(f'{self.name=} got handshake')
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
            logger.info(f'{self.name} completed {job}')
            await commits_queue.put('tx ended')
            await asyncio.sleep(0)

        ##  disconnect from the database
        await session.disconnect()
        self.session = None
        logger.info(f'{self.name} DISCONNECTED')


async def driver(dbname, jobs_queue, halt_event):
    '''drive the workers by queueing randomly chosen work_items'''

    logger.debug('driver() started')

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
            await jobs_queue.put(job)
            logger.debug(f'driver added {job} to the work queue')


async def starter(n_terminals, event_lock, ack_event, ready_event, run_event):
    '''signal start once all terminals are ready'''

    logger.debug('starter() started')

    ready_count = 0
    while ready_count < n_terminals:
        await ready_event.wait()
        logger.debug('got ready_event')
        ready_count = ready_count + 1
        ready_event.clear()
        logger.debug('cleared ready_event')
        ack_event.set()
        logger.info('sent handshake')
    logger.debug('signalling free running')
    run_event.set()
    logger.debug('starter COMPLETED')


async def tx_counter(tx_limit, commits_queue, halt_event):
    '''count transactions; signal stop if transaction limit is reached'''

    logger.debug('tx_counter() started')
    tx_count = 0
    while not halt_event.is_set():
        tx = await commits_queue.get()
        tx_count = tx_count + 1
        logger.debug('counted tx')
        if tx_limit and tx_count >= tx_limit:
            logger.info('signalling halt, on tx_limit')
            halt_event.set()
    logger.debug('tx_counter FINISHED')


async def timer(time_limit, halt_event):
    '''signal stop when time limit is reached'''

    if time_limit:
        logger.debug('time_terminator() started')
        await asyncio.sleep(time_limit)
        logger.info('signalling halt, on time_limit')
        halt_event.set()
    logger.debug('timer EXHAUSTED')


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
