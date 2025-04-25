# tpyc_c
**tpyc_c.py** is a Python script that demonstrates of the use of **pyngres.asyncio**. It implements a workload similar to the [TPC-C](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf) 
queries, being run by up to 10 sessions/terminals, without using multithreading. It incidentally also illustrates the use of Ingres repeated queries.
> [!NOTE]
> This demo was never really designed. It just "grew". Architecturally and stylistically it is a nightmare. I feel embarrased every time I look at it. (You know what to do...)

## Before You Start: 
You will need the [tpyc_c_db](https://github.com/quelgeek/tpyc_c_db) data. Be sure to see the README for it. 

It doesn't matter what you call the database. For the sake of definiteness I assume you will call it **tpcc_db**.

## Non-Standard Python Package Requirements
You should create an environment within which to run tpyc_c.py. I prefer Miniconda to manage my environments but it is entirely a matter of taste.

Activate your environment then install these three non-standard packages:

```
pip install pyngres
pip install iitypes
pip install xxhash
```

## Running tpyc_c
The tpyc_c.py script uses [Loguru](https://loguru.readthedocs.io/en/stable/) to report its progress (and problems). To get any visible output you need to set the Loguru logging level.
I recommend setting it to INFO, at least.
### On Windows
```
set LOGURU_LEVEL=INFO
```
### On Linux/Darwin
```
export LOGURU_LEVEL=INFO
```
### Getting Help
```
$ python tpyc_c.py -h                                           
usage: tpyc_c.py [-h] [-n N] [-c C] [-d D] [-r | -p] dbname               
                                                                          
Run Actian workload.                                                      
                                                                          
positional arguments:                                                     
  dbname          target database, of form [vnode::]dbname[/server_class] 
                                                                          
optional arguments:                                                       
  -h, --help      show this help message and exit                         
  -n N            number (<= 10) of warehouse terminals to run            
  -c C            count of transactions to execute                        
  -d D            duration of execution in seconds                        
  -r, --repeated  use REPEATED queries (this is the default)              
  -p, --prepared  use PREPARED queries (not yet implemented)
```
### Running a Trial with Defaults, on a Local Database
```
python tpyc_c.py tpcc_db
```
This will run two concurrent sessions for 5 seconds.
### Running a Trial with Specified Number of Terminals and Duration
```
python tpyc_c.py -n 10 -d 60 loathing::tpcc_db
```
This will run 10 concurrent sessions, for one minute, on a database called tpcc_db using a vnode called loathing.
