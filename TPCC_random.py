import random 

##  alias random.randint to make reference to the TPC-C spec obvious
TPCC_random = random.randint

def TPCC_NU_random(x, min, max):
    '''return a non-uniform random number using the TPC-C algorithm'''

    value = ((TPCC_random(0,x) | TPCC_random(min,max)) + 
        TPCC_random(0,x)) % (max-min+1) + min
    return value
