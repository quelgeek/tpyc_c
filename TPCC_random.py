import random 

##  alias random.randint to make reference to the TPC-C spec obvious
TPCC_random = random.randint

def TPCC_NU_random(x, min, max):
    '''return a non-uniform random number using the TPC-C algorithm'''

    value = ((TPCC_random(0,x) | TPCC_random(min,max)) + 
        TPCC_random(0,x)) % (max-min+1) + min
    return value


def get_itemID():
    ID = TPCC_NU_random(8191,1,100000)
    return ID


def get_customerID():
    ID = TPCC_NU_random(1023,1,3000)
    return ID


def get_lastname():
    fragment = ['BAR', 'OUGHT', 'ABLE', 'PRI', 'PRES', 'ESE',
        'ANTI', 'CALLY', 'ATION', 'EING']
    num = TPCC_NU_random(255,0,999)
    lastname = fragment[num//100] + fragment[(num//10)%10] + fragment[num%10]
    return lastname
