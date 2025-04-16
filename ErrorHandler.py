import pyngres as py
from Exceptions import UnknownReptHandle
from loguru import logger


def errorCheck(gpb):
    '''check status and log errors'''

    ##  do nothing unless there is an error
    if not gpb.gp_errorHandle:
        return

    ##  request (all) error details 
    gep = py.IIAPI_GETEINFOPARM()
    gep.ge_errorHandle = gpb.gp_errorHandle
    errors = []
    while True:
        py.IIapi_getErrorInfo( gep )
        if gep.ge_status != py.IIAPI_ST_SUCCESS:
            break
        type = gep.ge_type
        if type == py.IIAPI_GE_ERROR:
            label = 'ERROR'
            errors.append(gep.ge_errorCode)
        elif type == py.IIAPI_GE_WARNING:
            label = 'WARNING'
        elif type == py.IIAPI_GE_MESSAGE:
            label = 'USER MESSAGE'
        else:
            label = f'unknown error type {type=}'
        message = gep.ge_message if gep.ge_message else 'NULL'     
        report = (  f'Error info: {label} '
                    f'{gep.ge_SQLSTATE} {gep.ge_errorCode:#0x} '
                    f'{message}')
        logger.error( report )

    return errors

