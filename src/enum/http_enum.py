from enum import Enum

class HttpMethods(Enum):
    GET     = 'GET'
    POST    = 'POST'
    PUT     = 'PUT'
    DELETE  = 'DELETE'

class HttpResponses(Enum):
    OK                      = 200
    FORBIDDEN               = 403
    METHOD_NOT_ALLOWED      = 405
    INTERNER_SERVER_ERROR   = 500
