#encoding=utf-8 

#queue listening
#HOST = '10.4.4.22'
HOST = '127.0.0.1'
QPORT = 50000
AUTH_KEY = 'a secret'

#schduler console listening
CHOST = '127.0.0.1'
CPORT = 50005 #auto add when failed.

#scheduler console imput separated
SOCKET_SEP = '\r\n\r\n'

#scheduler log  collection
MONITOR_ERR_LOG = 'monitor.err.log'

#worker monitor log paths
LOG_PATHS = None #list/tupple

