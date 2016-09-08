#encoding=utf-8

from multiprocessing.managers import BaseManager
import Queue, pprint, logging
import myconfig as config

logq = Queue.Queue()
resq = Queue.Queue()
node2taskq = dict() #when to clean? markbyxds 
node2ctlq = dict() #when to clean? markbyxds 

def get_taskq(nodeid):
    if nodeid not in node2taskq:
        node2taskq[nodeid] = Queue.Queue()
    #pprint.pprint(node2taskq)
    return node2taskq[nodeid]

def get_ctlq(nodeid):
    if nodeid not in node2ctlq:
        node2ctlq[nodeid] = Queue.Queue()
    #pprint.pprint(node2ctlq)
    return node2ctlq[nodeid]

def push_task(task, nodeid=None):
    logging.debug('push_task %s node2taskq:%s' % (pprint.pformat(task), pprint.pformat(node2taskq)))
    if not nodeid:
        for nodeid,q in node2taskq.iteritems():
            q.put(task)
            logging.debug('taskq %s qsize %s' % (nodeid, q.qsize()))
    else:
        q = node2taskq[nodeid]
        if q:
            q.put(task)
            logging.debug('taskq %s qsize %s' % (nodeid, q.qsize()))

def push_ctl(task, nodeid=None):
    logging.debug('push_ctl %s node2ctlq:%s' % (pprint.pformat(task), pprint.pformat(node2ctlq)))
    if not nodeid:
        for nodeid,q in node2ctlq.iteritems():
            q.put(task)
            logging.debug('ctlq %s qsize %s' % (nodeid, q.qsize()))
    else:
        q = node2ctlq[nodeid]
        if q:
            q.put(task)
            logging.debug('ctlq %s qsize %s' % (nodeid, q.qsize()))

def start():
    #rpc register
    BaseManager.register('get_taskq', callable = get_taskq)
    BaseManager.register('get_ctlq', callable = get_ctlq)
    BaseManager.register('push_task', callable = push_task)
    BaseManager.register('push_ctl', callable = push_ctl)
    BaseManager.register('get_logq', callable = lambda:logq)
    BaseManager.register('get_resq', callable = lambda:resq)
    logging.info('listen at %s:%s ..' % (config.HOST,config.QPORT))
    #launch
    mgr = BaseManager(address = (config.HOST, config.QPORT), authkey = config.AUTH_KEY)  
    mgr.get_server().serve_forever() #manager run in current proc
