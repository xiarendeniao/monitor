#encoding=utf-8

from multiprocessing.managers import BaseManager
import subprocess, threading, logging, time, pprint
import myutils
import myconfig as config

taskq, ctlq, resq, logq, nodeid = None, None, None, None, None

def do_task(task):
    if 'sh' in task:
        try:
            proc = subprocess.Popen(task['sh'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError, e:
            logging.error("error start subprocess: %s" % str(e))
            return {'id':task['id'], 'sh':task['sh'], 'nodeid':nodeid, 'res':'error:%r'%e, 'rtcode':1}
        while None==proc.poll():
            time.sleep(1)
        proc.wait()
        res = proc.stdout.read() + '\n' + proc.stderr.read()
        rc = proc.returncode
        return {'id':task['id'], 'sh':task['sh'], 'nodeid':nodeid, 'res':res, 'rtcode':rc}
    return {'id':task['id'], 'rt':0}

def th_task():
    while True:
        curtask = taskq.get()
        logging.info('node %s pulled task (%s left): %s' % (nodeid, taskq.qsize(), pprint.pformat(curtask)))
        res = do_task(curtask)
        resq.put(res)
        logging.info('node %s pushed res (%s left): %s' % (nodeid, resq.qsize(), pprint.pformat(res)))

def th_monitorlog(paths=config.LOG_PATHS or ('.',)):
    def report(fname, lns):
        logq.put({'node':nodeid, 'logfile':fname, 'lines':lns})
    logging.info('nonitorlog thread start')
    import monitorlog as ml
    ml.start(paths)
    while True:
        ml.update(report)
        time.sleep(2)
    ml.stop()
    logging.info('nonitorlog thread exit')

def th_monitorsys():
    logging.info('nonitorsys thread exit')
    while True:
        pass
    logging.info('nonitorsys thread exit')

def th_ctl():
    while True:
        ctl = ctlq.get()
        logging.info('node %s pulled ctl (%s left): %s' % (nodeid, ctlq.qsize(), pprint.pformat(ctl)))
        assert(ctl['ctl'] in ('status','kill'))
        if ctl['ctl'] == 'status':
            pids = myutils.get_children_procs()
            pprint.pprint(pids)
            res = {'id':ctl['id'], 'ctl':ctl['ctl'], 'nodeid':nodeid, 'res':{'pids':pids}}
            resq.put(res)
            logging.info('node %s pushed res (%s left): %s' % (nodeid, resq.qsize(), pprint.pformat(res)))
        elif ctl['ctl'] == 'kill':
            myutils.kill_sub_proc()
    
def startfunc():
    #nodeid
    global nodeid
    nodeid = myutils.get_ip_addr('eth0')
    #rpc register
    BaseManager.register('get_taskq')
    BaseManager.register('get_ctlq')
    BaseManager.register('push_task')
    BaseManager.register('push_ctl')
    BaseManager.register('get_resq')
    BaseManager.register('get_logq')
    #connect
    qmgr = BaseManager(address = (config.HOST, config.QPORT), authkey = config.AUTH_KEY)
    logging.info('connecting to queue ..')
    qmgr.connect()
    logging.info('connected to queue success')
    #queeu
    global taskq, ctlq, resq, logq
    taskq = qmgr.get_taskq(nodeid)
    ctlq = qmgr.get_ctlq(nodeid)
    resq = qmgr.get_resq()
    logq = qmgr.get_logq()
    #thread
    ths = {th_monitorlog:None, th_task:None, th_ctl:None}
    for f,_ in ths.iteritems():
        th = threading.Thread(target = f)
        th.daemon = True
        th.start()
        ths[f] = th
    #main
    while True:
        for f, th in ths.iteritems():
            if th.is_alive(): continue
            logging.error('thread %s is dead' % f)
            th.join()
            th = threading.Thread(target = f)
            th.daemon = True
            th.start()
            ths[f] = th
        time.sleep(1)
    for _, th in ths.iteritems():
        th.join()

def start():
    import myutils
    myutils.daemon_start(startfunc)
