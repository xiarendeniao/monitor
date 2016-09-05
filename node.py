#encoding=utf-8 
from threading import Condition, Lock, Thread
from multiprocessing import Process
from multiprocessing.managers import BaseManager
import multiprocessing, Queue, time, sys, pprint, signal, uuid, socket, logging
import subprocess

HOST = '10.4.4.22'
QPORT = 50000
SPORT = 50001
CHOST = '127.0.0.1'
CPORT = 50005
AUTH_KEY = 'a secret'
LOG_PATHS = None #list/tupple
SOCKET_SEP = '\r\n\r\n'
MONITOR_ERR_LOG = 'monitor.err.log'

def save_pid(pname = None):
    p = multiprocessing.current_process()
    logging.debug('save_pid(%s) p.name %s, p.pid %s' % (pname, p.name, p.pid))
    file('%s.pid' % (pname or p.name,), 'w').write(str(p.pid))

def kill_sub_proc(pid=None):
    import psutil
    if not pid:
        pid = multiprocessing.current_process().pid
    p = psutil.Process(pid)
    if not p: return False
    for cp in p.children():
        logging.info('to kill proc %s' % cp.pid)
        cp.terminate()
        cp.join()
    return True

def fetch_children_pids(pid=None):
    import psutil
    if not pid:
        pid = multiprocessing.current_process().pid
    p = psutil.Process(pid)
    if not p: return
    return [(cp.name(),cp.pid) for cp in p.children()]

def get_ip_addr(ifname):
    import socket, fcntl, struct
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

def reg_rpc_client():
    BaseManager.register('get_taskq')
    BaseManager.register('get_ctlq')
    BaseManager.register('push_task')
    BaseManager.register('push_ctl')
    BaseManager.register('get_resq')
    BaseManager.register('get_logq')

def start_queue():
    #data
    logq = Queue.Queue()
    resq = Queue.Queue()
    node2taskq = dict() #when to clean? markbyxds 
    node2ctlq = dict() #when to clean? markbyxds 
    nodelock = Lock()
    #rpc method
    def get_taskq(nodeid):
        if nodeid not in node2taskq:
            nodelock.acquire()
            node2taskq[nodeid] = Queue.Queue()
            nodelock.release()
        #pprint.pprint(node2taskq)
        return node2taskq[nodeid]
    def get_ctlq(nodeid):
        if nodeid not in node2ctlq:
            nodelock.acquire()
            node2ctlq[nodeid] = Queue.Queue()
            nodelock.release()
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
                logging.debug('taskq %s qsize %s' % (nodeid, q.qsize()))
        else:
            q = node2ctlq[nodeid]
            if q:
                q.put(task)
                logging.debug('taskq %s qsize %s' % (nodeid, q.qsize()))
    #rpc register
    BaseManager.register('get_taskq', callable = get_taskq)
    BaseManager.register('get_ctlq', callable = get_ctlq)
    BaseManager.register('push_task', callable = push_task)
    BaseManager.register('push_ctl', callable = push_ctl)
    BaseManager.register('get_logq', callable = lambda:logq)
    BaseManager.register('get_resq', callable = lambda:resq)
    logging.info('listen at %s:%s ..' % (HOST,QPORT))
    #launch
    mgr = BaseManager(address = (HOST,QPORT), authkey = AUTH_KEY)  
    mgr.get_server().serve_forever() #manager run in current proc
  
def start_scheduler():
    reg_rpc_client()
    qmgr = BaseManager(address = (HOST,QPORT), authkey = AUTH_KEY)  
    logging.info('connecting to queue ..')
    qmgr.connect()
    logging.info('connected to queue success')
    def push_task(task, nodeid=None):
        logging.info('pushed task %s' % (pprint.pformat(task), ))
        qmgr.push_task(task, nodeid)
    def push_ctl(ctl, nodeid=None):
        logging.info('pushed ctl %s' % (pprint.pformat(ctl), ))
        qmgr.push_ctl(ctl, nodeid)
    def th_monitorres():
        logging.info('resq thread start')
        resq = qmgr.get_resq()
        while True:
            res = resq.get()
            logging.info('pulled response %s' % (pprint.pformat(res),))
            if 'sh' in res:
                sys.stderr.write('[%s]>>%s\n%s' % (res['nodeid'], res['sh'], res['res']))
        logging.info('resq thread exit')
    def th_console():
        logging.info('console thread start')
        def do_cmd(cmd):
            logging.debug('do_cmd(%s)' % pprint.pformat(cmd))
            if not cmd.strip(): return
            data = [td.strip() for td in cmd.strip().split(' ') if td.strip()]
            logging.debug('do_cmd data:%s' % pprint.pformat(data))
            if data[0] == 'pushtask':
                exec file(data[1],'r').read() in locals()
                push_task(task, len(data)>2 and data[2] or None)
            elif data[0] == 'sh':
                push_task({'id':uuid.uuid1(), 'sh':' '.join(data[1:])})
            elif data[0] == 'ctl':
                assert(data[1] in ('status', 'kill'))
                push_ctl({'id':uuid.uuid1(), 'ctl':data[1]})
            else:
                logging.error('unrecognized cmd:%s' % cmd)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            global CPORT
            try:
                s.bind((CHOST, CPORT))
            except Exception,e:
                logging.error('bind(%s) error:%r' % (CPORT, e))
                CPORT += 1
            else:
                break
        s.listen(1)
        logging.info('console listen at %s:%s ..' % (CHOST,CPORT))
        while True:
            #do_cmd('pushtask t1.py')
            #time.sleep(2)
            #continue
            conn, addr = s.accept()
            logging.info('console: %s connected' % str(addr))
            buff = ''
            while True:
                data = conn.recv(1024)
                if not data: break
                logging.debug('console: socket recv from %s: %s' % (str(addr), data))
                buff += data
                while True:
                    pos = buff.find(SOCKET_SEP)
                    if pos < 0: break
                    cmd = buff[0:pos]
                    try:
                        do_cmd(cmd)
                    except Exception,e:
                        logging.error('invalid cmd "%s": %r' % (cmd, e))
                    buff = buff[pos+len(SOCKET_SEP):]
            logging.info('console: %s closed' % str(addr))
            conn.close()
        s.close()
        logging.info('console thread exit')
    def th_monitorlog():
        logging.info('monitorlog thread start')
        logq = qmgr.get_logq()
        f = file(MONITOR_ERR_LOG,'wa')
        while True:
            msg = logq.get()
            logging.debug('pulled a log msg:%s' % pprint.pformat(msg))
            f.write('%s %s\n' % (msg['node'], msg['logfile']))
            for l in msg['lines']:
                f.write('%s\t%s\n' % l)
            f.write('\n')
            
        logging.info('monitorlog thread exit')
    #threads
    ths = {th_monitorres:None, th_console:None, th_monitorlog:None}
    for f,_ in ths.iteritems():
        th = Thread(target = f)
        th.daemon = True
        th.start()
        ths[f] = th
    while True:
        #push_task({'id':uuid.uuid1()})
        time.sleep(1)
    for _,th in ths.iteritems():
        th.join()


def start_worker():
    nodeid = get_ip_addr('eth0')
    reg_rpc_client()
    qmgr = BaseManager(address = (HOST,QPORT), authkey = AUTH_KEY)
    logging.info('connecting to queue ..')
    qmgr.connect()
    logging.info('connected to queue success')
    taskq = qmgr.get_taskq(nodeid)
    ctlq = qmgr.get_ctlq(nodeid)
    resq = qmgr.get_resq()
    logq = qmgr.get_logq()
    def do_task(task):
        if 'sh' in task:
            taskproc = subprocess.Popen(task['sh'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
            logging.debug('to read from sh child proc..')
            res = taskproc.stdout.read()
            logging.debug('read from sh child proc done..')
            taskproc = None
            return {'id':task['id'], 'sh':task['sh'], 'nodeid':nodeid, 'res':res}
        return {'id':task['id'], 'rt':0}
    def th_task():
        while True:
            curtask = taskq.get()
            logging.info('node %s pulled task (%s left): %s' % (nodeid, taskq.qsize(), pprint.pformat(curtask)))
            res = do_task(curtask)
            resq.put(res)
            logging.info('node %s pushed res (%s left): %s' % (nodeid, resq.qsize(), pprint.pformat(res)))
    def th_monitorlog(paths=LOG_PATHS or ('.',)):
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
    def th_ctl():
        while True:
            ctl = ctlq.get()
            logging.info('node %s pulled ctl (%s left): %s' % (nodeid, ctlq.qsize(), pprint.pformat(ctl)))
            assert(ctl['ctl'] in ('status','kill'))
            if ctl['ctl'] == 'status':
                #pprint.pprint(fetch_children_pids())
                res = {'id':ctl['id'], 'ctl':ctl['ctl'], 'nodeid':nodeid, 'res':{'pids':fetch_children_pids()}}
                resq.put(res)
                logging.info('node %s pushed res (%s left): %s' % (nodeid, resq.qsize(), pprint.pformat(res)))
            elif ctl['ctl'] == 'kill':
                kill_sub_proc()
    ths = {th_monitorlog:None, th_task:None, th_ctl:None}
    for f,_ in ths.iteritems():
        th = Thread(target = f)
        th.daemon = True
        th.start()
        ths[f] = th
    logging.info('waiting for task...')
    while True:
        for f, th in ths.iteritems():
            if th.is_alive(): continue
            logging.error('thread %s is dead' % f)
            th.join()
            th = Thread(target = f)
            th.daemon = True
            th.start()
            ths[f] = th
        time.sleep(1)
    for _, th in ths.iteritems():
        th.join()

def daemon_start(func):
    p = None
    def over():
        p.terminate()
        p.join()
        sys.exit(0)
    def sighandler(signum, frame):  
        logging.info('sighandler %s %s' % (signum, frame))
        over()
    def set_signal():  
        signal.signal(signal.SIGTERM, sighandler)  
        signal.signal(signal.SIGINT, sighandler)  
    def clear_signal():  
        signal.signal(signal.SIGTERM, 0)  
        signal.signal(signal.SIGINT, 0) 
    try:
        while True:
            clear_signal()
            p = Process(target=func)
            p.daemon = True
            p.start()
            set_signal()
            p.join()
    except BaseException,e:
        over()

if __name__ == '__main__':  
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    if len(sys.argv) < 2:
        logging.error('invalid argument.'); sys.exit(0)
    arg1 = sys.argv[1]
    if arg1 == 'queue':
        start_queue()
    if arg1 == 'scheduler':
        daemon_start(start_scheduler)
    elif arg1 == 'worker':
        daemon_start(start_worker)
    else:
        logging.debug('cur process %s' % multiprocessing.current_process().pid)
        assert(False)
