#encoding=utf-8 

from multiprocessing import Process
from multiprocessing.managers import BaseManager
import multiprocessing, Queue, time, sys, random, pprint, signal, uuid

HOST = '10.4.4.22'
QPORT = 50000
SPORT = 50001
AUTH_KEY = 'a secret'

taksq, resq = None, None

def reg_rpc():
    BaseManager.register('get_taskq', callable = lambda:taskq)  
    BaseManager.register('get_resq', callable = lambda:resq)  

def start_queue():
    global taskq, resq
    taskq, resq = Queue.Queue(), Queue.Queue()
    reg_rpc()
    print('listen at %s:%s ..' % (HOST,QPORT))
    mgr = BaseManager(address = (HOST,QPORT), authkey = AUTH_KEY)  
    #print('before mgr.start() %s' % multiprocessing.current_process().pid)
    mgr.get_server().serve_forever() #manager run in current proc
    '''
    mgr.start() #start child process to run manager
    print('after mgr.start() %s' % multiprocessing.current_process().pid)
    proxyq = mgr.get_taskq()
    while True:
        print('after mgr.start() %s queue sz %s, proxy q sz %s' % (multiprocessing.current_process().pid, taskq.qsize(), proxyq.qsize()))
        time.sleep(5)
    '''
  
def start_scheduler():
    reg_rpc()
    #mgr = BaseManager(address = (HOST,SPORT), authkey = AUTH_KEY)
    #mgr.start()
    qmgr = BaseManager(address = (HOST,QPORT), authkey = AUTH_KEY)  
    qmgr.connect()
    taskq = qmgr.get_taskq()
    while True:
        tid = uuid.uuid1()
        task = {'id':tid, 'data':{}}
        taskq.put(task)
        print 'put task "%s", %s left in queue' % (task, taskq.qsize())
        time.sleep(random.randrange(5))

def start_worker():
    reg_rpc()
    qmgr = BaseManager(address = (HOST,QPORT), authkey = AUTH_KEY)
    qmgr.connect()
    taskq = qmgr.get_taskq()
    while True:
        task = taskq.get()  
        print 'get task "%s", %s left in queue' % (task, taskq.qsize())  

def save_pid(pname = None):
    p = multiprocessing.current_process()
    #print('save_pid(%s) p.name %s, p.pid %s' % (pname, p.name, p.pid))
    file('%s.pid' % (pname or p.name,), 'w').write(str(p.pid))

def daemon_start(func):
    p = None
    def over():
        p.terminate()
        p.join()
        sys.exit(0)
    def sighandler(signum, frame):  
        print('sighandler %s %s' % (signum, frame))
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
    if len(sys.argv) < 2:
        print('invalid argument.'); sys.exit(0)
    #pprint.pprint(sys.argv)
    arg1 = sys.argv[1]
    if arg1 == 'queue':
        start_queue()
    if arg1 == 'scheduler':
        daemon_start(start_scheduler)
    elif arg1 == 'worker':
        daemon_start(start_worker)
    else:
        #print('cur process %s' % multiprocessing.current_process().pid)
        assert(False)
