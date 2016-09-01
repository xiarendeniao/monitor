#encoding=utf-8 

from multiprocessing import Process
from multiprocessing.managers import BaseManager
import multiprocessing, Queue, time, sys, random, pprint, signal, uuid

HOST = '10.4.4.22'  
PORT = 50000  
AUTH_KEY = 'a secret'

class QueueManager(BaseManager): pass 

class QueueProc(Process):  
    def __init__(self, *args, **kwargs):  
        self.taskq = Queue.Queue()
        super(QueueProc, self).__init__(*args, **kwargs)
    def run(self):  
        #rpc
        QueueManager.register('get_taskq', callable = lambda:self.taskq)  
        #listen
        manager = QueueManager(address = (HOST,PORT), authkey = AUTH_KEY)  
        server = manager.get_server()  
        try:
            server.serve_forever()  
        except BaseException,e:
            print('%s caught exception:%r' % (self.name, e))
  
class Worker(Process):  
    def run(self):  
        #rpc
        QueueManager.register('get_taskq')  
        #connect
        manager = QueueManager(address = (HOST,PORT), authkey = AUTH_KEY)  
        manager.connect()  
        #fetch task
        self.taskq = manager.get_taskq()  
        try:
            while True:
                task = self.taskq.get()  
                print '%s(%s) get task "%s", %s left in queue' % (self.name, self.pid, task, self.taskq.qsize())  
        except BaseException,e:
            print('%s caught exception:%r' % (self.name, e))
        
class Scheduler(Process):  
    def run(self):  
        #rpc
        QueueManager.register('get_taskq')
        #connect
        manager = QueueManager(address = (HOST,PORT), authkey = AUTH_KEY)  
        manager.connect()  
        #push task
        self.taskq = manager.get_taskq()  
        try:
            while True:  
                tid = uuid.uuid1()
                task = {'id':tid, 'data':{}} 
                self.taskq.put(task)  
                print '%s(%s) put task "%s", %s left in queue' % (self.name, self.pid, task, self.taskq.qsize())  
                #time.sleep(random.randrange(5))
        except BaseException,e:
            print('%s caught exception:%r' % (self.name, e))

def save_pid(pname = None):
    p = multiprocessing.current_process()
    #print('save_pid(%s) p.name %s, p.pid %s' % (pname, p.name, p.pid))
    file('%s.pid' % (pname or p.name,), 'w').write(str(p.pid))

if __name__ == '__main__':  
    if len(sys.argv) < 2:
        print('invalid argument.')
        sys.exit(0)
    arg1 = sys.argv[1]
    if arg1 == 'scheduler':
        #queue proc
        queueProc = QueueProc(name='queueproc')
        queueProc.daemon = True
        queueProc.start()  
        #sched proc
        scheduler = Scheduler(name='schedproc')
        scheduler.daemon = True  
        scheduler.start()
        #main proc
        def over():
            queueProc.terminate()
            queueProc.join()
            scheduler.terminate()
            scheduler.join()
            sys.exit(0)
        def sighandler(signum, frame):  
            print("proc %s terminate signal received: sig(%s)" % (multiprocessing.current_process().name, signum))  
            over()
        save_pid('schedmain')
        signal.signal(signal.SIGTERM, sighandler)  
        signal.signal(signal.SIGINT, sighandler)
        try:
            while True:
                time.sleep(1)
        except BaseException,e:
            print('main proc caught exception: %r' % e)
            over()
    elif arg1 == 'worker':
        #worker
        num = len(sys.argv)>2 and int(sys.argv[2]) or 5
        workers = [Worker(name='worker%s'%i) for i in range(num)]  
        for worker in workers:  
            worker.daemon = True  
            worker.start()
        #main proc
        def over():
            for worker in workers:
                worker.terminate()
                worker.join()
            sys.exit(0)
        def sighandler(signum, frame):  
            print("proc %s terminate signal received: sig(%s)" % (multiprocessing.current_process().name, signum))  
            over()
        save_pid('workermain')
        signal.signal(signal.SIGTERM, sighandler)  
        signal.signal(signal.SIGINT, sighandler)
        try:
            while True:
                for index in range(len(workers)):
                    worker = workers[index]
                    if not worker.is_alive():
                        name = worker.name
                        print('child no alive any more:%s %s' % (name,worker))
                        worker.terminate()
                        worker.join()
                        worker = Worker(name=name)
                        worker.daemon = True
                        worker.start()
                        workers[index] = worker
                time.sleep(1)
        except BaseException,e:
            print('main proc caught exception: %r' % e)
            over()
