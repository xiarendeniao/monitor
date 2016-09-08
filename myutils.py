#encoding=utf-8

import multiprocessing, logging, psutil, signal, sys

def save_pid(pname = None):
    p = multiprocessing.current_process()
    logging.debug('save_pid(%s) p.name %s, p.pid %s' % (pname, p.name, p.pid))
    file('%s.pid' % (pname or p.name,), 'w').write(str(p.pid))

def kill_sub_proc(pid=None):
    if not pid:
        pid = multiprocessing.current_process().pid
    p = psutil.Process(pid)
    if not p: return False
    for cp in p.children():
        logging.info('to kill proc %s' % cp.pid)
        cp.terminate()
        cp.wait()
    return True

def get_children_procs(pid=None):
    if not pid:
        pid = multiprocessing.current_process().pid
    p = psutil.Process(pid)
    if not p: return []
    return [(cp.name(),cp.pid) for cp in p.children()]

'''
[pthread(id=5234, user_time=22.5, system_time=9.2891),
 pthread(id=5235, user_time=0.0, system_time=0.0)]
'''
def get_thread_list(pid=None):
    if not pid:
        pid = multiprocessing.current_process().pid
    p = psutil.Process(pid)
    if not p: return []
    return p.threads()

def get_ip_addr(ifname):
    import socket, fcntl, struct
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

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
            p = multiprocessing.Process(target=func)
            p.daemon = True
            p.start()
            set_signal()
            p.join()
    except BaseException,e:
        logging.error('daemon_start catch exception:%r' % e)
        over()

