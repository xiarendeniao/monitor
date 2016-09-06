#encoding=utf-8

from multiprocessing.managers import BaseManager
import logging, pprint, threading, time, socket, uuid
import myconfig as config

qmgr = None

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
            import sys
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
            s.bind((config.CHOST, config.CPORT))
        except Exception,e:
            logging.error('bind(%s) error:%r' % (config.CPORT, e))
            config.CPORT += 1
        else:
            break
    s.listen(1)
    logging.info('console listen at %s:%s ..' % (config.CHOST, config.CPORT))
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
                pos = buff.find(config.SOCKET_SEP)
                if pos < 0: break
                cmd = buff[0:pos]
                try:
                    do_cmd(cmd)
                except Exception,e:
                    logging.error('invalid cmd "%s": %r' % (cmd, e))
                buff = buff[pos+len(config.SOCKET_SEP):]
        logging.info('console: %s closed' % str(addr))
        conn.close()
    s.close()
    logging.info('console thread exit')

def th_monitorlog():
    logging.info('monitorlog thread start')
    logq = qmgr.get_logq()
    f = file(config.MONITOR_ERR_LOG,'wa')
    while True:
        msg = logq.get()
        logging.debug('pulled a log msg:%s' % pprint.pformat(msg))
        f.write('%s %s\n' % (msg['node'], msg['logfile']))
        for l in msg['lines']:
            f.write('%s\t%s\n' % l)
        f.write('\n')
        
    logging.info('monitorlog thread exit')

def startfunc():
    #rpc register
    BaseManager.register('get_taskq')
    BaseManager.register('get_ctlq')
    BaseManager.register('push_task')
    BaseManager.register('push_ctl')
    BaseManager.register('get_resq')
    BaseManager.register('get_logq')
    #connect
    global qmgr
    qmgr = BaseManager(address = (config.HOST, config.QPORT), authkey = config.AUTH_KEY)  
    logging.info('connecting to queue ..')
    qmgr.connect()
    logging.info('connected to queue success')
    #threads
    ths = {th_monitorres:None, th_console:None, th_monitorlog:None}
    for f,_ in ths.iteritems():
        th = threading.Thread(target = f)
        th.daemon = True
        th.start()
        ths[f] = th
    while True:
        #push_task({'id':uuid.uuid1()})
        time.sleep(1)
    for _,th in ths.iteritems():
        th.join()

def start():
    import myutils
    myutils.daemon_start(startfunc)
