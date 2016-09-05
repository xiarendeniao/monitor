#encoding=utf-8
import os, re, sys, time, logging, pprint, threading, cPickle
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent

ob = None
lock = threading.Lock() #f2ut
f2ut = dict()
f2info = dict()
RESTR = r'ERROR|WARNING|CALL STACK|ASSERT|unexpected symbol'
IGSTR = r'while connecting log center|MAJOR ERROR/WARNING'
STACK_LEN = 5
F2POS_FILE = '.f2pos.bin'
SAVE_INTER_SEC = 40

class LogEventHandler(FileSystemEventHandler):
    def on_modified(self, ev):
        if not isinstance(ev, FileModifiedEvent): return
        fname = ev.src_path
        if fname.endswith('.log'):
            lock.acquire(True)
            f2ut[fname] = time.time()
            lock.release()
        #print('on_modified file: %s, thread %s, f2ut %s' % (ev.src_path, threading.currentThread(), pprint.pformat(f2ut)))

def start(paths):
    global ob, f2info
    assert(not ob)
    event_handler = LogEventHandler()
    ob = Observer()
    for path in paths:
        ob.schedule(event_handler, path, recursive=False)
    ob.start()
    if os.path.isfile(F2POS_FILE):
        try:
            f2pos = cPickle.load(file(F2POS_FILE,'rb'))
        except Exception,e:
            logging.error('load f2pos %s failed:%r' % (F2POS_FILE, e))
        else:
            #logging.debug('read from %s: %s' % (F2POS_FILE, pprint.pformat(f2pos)))
            for f,info in f2pos.iteritems():
                st = os.stat(f)
                if st.st_ino != info['inode']: continue
                try:
                    fo = file(f, 'r')
                except Exception,e:
                    logging.error('open file %s failed:%r' % (f,e))
                else:
                    fo.seek(info['pos'])
                    f2info[f] = {'fo':fo, 'pos':fo.tell(), 'ln':info['pos'], 'inode':info['inode']}
            #logging.debug('restored from %s: %s' % (F2POS_FILE, pprint.pformat(f2info)))

last_save_t = 0
def update(reportFunc):
    #print('main: %s, f2ut %s' % (threading.currentThread(), pprint.pformat(f2ut)))
    global f2ut, last_save_t
    lock.acquire(True)
    for f, ut in f2ut.iteritems():
        #posix.stat_result(st_mode=33188, st_ino=21103778, st_dev=2051L, st_nlink=1, st_uid=0, 
        #st_gid=0, st_size=2, st_atime=1472559215, st_mtime=1472572514, st_ctime=1472572514)
        st = os.stat(f) 
        if (f not in f2info) or (f2info[f]['inode']!=st.st_ino):
            logging.info('open file %s (inode %d)' % (f,st.st_ino))
            f2info[f] = {'fo':file(f,'r'), 'pos':0, 'ln':0, 'inode':st.st_ino}
        inf = f2info[f]
        if inf['pos'] > st.st_size:
            logging.info('%s has been overwritten' % f)
            inf['pos'], inf['ln'] = 0, 0
        if inf['pos'] == st.st_size: continue
        def rl():
            line = inf['fo'].readline()
            line = line.strip()
            #logging.debug('%s readline: "%s"' % (f, line))
            if line: inf['ln'] += 1
            return line
        while True:
            line = rl()
            if not line: break
            rert = re.findall(RESTR, line, flags=re.IGNORECASE)
            if rert:
                igrt = re.findall(IGSTR, line)
                if not igrt:
                    logging.debug('detected sensitive log: "%s"' % (line,))
                    report = [(inf['ln'],line),]
                    for _ in range(STACK_LEN):
                        tl = rl()
                        if not tl: break
                        report.append((inf['ln'],tl))
                    reportFunc(f, report)
        inf['pos'] = inf['fo'].tell()
    f2ut = dict() #效率问题 markbyxds 
    lock.release()
    if True or f2info: #markbyxds
        now = time.time()
        if now - last_save_t > SAVE_INTER_SEC:
            f2pos = dict((fname,{'pos':inf['pos'], 'ln':inf['ln'], 'inode':inf['inode']}) for fname,inf in f2info.iteritems())
            cPickle.dump(f2pos, file(F2POS_FILE, 'wb'))
            last_save_t = now
            #logging.debug('save f2pos into %s: %s' % (F2POS_FILE, pprint.pformat(f2pos)))

def stop():
    global ob
    ob.stop()
    ob.join()

def reportFunc(fname, lines):
    #logging.debug('reportFunc "%s":\n%s' % (fname, pprint.pformat(lines)))
    print('%s:\n___________' % fname)
    for l in lines:
        print('%s %s' % l) 
    print('')

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    start(('/tmp/','/root/xds/eu-en/'))
    try:
        while True:
            update(reportFunc)
            time.sleep(1)
    except KeyboardInterrupt:
        stop()
