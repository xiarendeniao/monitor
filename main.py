#encoding=utf-8 

if __name__ == '__main__':  
    import logging, sys
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    if len(sys.argv) < 2:
        logging.error('invalid argument.'); sys.exit(0)
    arg1 = sys.argv[1]
    if arg1 == 'queue':
        import myqueue
        myqueue.start()
    if arg1 == 'scheduler':
        import scheduler
        scheduler.start()
    elif arg1 == 'worker':
        import worker
        worker.start()
    else:
        import multiprocessing
        logging.debug('cur process %s' % multiprocessing.current_process().pid)
        assert(False)
