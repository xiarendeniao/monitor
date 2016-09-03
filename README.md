# monitor
distributed monitor:  log file , cpu, memory, disk ...

主要功能

    1.监控日志文件

    2.监控系统资源

    3.执行shell

    3.自动更新(执行python)
    
架构

    queue process * 1

    scheduler process * 1

    worker process * n

    基本思想类似于消息队列(如activemq)，只是简洁一些(逻辑简洁、安装也简单些)

    只有广播机制和指定节点推送，丢掉了多节点从同一队列消耗数据的机制

INSTALL

    easy_install watchdog

BUG & TODO:

    1.sh top. 卡死，需要一种kill机制，杀死worker执行不完的任务

    2.监控系统资源，自动更新

    3.稳定性
