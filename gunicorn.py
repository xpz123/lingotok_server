import gevent.monkey

gevent.monkey.patch_all()

import multiprocessing
import os

if not os.path.exists('logs'):
    os.mkdir('logs')

debug = True
loglevel = 'debug'
bind = '127.0.0.1:5000'
pidfile = 'logs/gunicorn.pid'
logfile = 'logs/debug.log'
errorlog = 'logs/error.log'
accesslog = 'logs/access.log'

# 启动的进程数
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = 'gunicorn.workers.ggevent.GeventWorker'

x_forwarded_for_header = 'X-FORWARDED-FOR'