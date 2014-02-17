#!/usr/bin/env python
#encoding=utf8
import sys
import traceback
import argparse
import gevent
import time

UNKNOWN = -1
OK = 0
WARNING = 1
CRITICAL = 2

try:
    import redis
except ImportError, e:
    print e
    sys.exit(CRITICAL)


class Redis_Checks:
    def __init__(self):
        self.method_mapping = {
            "connect": "check_connnect",
            "connected_clients": "check_connected_clients",
            "used_memory": "check_used_memory",
            "used_memory_human": "check_used_memory_human",
            "used_memory_rss": "check_used_memory_rss",
            "latency": "check_latency"
            }

    def check_connnect(self, con, host, port, db, warning, critical):
        with gevent.Timeout(30, False):
            if con.ping():
                message = "ping redis host %s: port %s: db %s success!" % (
                    host, str(port), str(db))
                return message, OK
            else:
                message = "ping redis host %s: port %s: db %s fail!" % (
                    host, str(port), str(db))
                return message, CRITICAL
        message = "ping redis host %s: port %s: db %s time out!" % (
            host, str(port), str(db))
        return message, CRITICAL

    def check_connected_clients(self, con, host, port, db, warning, critical):
        self.check_connnect(con, host, port, db)
        result = con.info()
        connected_count = result["connected_clients"]
        message = "current connections is %s !" % connected_count
        if connected_count > critical:
            return message, CRITICAL
        elif connected_count > warning:
            return message, WARNING
        else:
            return message, OK

    def check_used_memory(self, con, host, port, db, warning, critical):
        self.check_connnect(con, host, port, db)
        result = con.info()
        connected_count = result["used_memory"]
        message = "current used memory is %s !" % connected_count
        if connected_count > critical:
            return message, CRITICAL
        elif connected_count > warning:
            return message, WARNING
        else:
            return message, OK

    def check_used_memory_human(self, con, host, port, db, warning, critical):
        self.check_connnect(con, host, port, db)
        result = con.info()
        connected_count = result["used_memory_human"]
        message = "current used memory is %s !" % connected_count
        if connected_count > critical:
            return message, CRITICAL
        elif connected_count > warning:
            return message, WARNING
        else:
            return message, OK

    def check_used_memory_rss(self, con, host, port, db, warning, critical):
        self.check_connnect(con, host, port, db)
        result = con.info()
        connected_count = result["used_memory_rss"]
        message = "current used memory is %s !" % connected_count
        if connected_count > critical:
            return message, CRITICAL
        elif connected_count > warning:
            return message, WARNING
        else:
            return message, OK

    def check_latency(self, con, host, port, db, warning, critical):
        st = time.time()
        for i in range(10000):
            con.ping()
        et = time.time()
        total_time = et - st
        message = "ping 10000 times cost %s seconds!" % total_time
        if total_time > critical:
            return message, CRITICAL
        elif total_time > warning:
            return message, WARNING

    def method(self, name):
        try:
            return getattr(self, name)
        except Exception, e:
            print e, traceback.format_exc()
            return self.defaultmethod(name)

    def defaultmethod(self, name):
        return "There is no method named %s!" % name, CRITICAL


def main():
    parser = argparse.ArgumentParser(
        description='This Nagios plugin checks the health of redis')
    parser.add_argument('-h', action="store", dest="h")
    parser.add_argument('-p', action="store", dest="p", type=int)
    parser.add_argument('-w', action="store", dest="w", type=int)
    parser.add_argument('-c', action="store", dest="c", type=int)
    parser.add_argument('-db', action="store", dest="db", type=int)
    parser.add_argument('-a', action="store", dest="a")
    args = parser.parse_args()
    redis_host = args.h if args.h else "localhost"
    redis_port = args.p if args.p else 6379
    warning = args.w if args.w else 0
    critical = args.c if args.c else 0
    redis_db = args.db if args.db else 0
    action = args.a if args.a else "connect"

    #SET REDIS CONNECTION
    redis_conn = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        socket_timeout=3)
    redistool = Redis_Checks()
    if redistool.check_connnect(redis_conn, redis_host, redis_port, redis_db)\
            == CRITICAL:
        print "Can not connect to redis host %s: port %s: db %s!" % (
            redis_host, str(redis_port), str(redis_db))
        sys.exit(CRITICAL)
    else:
        message, status = redistool.method(redistool.method_mapping[action])(
            redis_conn, redis_host, redis_port, redis_db, warning, critical)
        print message
        sys.exit(status)


if __name__ == "__main__":
    main()
