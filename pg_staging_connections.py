import time
from plumbum import local
from statsd import StatsClient



psql = local['psql']
echo = local['echo']

REQUEST="SELECT client_addr, count(*)  FROM pg_stat_activity where client_addr='10.7.1.148'  group by client_addr;"


statsd = StatsClient()


def get_connections():
    output = (echo[REQUEST] | psql[ '-t', '-a', '-F":"', 'template1'])().split('\n')[1:2][0].split('|')

    output = [line.strip() for line in output]
    key = 'postgresql.connections.10.7.1.148'
    value = 0
    if len(output) == 2:
	
	value = int(output[1])

    return (key, value)


if __name__ == "__main__":
    while 1:
        key, value = get_connections()
        print key," => ", value
        statsd.gauge(key, value)
        time.sleep(1)
