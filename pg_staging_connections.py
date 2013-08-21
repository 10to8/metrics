import time
from plumbum import local
from statsd import StatsClient



psql = local['psql']
echo = local['echo']

REQUEST="SELECT client_addr, count(*)  FROM pg_stat_activity group by client_addr;"


statsd = StatsClient()


def update():
    output = (echo[REQUEST] | psql[ '-t', '-a', '-F":"', 'template1'])().split('\n')[1:-2]
    output = [[value.strip() for value in line.split('|')] for line in output]
    for index, (ip, counter) in enumerate(output):
    	if not ip:
	    ip = 'localhost'
	
	key = 'postgresql.connections.{0}'.format(ip)
	value = int(counter)
	print key," => ", value
    	statsd.gauge(key, value)


if __name__ == "__main__":
    while 1:
	update()
    	time.sleep(1)
