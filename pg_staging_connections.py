import time
from plumbum import local
from statsd import StatsClient
from sqlalchemy import create_engine


REQUEST="SELECT client_addr, count(*)  FROM pg_stat_activity group by client_addr;"

engine = create_engine('postgresql+psycopg2://postgres@/template1')
statsd = StatsClient()


def update2():
    result = engine.execute(REQUEST).fetchall()
    
    for index, (ip, counter) in enumerate(result):
    	if not ip:
	    ip = 'localhost'
	
	key = 'postgresql.connections.{0}'.format(ip)
	value = int(counter)
	print key," => ", value
    	statsd.gauge(key, value)



def update():
    psql = local['psql']
    echo = local['echo']
    output = (echo[REQUEST] | psql['-Ustatsd', '-t', '-a', '-F":"', 'template1'])().split('\n')[1:-2]
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
    	update2()
        time.sleep(1)
