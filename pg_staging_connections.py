import time
from statsd import StatsClient
from sqlalchemy import create_engine


REQUEST="SELECT client_addr, count(*)  FROM pg_stat_activity group by client_addr;"

engine = create_engine('postgresql+psycopg2://postgres@/template1')
statsd = StatsClient()


def update():
    result = engine.execute(REQUEST).fetchall()
    print "-"*50
    total = 0

    for index, (ip, counter) in enumerate(result):
    	if not ip:
	    ip = 'localhost'
	
	key = 'postgresql.connections.{0}'.format(ip)
	value = int(counter)
    	statsd.gauge(key, value)
        total += value
    
    key = 'postgresql.total_connections'
    statsd.gauge(key, total)     
    





if __name__ == "__main__":
    while 1:
    	update()
        time.sleep(1)
