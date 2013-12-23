import time
from statsd import StatsClient
from sqlalchemy import create_engine


import argparse


REQUEST="SELECT client_addr, count(*)  FROM pg_stat_activity group by client_addr;"

engine = create_engine('postgresql+psycopg2://postgres@/template1')
statsd = StatsClient()


# Thanks to Dan Sketcher for these queries:
# http://www.dansketcher.com/2013/01/27/monitoring-postgresql-streaming-replication/

MASTER_QUERY = """SELECT
    client_addr,
    sent_offset - (
        replay_offset - (sent_xlog - replay_xlog) * 255 * 16 ^ 6 ) AS byte_lag
FROM (
    SELECT
        client_addr,
        ('x' || lpad(split_part(sent_location,   '/', 1), 8, '0'))::bit(32)::bigint AS sent_xlog,
        ('x' || lpad(split_part(replay_location, '/', 1), 8, '0'))::bit(32)::bigint AS replay_xlog,
        ('x' || lpad(split_part(sent_location,   '/', 2), 8, '0'))::bit(32)::bigint AS sent_offset,
        ('x' || lpad(split_part(replay_location, '/', 2), 8, '0'))::bit(32)::bigint AS replay_offset
    FROM pg_stat_replication
) AS s;"""

SLAVE_QUERY = """SELECT
CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location()
THEN 0
ELSE EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()) END AS log_delay;"""


def update(key_name='master', master=True, slave=False):

    key_base = "postgresql.{}".format(key_name)

    #############
    # Connections
    result = engine.execute(REQUEST).fetchall()
    total = 0

    for index, (ip, counter) in enumerate(result):
        if not ip:
            ip = 'localhost'

        key = '{}.connections.{}'.format(key_base, ip)
        value = int(counter)
        statsd.gauge(key, value)
        total += value

    key = '{}.total_connections'.format(key_base)
    statsd.gauge(key, total)

    #########
    # Streaming monitoring

    if slave:
        key = '{}.log_delay'.format(key_base)
        result = engine.execute(SLAVE_QUERY).fetchall()
        for row in result:
            value = int(row['log_delay'])
        statsd.gauge(key, value)

    elif master:
        result = engine.execute(MASTER_QUERY).fetchall()
        total = len(result)
        for index, (ip, log_delay) in enumerate(result):
            if not ip:
                ip = 'localhost'
            key = '{}.replica_log_delay.{}'.format(key_base, ip)
            value = int(log_delay)
            statsd.gauge(key, value)
        key = '{}.replicas_count'.format(key_base)
        statsd.gauge(key, total)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Monitor postgres')
    parser.add_argument("-n", "--name", type=str, default='master',
                        help="name to use in statsd key")
    parser.add_argument("-m", "--master", action='store_true',
                        help='Monitor a replicate master. The default.')
    parser.add_argument("-s", "--slave", action='store_true',
                        help='Monitor a replicate slave')

    args = parser.parse_args()

    if not args.slave and not args.master:
        args.master = True

    while 1:
        update(key_name=args.name, master=args.master, slave=args.slave)
        time.sleep(1)
