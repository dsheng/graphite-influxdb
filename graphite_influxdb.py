import re
import structlog

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

from influxdb import InfluxDBClient

logger = structlog.get_logger()


def config_to_client(config=None):
    if config is not None:
        cfg = config.get('influxdb', {})
        host = cfg.get('host', 'localhost')
        port = cfg.get('port', 8086)
        user = cfg.get('user', 'graphite')
        passw = cfg.get('pass', 'graphite')
        db = cfg.get('db', 'graphite')
        # TODO: May be set to '.' or removed entirely, if caching for finder is implemented
        leafvaluedelimiter = cfg.get('leafvaluedelimiter', '._field_')
    else:
        from django.conf import settings
        host = getattr(settings, 'INFLUXDB_HOST', 'localhost')
        port = getattr(settings, 'INFLUXDB_PORT', 8086)
        user = getattr(settings, 'INFLUXDB_USER', 'graphite')
        passw = getattr(settings, 'INFLUXDB_PASS', 'graphite')
        db = getattr(settings, 'INFLUXDB_DB', 'graphite')
        # TODO: May be set to '.' or removed entirely, if caching for finder is implemented
        leafvaluedelimiter = getattr(settings, 'INFLUXDB_LEADVALUEDELIMITER', '._field_')

    return (InfluxDBClient(host, port, user, passw, db), leafvaluedelimiter)


class InfluxdbReader(object):
    __slots__ = ('client', 'path', 'value')

    def __init__(self, client, path, value):
        self.client = client
        self.path = path
        self.value = value

    def fetch(self, start_time, end_time):
        data = self.client.query("select time, %s from %s where time > %ds "
                                 "and time < %ds order asc" % (
                                     self.value, self.path, start_time, end_time))
        datapoints = []
        start = 0
        end = 0
        step = 10
        try:
            points = data[0]['points']
            start = points[0][0]
            end = points[-1][0]
            step = points[1][0] - start
            datapoints = [p[2] for p in points]
        except Exception:
            pass
        time_info = start, end, step
        logger.debug("influx REQUESTED RANGE for %s: %d to %d" % (
            self.path, start_time, end_time))
        logger.debug("influx RETURNED  RANGE for %s: %d to %d" % (
            self.path, start, end))
        return time_info, datapoints

    def get_intervals(self):
        last_data = self.client.query("select * from %s limit 1" % self.path)
        first_data = self.client.query("select * from %s limit 1 order asc" %
                                       self.path)
        last = 0
        first = 0
        try:
            last = last_data[0]['points'][0][0]
            first = first_data[0]['points'][0][0]
        except Exception:
            pass
        return IntervalSet([Interval(first, last)])


class InfluxdbFinder(object):
    __slots__ = ('client','leafvaluedelimiter')

    def __init__(self, config=None):
        self.client, self.leafvaluedelimiter = config_to_client(config)

    def find_nodes(self, query):
        # if it is a leaf and the query does not contain a *, return right a way
        # TODO: Implement caching for finder!! May be based on
        # https://github.com/vimeo/graphite-influxdb/pull/3
        if self.leafvaluedelimiter in query.pattern and '*' not in query.pattern:
            yield LeafNode(query.pattern, InfluxdbReader(self.client, query.pattern.rpartition(self.leafvaluedelimiter)[0], query.pattern.rpartition(self.leafvaluedelimiter)[2]))
            return

        # query.pattern is basically regex, though * should become [^\.]+
        # and . \.
        # but list series doesn't support pattern matching/regex yet
        regex = '^{0}$'.format(
            query.pattern.replace('.', '\.').replace('*', '[^\.]+')
        )
        logger.info("searching for nodes", pattern=query.pattern, regex=regex)
        regex = re.compile(regex)
        series = self.client.query("list series")

        hidden_columns = set(['time', 'sequence_number'])
        seen_branches = set()
        for s in series:
            res = self.client.query("select * from %s limit 1" % s['name'])
            for row in res:
                for column in row['columns']:
                    if column not in hidden_columns:
                        name = s['name'] + self.leafvaluedelimiter + column
                        if regex.match(name) is not None:
                            logger.debug("found leaf", name=name)
                            yield LeafNode(name, InfluxdbReader(self.client, name.rpartition(self.leafvaluedelimiter)[0], name.rpartition(self.leafvaluedelimiter)[2]))

            while '.' in name:
                name = name.rsplit('.', 1)[0]
                if name not in seen_branches:
                    seen_branches.add(name)
                    if regex.match(name) is not None:
                        logger.debug("found branch", name=name)
                        yield BranchNode(name)
