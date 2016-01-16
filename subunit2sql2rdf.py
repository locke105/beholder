#!/usr/bin/env python2

#
# Copyright (C) 2016 Mathew Odden
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import argparse
import sys

import pymysql.cursors
import rdflib
import yaml

S2S = rdflib.Namespace('#')


def _get_config():
    with open('config.yaml') as config_file:
        data = yaml.safe_load(config_file)

    defaults = {'host': 'localhost',
                'port': 3306,
                'database': 'subunit2sql'}

    # inject overrides from config and return
    defaults.update(data)
    return defaults


def _get_db_uriref():
    return 'mysql://%(host)s:%(port)s/%(database)s' % _get_config()


def _get_connection():
    config = _get_config()
    conn = pymysql.connect(host=config['host'],
                           port=config['port'],
                           user=config['user'],
                           password=config['password'],
                           db=config['database'],
                           cursorclass=pymysql.cursors.DictCursor)
    return conn


def _get_store():
    g = rdflib.ConjunctiveGraph(store='Sleepycat')

    path = './runs.sc'
    rt = g.open(path, create=False)

    if rt == rdflib.store.NO_STORE:
        g.open(path, create=True)

    print 'Found %d items in loaded store.' % len(g)

    return g



def run_to_rdf(run, graph=None):

    runnode = rdflib.URIRef(_get_db_uriref() + '/runs#id=%s' % run['id'])

    if graph is None:
        g = rdflib.Graph()
    else:
        g = graph

    g.bind('', S2S)

    for prop,val in run.iteritems():
        if not prop.startswith('_'):
            g.add((runnode, S2S[prop], rdflib.Literal(val)))

    return g


def dump_all_run_metadata():
    g = _get_store()

    with _get_connection().cursor() as cursor:
        cursor.execute('SELECT * FROM run_metadata')
        for i, md in enumerate(cursor):
            if i % 100 == 0:
                print >> sys.stderr, 'Processing record %d of %d' % (i, cursor.rowcount)

            runnode = rdflib.URIRef(_get_db_uriref() + '/runs#id=%s' % md['run_id'])
            g.add((runnode, S2S[md['key']], rdflib.Literal(md['value'])))

    print 'Found %d items after import.' % len(g)

    g.close()


def dump_run(run):
    return run_to_rdf(run).serialize(format='n3')


def run_is_in_graph(g, run):
    return g.value(predicate=S2S.uuid, object=rdflib.Literal(run.uuid)) is not None


def dump_all_runs():
    big_graph = _get_store()

    with _get_connection().cursor() as cursor:
        cursor.execute('SELECT * FROM runs')
        for i, run in enumerate(cursor):
            if i % 100 == 0:
                print >> sys.stderr, 'Processing record %d of %d' % (i, cursor.rowcount)
            run_to_rdf(run, graph=big_graph)

    print 'Found %d items after import.' % len(big_graph)

    big_graph.close()


def do_query(query):

    g = _get_store()

    res = g.query(query)

    for row in res:
        for item in row:
            print item,
        print

    g.close()


def _parse_args():
    p = argparse.ArgumentParser()

    p.add_argument('command', choices=['import', 'query'])
    p.add_argument('query')

    return p.parse_args()


def main():
    args = _parse_args()

    if args.command == 'import':
        dump_all_runs()
        dump_all_run_metadata()

    if args.command == 'query':
        do_query(args.query)



if __name__=='__main__':
    main()
