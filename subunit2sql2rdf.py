#!/usr/bin/env python2

import argparse

import rdflib

from subunit2sql.db import api as s_api


class Config(dict):
    backend = 'sqlalchemy'
    connection = 'CHANGEME'

s_api.CONF.database = Config()


S2S = rdflib.Namespace('#')


def run_to_rdf(run, graph=None):

    runnode = rdflib.URIRef('mysql://logstash.openstack.org/subunit2sql/runs#id=%s' % run.id)

    if graph is None:
        g = rdflib.Graph()
    else:
        g = graph

    g.bind('', S2S)

    for prop,val in dict(run).iteritems():
        if not prop.startswith('_'):
            g.add((runnode, S2S[prop], rdflib.Literal(val)))

    # run_metadata
    metadatas = s_api.get_run_metadata(run.uuid)

    for md in metadatas:
        g.add((runnode, S2S[md.key], rdflib.Literal(md.value)))

    return g


def dump_run(run):
    return run_to_rdf(run).serialize(format='n3')


def run_is_in_graph(g, run):
    return g.value(predicate=S2S.uuid, object=rdflib.Literal(run.uuid)) is not None


def dump_all_runs():
    #runs = s_api.get_all_runs()
    sess = s_api.get_session()

    query = s_api.db_utils.model_query(s_api.models.Run, session=sess)
    runs = query.limit(1000).all()

    big_graph = rdflib.Graph()
    for run in runs:
        run_to_rdf(run, graph=big_graph)

    print big_graph.serialize(format='n3')


def _parse_args():
    p = argparse.ArgumentParser()

    p.add_argument('command', choices=['dump-all', 'dump-latest'])

    return p.parse_args()


def main():
    args = _parse_args()

    if args.command == 'dump-latest':
        run = s_api.get_latest_run()
        print dump_run(run)
    elif args.command == 'dump-all':
        print dump_all_runs()


if __name__=='__main__':
    main()
