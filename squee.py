#!/usr/bin/env python

from flask import Flask,Response,request
from os.path import join,isfile
from argparse import ArgumentParser

app = Flask(__name__)
#app.debug = True

basedir, done_file, queue_file, queue, queue_set, pending, done = None, None, None, None, None, None, None

def reload():
    global queue, done_file, queue_file, queue_set, pending, done
    queue, done_file, queue_file, queue_set, pending, done = [], join(basedir, 'done.txt'), join(basedir, 'queue.txt'), set(), set(), set()

    # 1. load done tokens
    if not isfile(done_file):
        open(done_file, 'w').close()

    with open(done_file) as f:
        for id in f:
            done.add(id.strip())

    # 2. load queued tokens
    if not isfile(queue_file):
        open(queue_file, 'w').close()

    with open(queue_file) as f:
        for id in f:
            id = id.strip()

            if id not in done:
                queue.append(id)
                queue_set.add(id)
    

@app.route('/add')
def add():
    id = request.args.get('id', '')

    if id == '':
        return Response('Bad request', 400)
    elif id in done:
        return Response('Already processed', 400)
    elif id in pending:
        return Response('Currently processing', 400)
    elif id in queue_set:
        return Response('Already in queue', 400)
    else:
        queue.append(id)
        queue_set.add(id)

        with open(queue_file, 'a') as f:
            f.write(id + '\n')

        return Response('Accepted', 202)


@app.route('/get')
def get():
    if len(queue) != 0:
        id = queue.pop(0)
        pending.add(id)

        return id
    else:
        return Response('Queue empty', 404)


@app.route('/finish')
def finish():
    id = request.args.get('id', '')

    if id == '':
        return Response('Bad request - empty id parameter', 400)
    elif id not in pending:
        return Response('Bad request - not pending', 400)
    else:
        pending.remove(id)
        done.add(id)

        with open(done_file, 'a') as f:
            f.write(id + '\n')

        return 'OK'


@app.route('/status')
def status():
    return 'queue: %s\npending: %s\ndone: %s\n' % (str(queue), str(pending), str(done))


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-b', '--basedir', required=True)
    parser.add_argument('-p', '--port', default=8080)
    args = vars(parser.parse_args())
    basedir = args['basedir']

    reload()
    app.run(host='0.0.0.0', port=args['port'])

