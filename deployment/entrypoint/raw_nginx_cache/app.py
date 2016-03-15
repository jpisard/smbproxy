from flask import Flask, request

import logging
import os
import shutil


app = Flask(__name__)

from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.options import parse_command_line
from tornado.wsgi import WSGIContainer

from seekscale_commons.flask_utils import json_endpoint
from seekscale_commons.base import sha256sum, create_dir


logger = logging.getLogger(__name__)


LISTEN_PORT = 35968
FILE_CACHE_DIRECTORY = '/home/data/file_cache'


@app.route('/upload', methods=['POST'])
@json_endpoint
def upload():
    ret = {}

    expected_length = int(request.headers.get('X-Seekscale-Payload-Length'))
    expected_shasum = request.headers.get('X-Seekscale-Payload-Shasum')

    uploaded_body_path = request.headers.get('X-FILE')

    if uploaded_body_path is not None:
        uploaded_file_length = os.path.getsize(uploaded_body_path)
        uploaded_file_shasum = sha256sum(uploaded_body_path)

        if uploaded_file_length != expected_length:
            logger.info('Size mismatch (expected %d got %d)' % (expected_length, uploaded_file_length))
            ret['Size mismatch'] = True
        if uploaded_file_shasum != expected_shasum:
            logger.info('Shasum mismatch (expected %s got %s)' % (expected_shasum, uploaded_file_shasum))
            ret['Shasum mismatch'] = True

        if uploaded_file_length == expected_length and uploaded_file_shasum == expected_shasum:
            ret['Size+shasum match'] = True
            directory = os.path.join(FILE_CACHE_DIRECTORY, expected_shasum[0], expected_shasum[1], expected_shasum[2])
            create_dir(directory)
            new_path = os.path.join(directory, expected_shasum)
            shutil.move(uploaded_body_path, new_path)
            os.chmod(new_path, 0644)
            ret['path'] = new_path

    return ret


if __name__ == "__main__":
    parse_command_line()
    wsgi_app = WSGIContainer(app)

    http_server = HTTPServer(wsgi_app)

    http_server.listen(LISTEN_PORT, address='127.0.0.1')

    IOLoop.instance().start()
