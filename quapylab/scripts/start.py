import json
import json
import logging
import os
import sys

import cherrypy
import quapy
from cherrypy.process.plugins import SignalHandler
from configargparse import ArgParser

from quapylab.db.filedb import FileDB
from quapylab.services.background_processor import BackgroundProcessor, setup_background_processor_log
from quapylab.util import get_quapylab_home
from quapylab.web import QuaPyLab
from quapylab.web.auth import any_of, redirect, logged_in, enable_controller_service


def jsonify_error(status, message, traceback, version):
    response = cherrypy.response
    response.headers['Content-Type'] = 'application/json'
    return json.dumps({'status': 'Failure', 'status_details': {
        'message': status,
        'description': message,
        'traceback': traceback.split('\n'),
        'version': version
    }})


def main():
    logging.basicConfig(encoding='utf-8', stream=sys.stderr, level=logging.INFO)
    parser = ArgParser()
    parser.add_argument('--name', help='name of the application instance', type=str, default='QuaPyLab')
    parser.add_argument('--host', help='host server address', type=str, default='127.0.0.1')
    parser.add_argument('--port', help='host server port', type=int, default=8080)
    parser.add_argument('--main_app_path', help='server path of the web client app', type=str, default='/')
    parser.add_argument('--data_dir', help='path to the directory with QuaPyLab data', type=str,
                        default=get_quapylab_home())
    parser.add_argument('--svmperf_dir', help='path to SVMPerf executable', type=str, default=get_quapylab_home())
    args = parser.parse_args(sys.argv[1:])

    quapy.environ['SVMPERF_HOME'] = args.svmperf_dir

    with FileDB(args.data_dir) as db, \
            QuaPyLab(args.name, db) as main_app, \
            BackgroundProcessor(args.data_dir, os.cpu_count() // 2, initializer=setup_background_processor_log) as bp:
        cherrypy.server.socket_host = args.host
        cherrypy.server.socket_port = args.port

        conf_main_app = {
            '/': {
                'error_page.default': jsonify_error,
                'tools.sessions.on': True,
                'tools.auth.on': True,
                'tools.auth.require': [any_of(logged_in(), redirect(args.main_app_path + 'login'))],
            },
            '/login': {
                'error_page.default': jsonify_error,
                'tools.auth.require': [],
            },
        }
        cherrypy.tree.mount(main_app, args.main_app_path, config={**main_app.get_config(), **conf_main_app})

        signal_handler = SignalHandler(cherrypy.engine)
        signal_handler.handlers['SIGTERM'] = cherrypy.engine.exit
        signal_handler.handlers['SIGHUP'] = cherrypy.engine.exit
        signal_handler.handlers['SIGQUIT'] = cherrypy.engine.exit
        signal_handler.handlers['SIGINT'] = cherrypy.engine.exit
        signal_handler.subscribe()

        enable_controller_service()

        bp.start()
        cherrypy.engine.subscribe('stop', bp.stop)

        cherrypy.engine.start()
        cherrypy.engine.block()

    return 0


if __name__ == "__main__":
    exit(main())
