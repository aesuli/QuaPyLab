import logging
import sys

import cherrypy
import quapy
from cherrypy.process.plugins import SignalHandler
from configargparse import ArgParser

from quapylab.db.filedb import FileDB
from quapylab.util import get_quapylab_home
from quapylab.web import QuaPyLab
from quapylab.web.auth import any_of, redirect, logged_in, enable_controller_service


def main():
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    parser = ArgParser()
    parser.add_argument('--name', help='name of the application instance', type=str, default='QuaPyLab')
    parser.add_argument('--host', help='host server address', type=str, default='127.0.0.1')
    parser.add_argument('--port', help='host server port', type=int, default=8080)
    parser.add_argument('--main_app_path', help='server path of the web client app', type=str, default='/')
    parser.add_argument('--data_dir', help='path to the directory with QuaPyLab data', type=str, default=get_quapylab_home())
    parser.add_argument('--svmperf_dir', help='path to SVMPerf executable', type=str, default=get_quapylab_home())
    args = parser.parse_args(sys.argv[1:])

    quapy.environ['SVMPERF_HOME'] = args.svmperf_dir

    with FileDB(args.data_dir) as db,\
            QuaPyLab(args.name, db) as main_app:

        cherrypy.server.socket_host = args.host
        cherrypy.server.socket_port = args.port

        conf_main_app = {
            '/': {
                'tools.sessions.on': True,
                'tools.auth.on': True,
                'tools.auth.require': [any_of(logged_in(), redirect(args.main_app_path + 'login'))],
            },
            '/login': {
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

        cherrypy.engine.start()
        cherrypy.engine.block()

    return 0


if __name__ == "__main__":
    exit(main())
