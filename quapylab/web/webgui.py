import os

import cherrypy
from mako.lookup import TemplateLookup

import quapylab
from quapylab.db.quapydb import QuaPyDB

from quapylab.experiments import train, SKIP_NAME
from quapylab.web import media
from quapylab.web.auth import USER_SESSION_KEY


class QuaPyLab:
    def __init__(self, name, db: QuaPyDB):
        self._name = name
        self._db = db
        self._media_dir = media.__path__[0]
        self._template_data = {'name': self._name,
                               'version': self.version(),
                               'db': self._db,
                               'SKIP_NAME': SKIP_NAME,
                               }
        self._lookup = TemplateLookup(os.path.join(self._media_dir, 'template'), input_encoding='utf-8',
                                      output_encoding='utf-8')

    def get_config(self):
        return {
            '/css':
                {'tools.staticdir.on': True,
                 'tools.staticdir.dir': os.path.join(self._media_dir, 'css'),
                 'tools.auth.on': False,
                 },
            '/js':
                {'tools.staticdir.on': True,
                 'tools.staticdir.dir': os.path.join(self._media_dir, 'js'),
                 'tools.auth.on': False,
                 },
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    @property
    def session_data(self):
        return {'username': cherrypy.request.login, 'mount_dir': cherrypy.request.app.script_name}

    @cherrypy.expose
    def login(self, username=None, password=None, error_message=None):
        if username is None:
            username = ""
        else:
            if password is None:
                error_message = 'Wrong credentials'
            else:
                if self._db.validate(username, password):
                    cherrypy.session[USER_SESSION_KEY] = cherrypy.request.login = username
                else:
                    error_message = 'Wrong credentials'
        template = self._lookup.get_template('login.html')
        if error_message is None:
            error_message = ""
        return template.render(
            **{**self._template_data, **self.session_data, **{'username': username, 'msg': error_message}})

    @cherrypy.expose
    def logout(self):
        sess = cherrypy.session
        username = sess.get(USER_SESSION_KEY, None)
        sess[USER_SESSION_KEY] = None
        if username:
            cherrypy.request.login = None
            cherrypy.log('LOGOUT(username="' + username + '")')

    @cherrypy.expose
    def index(self):
        return self.train()

    @cherrypy.expose
    def train(self):
        template = self._lookup.get_template('train.html')
        return template.render(**{**self._template_data, **self.session_data})

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def run_training(self, *args, **kwargs):
        kwargs['db'] = self._db
        output = train(**kwargs)
        return output

    @cherrypy.expose
    def about(self):
        template = self._lookup.get_template('about.html')
        return template.render(**{**self._template_data, **self.session_data})

    @cherrypy.expose
    def version(self):
        return quapylab.__version__
