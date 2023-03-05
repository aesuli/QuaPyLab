import os

import cherrypy
from mako.lookup import TemplateLookup

import quapylab
from quapylab.db.quapydb import QuaPyDB

from quapylab.services.experiments import train, SKIP_NAME
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
                raise cherrypy.HTTPError(401, 'Wrong credentials')
            else:
                if self._db.validate(username, password):
                    cherrypy.session[USER_SESSION_KEY] = cherrypy.request.login = username
                    raise cherrypy.HTTPRedirect('')
                else:
                    raise cherrypy.HTTPError(401, 'Wrong credentials')
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
        dataset_names = kwargs['dataset']
        if type(dataset_names) == str:
            dataset_names = [dataset_names]

        quantifier_names = kwargs['quantifier']
        if type(quantifier_names) == str:
            quantifier_names = [quantifier_names]

        classifier_names = kwargs.get('classifier', [SKIP_NAME])
        if type(classifier_names) == str:
            classifier_names = [classifier_names]

        calibration_names = kwargs.get('calibration', [SKIP_NAME])
        if type(calibration_names) == str:
            calibration_names = [calibration_names]

        metaquantifier_names = kwargs['metaquatifier']
        if type(metaquantifier_names) == str:
            metaquantifier_names = [metaquantifier_names]

        selection_names = kwargs['selection']
        if type(selection_names) == str:
            selection_names = [selection_names]

        protocol_names = kwargs.get('protocol', [SKIP_NAME])
        if type(protocol_names) == str:
            protocol_names = [protocol_names]

        name = kwargs['name']

        overwrite = 'overwrite' in kwargs

        job_count = 0
        for dataset_name in dataset_names:
            for metaquantifier_name in metaquantifier_names:
                for selection_name in selection_names:
                    if selection_name == SKIP_NAME:
                        loop_protocol_names = [SKIP_NAME]
                    else:
                        loop_protocol_names = protocol_names
                    for protocol_name in loop_protocol_names:
                        for quantifier_name in quantifier_names:
                            if quantifier_name == 'MaximumLikelihoodPrevalenceEstimation':
                                loop_classifier_names = [SKIP_NAME]
                                loop_calibration_names = [SKIP_NAME]
                            else:
                                loop_classifier_names = classifier_names
                                loop_calibration_names = calibration_names
                            for classifier_name in loop_classifier_names:
                                for calibration_name in loop_calibration_names:
                                    kwargs = {'name':name, 'dataset_name':dataset_name,
                                              'metaquantifier_name':metaquantifier_name,
                                              'selection_name':selection_name, 'protocol_name':protocol_name,
                                              'quantifier_name':quantifier_name,'classifier_name':classifier_name,
                                              'calibration_name': calibration_name, 'overwrite': overwrite}
                                    self._db.add_job(train,kwargs)
                                    job_count += 1
        return f'Created {job_count} training jobs'

    @cherrypy.expose
    def about(self):
        template = self._lookup.get_template('about.html')
        return template.render(**{**self._template_data, **self.session_data})

    @cherrypy.expose
    def version(self):
        return quapylab.__version__
