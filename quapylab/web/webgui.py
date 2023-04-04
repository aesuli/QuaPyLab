import os

import cherrypy
from mako.lookup import TemplateLookup

import quapylab
from quapylab.db.quapydb import QuaPyDB, JobStatus
from quapylab.services.experiments import train_quantifier
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
            '/reports':
                {'tools.staticdir.on': True,
                 'tools.staticdir.dir': self._db.get_report_dir(),
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
    def upload_dataset(self, name, file, overwrite=False):
        if isinstance(overwrite, str):
            if overwrite.lower() == 'false':
                overwrite = False
        self._db.set_dataset_from_file(name, file, overwrite)
        self._db.create_job(train_quantifier, {'name': name, 'overwrite': overwrite})

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_dataset(self, name):
        self._db.delete_dataset(name)
        return 'Ok'

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_dataset_count(self):
        return self._db.get_dataset_count()

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_dataset_list(self, page=None, page_size=20):
        try:
            page = int(page)
        except (ValueError, TypeError):
            page = 0
        try:
            page_size = int(page_size)
        except (ValueError, TypeError):
            page_size = 0
        dataset_names = self._db.get_dataset_names()[page * page_size:(page + 1) * page_size]
        return [self._db.get_dataset_info(dataset_name) for dataset_name in dataset_names]

    @cherrypy.expose
    def report(self, name):
        template = self._lookup.get_template('report.html')
        return template.render(**{**self._template_data, **self.session_data, **{'name': name}})

    @cherrypy.expose
    def jobs(self):
        template = self._lookup.get_template('jobs.html')
        return template.render(**{**self._template_data, **self.session_data})

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_job_count(self):
        return self._db.get_job_count()

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_job_list(self, page=None, page_size=20):
        try:
            page = int(page)
        except (ValueError, TypeError):
            page = 0
        try:
            page_size = int(page_size)
        except (ValueError, TypeError):
            page_size = 0
        job_ids = self._db.get_job_ids()[page * page_size:(page + 1) * page_size]
        return [self._db.get_job_info(job_id) for job_id in job_ids]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_jobs_done(self):
        for job_id in self._db.get_job_ids():
            if self._db.get_job_info(job_id)['status'] == JobStatus.done.value:
                self._db.delete_job(job_id)
        return 'ok'

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_jobs_all(self):
        for job_id in self._db.get_job_ids():
            self._db.delete_job(job_id)
        return 'ok'

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_job(self, job_id):
        self._db.delete_job(job_id)
        return 'ok'

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def rerun_job(self, job_id):
        self._db.rerun_job(job_id)
        return 'ok'

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def get_job_log(self, job_id):
        return self._db.get_job_log_content(job_id)

    @cherrypy.expose
    def about(self):
        template = self._lookup.get_template('about.html')
        return template.render(**{**self._template_data, **self.session_data})

    @cherrypy.expose
    def version(self):
        return quapylab.__version__

