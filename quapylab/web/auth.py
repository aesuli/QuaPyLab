import cherrypy

__author__ = 'Andrea Esuli, adapted from: https://github.com/cherrypy/tools/blob/master/AuthenticationAndAccessRestrictions'

USER_SESSION_KEY = '_cp_username'


def check_auth(*args, **kwargs):
    conditions = cherrypy.request.config.get('tools.auth.require', None)
    if conditions is not None:
        cherrypy.request.login = cherrypy.session.get(USER_SESSION_KEY, None)
        for condition in conditions:
            if not condition():
                raise cherrypy.HTTPError(401)


def enable_controller_service():
    cherrypy.tools.auth = cherrypy.Tool('before_handler', check_auth)


def require(*conditions):
    def decorate(f):
        if not hasattr(f, '_cp_config'):
            f._cp_config = dict()
        if 'tools.auth.require' not in f._cp_config:
            f._cp_config['tools.auth.require'] = []
        f._cp_config['tools.auth.require'].extend(conditions)
        return f

    return decorate


def fail_with_error_message(code, message):
    def check():
        raise cherrypy.HTTPError(code, message)

    return check


def redirect(redirect_path):
    def check():
        raise cherrypy.HTTPRedirect(redirect_path)

    return check


def fail():
    def check():
        return False

    return check


def negation_of(condition):
    def check():
        return not condition()

    return check


def any_of(*conditions):
    def check():
        for c in conditions:
            if c():
                return True
        return False

    return check


def all_of(*conditions):
    def check():
        for c in conditions:
            if not c():
                return False
        return True

    return check


def logged_in():
    def check():
        return cherrypy.request.login is not None

    return check


def name_is(required_username):
    def check():
        return required_username == cherrypy.request.login

    return check
