import re
import logging
from urlparse import parse_qs
from cStringIO import StringIO

import gevent.monkey
gevent.monkey.patch_socket()

from wsgiref.simple_server import (
    make_server,
    WSGIRequestHandler,
    WSGIServer,
    ServerHandler,
)

from dulwich.server import (
    DEFAULT_HANDLERS,
)

from dulwich.swift import (
    load_conf,
    SwiftRepo,
    InvalidRepoException,
)

from dulwich.protocol import (
    ReceivableProtocol,
)

HTTP_OK = '200 OK'
HTTP_NOT_FOUND = '404 Not Found'
HTTP_ERROR = '500 Internal Server Error'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

class ServerHandlerLogger(ServerHandler):

    def log_exception(self, exc_info):
        logger.exception('Exception happened during processing of request')

    def log_message(self, format, *args):
        logger.info(format, *args)

    def log_error(self, *args):
        logger.error(*args)

class WSGIRequestHandlerLogger(WSGIRequestHandler):

    def log_exception(self, exc_info):
        logger.exception('Exception happened during processing of request')

    def log_message(self, format, *args):
        logger.info(format, *args)

    def log_error(self, *args):
        logger.error(*args)

    def handle(self):

        self.raw_requestline = self.rfile.readline()
        if not self.parse_request(): # An error code has been sent, just exit
            return

        handler = ServerHandlerLogger(
            self.rfile, self.wfile, self.get_stderr(), self.get_environ()
        )
        handler.request_handler = self      # backpointer for logging
        handler.run(self.server.get_app())

class WSGIServerLogger(WSGIServer):
    def handle_error(self, request, client_address):
        logger.exception('Exception happened during processing of request from %s' % str(client_address))

class HTTPGitRequest(object):
    def __init__(self, environ, start_response, handlers=None):
        self.environ = environ
        self._start_response = start_response
        self._cache_headers = []
        self._headers = []
        self.handlers = handlers

    def add_header(self, name, value):
        self._headers.append((name, value))

    def respond(self, status=HTTP_OK, content_type=None, headers=None):
        if headers:
            self._headers.extend(headers)
        if content_type:
            self._headers.append(('Content-Type', content_type))
        self._headers.extend(self._cache_headers)

        return self._start_response(status, self._headers)

    def not_found(self, message):
        self._cache_headers = []
        self.respond(HTTP_NOT_FOUND, 'text/plain')
        return message

    def error(self, message):
        self._cache_headers = []
        self.respond(HTTP_ERROR, 'text/plain')
        return message

def url_prefix(mat):
    """Extract the URL prefix from a regex match.

    :param mat: A regex match object.
    :returns: The URL prefix, defined as the text before the match in the
        original string. Normalized to start with one leading slash and end with
        zero.
    """
    return '/' + mat.string[:mat.start()].strip('/')

def get_info_refs(req, backend, mat):
    params = parse_qs(req.environ['QUERY_STRING'])
    service = params.get('service', [None])[0]
    if service:
        handler_cls = req.handlers.get(service, None)
        if handler_cls is None:
            yield req.forbidden('Unsupported service %s' % service)
            return
        try:
            backend.open_repository(url_prefix(mat))
        except InvalidRepoException, e:
            yield req.not_found('')
            return
        write = req.respond(content_type='application/x-%s-advertisement' % service)
        proto = ReceivableProtocol(StringIO().read, write)
        handler = handler_cls(backend, [url_prefix(mat)], proto,
                              http_req=req, advertise_refs=True)
        handler.proto.write_pkt_line('# service=%s\n' % service)
        handler.proto.write_pkt_line(None)
        handler.handle()
    else:
        yield req.not_found('')

def handle_service_request(req, backend, mat):
    service = mat.group().lstrip('/')
    logger.info('Handling service request for %s', service)
    handler_cls = req.handlers.get(service, None)
    if handler_cls is None:
        yield req.forbidden('Unsupported service %s' % service)
        return
    write = req.respond(HTTP_OK, 'application/x-%s-result' % service)
    proto = ReceivableProtocol(req.environ['wsgi.input'].read, write)
    handler = handler_cls(backend, [url_prefix(mat)], proto, http_req=req)
    handler.handle()

class _LengthLimitedFile(object):
    """Wrapper class to limit the length of reads from a file-like object.

    This is used to ensure EOF is read from the wsgi.input object once
    Content-Length bytes are read. This behavior is required by the WSGI spec
    but not implemented in wsgiref as of 2.5.
    """

    def __init__(self, input, max_bytes):
        self._input = input
        self._bytes_avail = max_bytes

    def read(self, size=-1):
        if self._bytes_avail <= 0:
            return ''
        if size == -1 or size > self._bytes_avail:
            size = self._bytes_avail
        self._bytes_avail -= size
        return self._input.read(size)

class LimitedInputFilter(object):
    """WSGI middleware that limits the input length of a request to that
    specified in Content-Length.
    """

    def __init__(self, application):
        self.app = application

    def __call__(self, environ, start_response):
        # This is not necessary if this app is run from a conforming WSGI
        # server. Unfortunately, there's no way to tell that at this point.
        # TODO: git may used HTTP/1.1 chunked encoding instead of specifying
        # content-length
        content_length = environ.get('CONTENT_LENGTH', '')
        if content_length:
            environ['wsgi.input'] = _LengthLimitedFile(
                environ['wsgi.input'], int(content_length))
        return self.app(environ, start_response)

class HTTPGitApplication(object):
    services = {
      ('GET', re.compile('/info/refs$')): get_info_refs,
      ('POST', re.compile('/git-upload-pack$')): handle_service_request,
      ('POST', re.compile('/git-receive-pack$')): handle_service_request,
    }

    def __init__(self, backend):
        self.backend = backend
        self.handlers = dict(DEFAULT_HANDLERS)

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO']
        method = environ['REQUEST_METHOD']
        req = HTTPGitRequest(environ, start_response, handlers=self.handlers)
        handler = None
        for smethod, spath in self.services.iterkeys():
            if smethod != method:
                continue
            mat = spath.search(path)
            if mat:
                handler = self.services[smethod, spath]
                break

        if handler is None:
            return req.not_found('Sorry, that method is not supported')

        return handler(req, self.backend, mat)

class SwiftSystemBackend():
    def __init__(self, conf):
        self.conf = conf

    def open_repository(self, path):
        return SwiftRepo(path, self.conf)

def main():
    listen_addr = ''
    port = 8000
    backend = SwiftSystemBackend(load_conf())
    app = LimitedInputFilter(HTTPGitApplication(backend))
    server = make_server(listen_addr, port, app,
                         handler_class=WSGIRequestHandlerLogger,
                         server_class=WSGIServerLogger)
    logger.info('Listening for HTTP connections on %s:%d', listen_addr,
                port)
    server.serve_forever()

main()
