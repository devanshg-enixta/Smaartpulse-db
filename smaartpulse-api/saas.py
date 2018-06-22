from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from smartpulse_app import app
import logging, os
from logging.handlers import RotatingFileHandler

if __name__ == '__main__':
    if not os.path.exists('logs/'):
        os.makedirs('logs/')
    # initialize the log handler
    logHandler = RotatingFileHandler('logs/saas.log', maxBytes=100000, backupCount=20)
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
    logHandler.setFormatter(formatter)
    # set the log handler level
    logHandler.setLevel(logging.INFO)

    # set the app logger level
    app.logger.setLevel(logging.INFO)
    app.logger.addHandler(logHandler)

    http_server = HTTPServer(WSGIContainer(app))
    http_server.listen(9191)
    IOLoop.instance().start()
