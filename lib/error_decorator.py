import os
import traceback
from datetime import date, datetime

from flask import current_app
from flask_mail import Message
from loguru import logger

from lib.my_email import send_email

today_str = date.today().strftime('%Y%m%d')
today_time = datetime.now().strftime('%Y%m%d%H%M%S')
APP_PATH = os.path.dirname(os.path.abspath(__file__))


def safe_run(func):
    def func_wrapper(*args, **kwargs):

        try:
            return func(*args, **kwargs)

        except Exception as e:

            handle_exception(e)

    return func_wrapper


def handle_exception(e):
    trace = log_traceback(e)
    if isinstance(trace, list):
        msg = '<br/> '.join(trace)
    else:
        msg = repr(trace)
    send_email('phanveehuen@gmail.com', message_=msg, subject='ds api error', message_type='html')


def log_traceback(ex, ex_traceback=None):
    if ex_traceback is None:
        ex_traceback = ex.__traceback__
    tb_lines = [line.rstrip('\n') for line in
                traceback.format_exception(ex.__class__, ex, ex_traceback)]
    return tb_lines
