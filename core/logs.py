from loguru import logger as log
import os
import requests as req
from datetime import datetime


class Logger:
    _URL_MESSAGE = ('https://api.telegram.org/bot{token}/sendMessage?'
                    'chat_id={chat_id}&disable_notification={notify}&text={message}')

    _URL_FILE    = ('https://api.telegram.org/bot{token}/sendDocument?'
                    'chat_id={chat_id}')

    _PARSER_LOGS_PATH = os.getenv('PARSER_LOGS_PATH', 'logs')

    # Получение настроек мониторинга ТГ-бота из переменных окружения
    _PARSER_TG_BOT_KEY = os.getenv('PARSER_TG_BOT_KEY', '')
    _CHATS             = os.getenv('PARSER_TG_CHAT_IDS', '0').split(',')

    _CRITICAL_FORMAT = '⛔️ ОШИБКА\n🕓 {time:%d.%m.%Y %H:%M:%S.%f}\n\n✉️ {message}'
    _SUCCESS_FORMAT  = '✅ УСПЕХ \n🕓 {time:%d.%m.%Y %H:%M:%S.%f}\n\n✉️ {message}'
    _RUN_FORMAT      = '🚀 НАЧАЛО \n🕓 {time:%d.%m.%Y %H:%M:%S.%f}\n\n✉️ {message}'

    _CRITICAL_FORMAT_CHECKER = '⛔️ ОШИБКА\n🕓 {time:%d.%m.%Y %H:%M:%S.%f}\n\n✉️ {message}'
    _SUCCESS_FORMAT_CHECKER  = '✅ УСПЕХ \n🕓 {time:%d.%m.%Y %H:%M:%S.%f}\n\n✉️ {message}'
    _RUN_FORMAT_CHECKER      = '🚀 НАЧАЛО \n🕓 {time:%d.%m.%Y %H:%M:%S.%f}\n\n✉️ {message}'

    def __init__(self):
        self.log = log
        self.filename = '{path}/file_{time:%d-%m-%Y_%H_%M}.log'.format(time=datetime.now(), path=self._PARSER_LOGS_PATH)
        log.add(self.filename, rotation="150 MB", compression="zip", enqueue=True)

    def send(self, message: str, message_format: str, success: bool = True):
        message = message_format.format(time=datetime.now(), message=message)
        for chat_id in self._CHATS:
            try:
                req.get(
                    self._URL_MESSAGE.format(
                        token=self._PARSER_TG_BOT_KEY,
                        chat_id=chat_id,
                        message=message,
                        notify='true' if success else 'false'
                    )
                )
            except Exception as e:
                log.critical(f'{type(e)}: {e}')

    def send_log_file(self):
        for chat_id in self._CHATS:
            try:
                with open(self.filename, 'rb') as document:
                    req.post(
                        self._URL_FILE.format(
                            token=self._PARSER_TG_BOT_KEY,
                            chat_id=chat_id
                        ),
                        files={'document': document}
                    )
            except Exception as e:
                log.error(f'Ошибка отправки логов боту. {type(e)}: {e}')

    def success(self, message, check=False):
        #self.send(message, self._SUCCESS_FORMAT if not check else self._SUCCESS_FORMAT_CHECKER)
        log.success(message)

    def critical(self, message, check=False):
        #self.send(message, self._CRITICAL_FORMAT if not check else self._CRITICAL_FORMAT_CHECKER, False)
        log.critical(message)

    def run(self, message, check=False):
        #self.send(message, self._RUN_FORMAT if not check else self._RUN_FORMAT_CHECKER)
        log.success(message)

    @staticmethod
    def error(message):
        log.error(message)

    @staticmethod
    def info(message):
        log.info(message)


logger: Logger = Logger()
