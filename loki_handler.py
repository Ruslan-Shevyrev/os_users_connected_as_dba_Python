from logging import StreamHandler
import os, time, logging, requests
from configparser import ConfigParser

URL_CONF = 'config/config.ini'


# Добавление логирования.

try:
    LOKI_URL = os.environ["LOKI_URL"]
except KeyError:
    config = ConfigParser()
    config.read(URL_CONF)
    LOKI_URL = config['loki']['LOKI_URL']

try:
    LOKI_JOB_NAME = os.environ["LOKI_JOB_NAME"]
except KeyError:
    config = ConfigParser()
    config.read(URL_CONF)
    LOKI_JOB_NAME = config['loki']['LOKI_JOB_NAME']


logger = logging.getLogger('my_logger')
logger.setLevel(logging.INFO)

LOKI_LABELS = {
    'job': LOKI_JOB_NAME,
    'app': 'microservices',
}


class LokiHandler(StreamHandler):
    """
        # Настройка логгера
        import logging
        logger = logging.getLogger('my_logger')
        logger.setLevel(logging.INFO)

        # Создание обработчика для Loki
        loki_handler = LokiHandler(url=loki_url, labels=loki_labels)
        logger.addHandler(loki_handler)

        # Пример записи логов
        logger.info({'message': 'EXAMPLE INFO'})
        logger.error({'message': 'EXAMPLE ERROR'})
    """

    def __init__(self, url, labels):
        """
        loki_url=http://oraapex-s1.gksm.local:3100/loki/api/v1/push/
        loki_labels = {
                        'job': 'test_job',
                        'app': 'microservices'
                        }
        """
        super().__init__()
        self.url = url
        self.labels = labels

    def emit(self, record):
        log_entry = self.format(record)
        timestamp = int(time.time() * 1000000000)  # nanoseconds
        log_data = {
            "streams": [
                {
                    "stream": {
                        **self.labels,
                        'level': record.levelname,
                        'level_number': str(record.levelno)
                    },
                    "values": [
                        [str(timestamp), log_entry]
                    ]
                }
            ]
        }
        try:
            response = requests.post(self.url, json=log_data)
        except Exception as e:
            print(f'FAILED TO SEND LOG TO THE LOKI. [EXCEPTION]: {e}')
            print(f'log_data={log_data}')
            return
        if response.status_code != 204:
            print(f"Failed to send log to Loki: {response.text}")


loki_handler = LokiHandler(url=LOKI_URL, labels=LOKI_LABELS)
logger.addHandler(loki_handler)
