import logging
import colorlog


def add_file_handler(file_path, log, formatter) -> None:
    fileHandler = logging.FileHandler(file_path)

    fileHandler.setFormatter(formatter)

    log.addHandler(fileHandler)


log = logging.getLogger('pythonConfig')

log.setLevel(logging.DEBUG)

date_format = '%Y-%m-%d %H:%M:%S'

cformat = ' %(log_color)s %(asctime)s\t%(levelname)s\t%(funcName)30s%(lineno)4d\t%(message)s'

colors = {
    'DEBUG': 'green',
    'INFO': 'cyan',
    'WARNING': 'bold_yellow',
    'ERROR': 'bold_red',
    'CRITICAL': 'bold_purple'
}

formatter = colorlog.ColoredFormatter(cformat, date_format, log_colors = colors)

stream_handler = logging.StreamHandler()

stream_handler.setFormatter(formatter)

log.addHandler(stream_handler)
