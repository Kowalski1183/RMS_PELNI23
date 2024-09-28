from fastapi import FastAPI
from app import app
from uvicorn.config import LOGGING_CONFIG as log_config
import uvicorn

# import os
# import logging
# import sys

# from uvicorn import Config, Server
# from loguru import logger

# LOG_LEVEL = logging.getLevelName(os.environ.get("LOG_LEVEL", "DEBUG"))
# JSON_LOGS = True if os.environ.get("JSON_LOGS", "0") == "1" else False

# class InterceptHandler(logging.Handler):
#     def emit(self, record):
#         # get corresponding Loguru level if it exists
#         try:
#             level = logger.level(record.levelname).name
#         except ValueError:
#             level = record.levelno

#         # find caller from where originated the logged message
#         frame, depth = sys._getframe(6), 6
#         while frame and frame.f_code.co_filename == logging.__file__:
#             frame = frame.f_back
#             depth += 1

#         logger.opt(depth=depth, exception=record.exc_info).log(
#             level, record.getMessage())


# def setup_logging():
#     # intercept everything at the root logger
#     logging.root.handlers = [InterceptHandler()]
#     logging.root.setLevel(LOG_LEVEL)

#     # remove every other logger's handlers
#     # and propagate to root logger
#     for name in logging.root.manager.loggerDict.keys():
#         logging.getLogger(name).handlers = []
#         logging.getLogger(name).propagate = True

#     # configure loguru
#     logger.configure(handlers=[{"sink": sys.stdout, "serialize": JSON_LOGS}])


# if __name__ == '__main__':
#     server = Server(
#         Config(
#             app="main:app",
#             host="0.0.0.0",
#             port=8080,
#             log_level=LOG_LEVEL,
#         ),
#     )

#     setup_logging()

#     server.run()


if __name__ == "__main__":
    log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"
    log_config["formatters"]["default"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"
    # PROD
    # uvicorn.run("main:app", host="172.19.0.53", port=8088, workers=4, log_config=log_config)
    # DEV
    uvicorn.run("main:app", host="172.125.192.164", port=8088, workers=4, log_config=log_config)
    # LOCAL
    # uvicorn.run("main:app", host="0.0.0.0", port=8080, workers=4, log_config=log_config)
