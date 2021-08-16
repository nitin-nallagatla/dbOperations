import logging

logging.basicConfig(filename='mySqlClass.log', filemode='w',
                    format='[{%(asctime)s} %(filename)s: %(lineno)d]\t%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
