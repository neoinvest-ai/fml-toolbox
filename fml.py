import logging

from fml import pipeline

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    pipeline.run()
