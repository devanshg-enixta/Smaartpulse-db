import yaml
from sqlalchemy import create_engine
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

yaml_file = open(os.path.join(BASE_DIR, "config/databases.yaml"), 'r')
dbs = yaml.load(yaml_file)
yaml_file.close()


def get_engine():
    db = dbs['saas']
    engine = create_engine(
        'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(db['user'], db['passwd'], db['host'], db['port'], db['db']),
        encoding='utf-8')
    return engine