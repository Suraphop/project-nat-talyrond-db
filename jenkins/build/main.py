import utils.constant as constant
import os

from utils.csv_to_db import TALYROND
from dotenv import load_dotenv

load_dotenv()

talyrond = TALYROND(
        path=constant.TALYROND_PATH,
        server=os.getenv('SERVER'),
        database=os.getenv('DATABASE'),
        user_login=os.getenv('USER_LOGIN'),
        password=os.getenv('PASSWORD'),
        table=constant.TALYROND_TABLE,
        table_columns=constant.TALYROND_TABLE_COLUMNS,
        table_log=constant.TALYROND_TABLE_LOG,
        table_columns_log=constant.TALYROND_TABLE_COLUMNS_LOG,
        line_notify_token=os.getenv('SLACK_NOTIFY_TOKEN'),
    )


talyrond.run()