import os

import dotenv

_ = dotenv.load_dotenv()


class MissingEnvironmentVariableError(Exception):
    pass


try:
    DCH_API_KEY = os.environ["DCH_API_KEY"]
    DCH_UPLOAD_URL = "https://dataclearinghouse.org/api/chronos/v1/observations/upload"
    DCH_DATA_POOL_ID = "evse_kerbside_wholesale_price_drf"
    DCH_POINT_ID = "wholesale_price_drf"
    DB_HOST = os.environ["DB_HOST"]
    DB_NAME = os.environ["DB_NAME"]
    DB_PASSWORD = os.environ["DB_PASSWORD"]
    DB_PORT = os.environ["DB_PORT"]
    DB_USER = os.environ["DB_USER"]
except KeyError as err:
    message = f"Missing environment variable:{err.args[0]}"
    raise MissingEnvironmentVariableError(message) from err
