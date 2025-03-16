import datetime
import jwt
from src.constants.config import Config
from src.utils.logger import get_logger

logger = get_logger("Auth Service")

def generate_jwt(user_id, role, secret_key=Config.SECRET, expire=Config.EXPIRATION_JWT):
    expiration_time = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=expire)

    token = jwt.encode(
        {
            "userId": user_id,
            "exp": expiration_time,
            "role": role
        },
        secret_key
    )

    return str(token)


def decode_jwt(token, secret_key=Config.SECRET):
    try:
        info = jwt.decode(token, secret_key, algorithms=["HS256"])
        return info
    except jwt.exceptions.InvalidTokenError:
        return None
    except Exception as ex:
        logger.exception(ex)
    return None
