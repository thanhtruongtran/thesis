from functools import wraps
import jwt
from sanic import Request
from sanic.exceptions import Unauthorized, Forbidden, NotFound

from src.constants.config import Roles, Config


def check_token(secret_key, token):
    if not token:
        raise Unauthorized('JWT required')

    try:
        jwt_ = jwt.decode(
            token, secret_key, algorithms=["HS256"]
        )

        if not jwt_.get('userId'):
            raise Unauthorized('Invalid JWT')

    except jwt.exceptions.InvalidTokenError:
        raise Unauthorized('Invalid JWT')

    return jwt_


def require_auth(project_type, required_role):
    required_role_level = Roles.mapping.get(required_role)

    if (not project_type) or (project_type == Roles.private):
        if required_role_level > Roles.mapping[Roles.guest]:
            return True

    else:
        if required_role == Roles.private or required_role_level > Roles.mapping[Roles.private]:
            return True

    return False


def check_role_to_access_resource_permission(user_id, project_user_roles, required_role):
    required_role_level = Roles.mapping.get(required_role)
    if project_user_roles.get('type') == Roles.public:
        if (required_role_level < Roles.mapping[Roles.viewer]) or (required_role == Roles.viewer):
            return

    users_role = project_user_roles.get('userRole', {})
    if user_id is not None:
        role = users_role.get(user_id, {}).get('role', Roles.public)
    else:
        role = Roles.guest

    if Roles.mapping.get(role, float('-inf')) < required_role_level:
        raise Forbidden('Permission denied')


def check_permissions(roles, user_id, project_user_roles, permission_path=None):
    permissions = []
    for permission, info in roles.items():
        if permission.startswith('_'):
            continue

        p_path = f'{permission_path}.{permission}' if permission_path is not None else permission
        if not isinstance(info, dict):
            try:
                check_role_to_access_resource_permission(
                    user_id=user_id, project_user_roles=project_user_roles,
                    required_role=info
                )
            except Forbidden:
                continue

            permissions.append(p_path)
        else:
            sub_permissions = check_permissions(
                info, user_id=user_id, project_user_roles=project_user_roles,
                permission_path=p_path
            )
            permissions.extend(sub_permissions)

    return permissions


def authenticate():
    def decorator(f):
        @wraps(f)
        async def decorated_function(request, *args, **kwargs):
            token = request.headers.get('Authorization')

            if not token or token == 'null':
                raise Unauthorized('JWT required')

            info = check_token(
                secret_key=Config.SECRET,
                token=token
            )
            user_id = info.get('userId')

            response = await f(request, user_id, *args, **kwargs)
            return response

        return decorated_function

    return decorator
 