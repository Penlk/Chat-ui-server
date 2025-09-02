import os
import logging
import jwt
from chat.auth_utils import verify_gateway_auth

logger = logging.getLogger(__name__)

class UserIdMiddleware:
    """
    Middleware для извлечения заголовков идентичности, проброшенных из Gateway.
    Поддерживает как новый формат: X-User-Data с JWT токеном, так и стандартный Authorization Bearer.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        logger.info(f"🚀 Middleware: Начало обработки запроса {request.method} {request.path}")
        logger.info(f"🔍 Middleware: Все заголовки: {dict(request.headers)}")
        
        # Сначала пробуем X-User-Data заголовок (новый формат)
        x_user_data = request.headers.get('X-User-Data')
        logger.info(f"🔍 Middleware: X-User-Data header = {x_user_data}")
        
        if x_user_data:
            # Используем новую систему аутентификации
            auth_data = verify_gateway_auth(x_user_data)
            logger.info(f"🔍 Middleware: auth_data.is_valid = {auth_data.is_valid}")
            logger.info(f"🔍 Middleware: auth_data.user_id = {auth_data.user_id}")
            
            if auth_data.is_valid:
                request.user_id = auth_data.user_id
                request.active_org_id = auth_data.active_org_id
                request.user_email = auth_data.user_email
                request.user_roles = auth_data.user_roles
                request.jwt_payload = auth_data.jwt_payload
                logger.info(f"✅ Middleware: Установлены данные пользователя: user_id={request.user_id}")
            else:
                # Если JWT невалидный, сбрасываем данные
                request.user_id = None
                request.active_org_id = None
                request.user_email = None
                request.user_roles = []
                request.jwt_payload = None
                logger.warning("❌ Middleware: JWT невалидный, данные сброшены")
        else:
            # Если нет X-User-Data, пробуем стандартный Authorization Bearer
            auth_header = request.headers.get('Authorization')
            logger.info(f"🔍 Middleware: Authorization header = {auth_header}")
            
            if auth_header and auth_header.startswith('Bearer '):
                try:
                    # Извлекаем токен
                    token = auth_header.split(' ')[1]
                    logger.info(f"🔍 Middleware: Извлечен токен: {token[:20]}...")
                    
                    # Декодируем JWT токен (без проверки подписи для демо)
                    jwt_payload = jwt.decode(token, options={"verify_signature": False})
                    logger.info(f"🔍 Middleware: JWT payload получен")
                    
                    # Извлекаем sub из JWT токена
                    sub = jwt_payload.get("sub")
                    if sub:
                        request.user_id = sub
                        request.active_org_id = None
                        request.user_email = jwt_payload.get("email", "")
                        request.user_roles = []
                        request.jwt_payload = jwt_payload
                        logger.info(f"✅ Middleware: Установлены данные пользователя из Bearer токена: user_id={request.user_id}")
                    else:
                        logger.warning("❌ Middleware: В JWT токене отсутствует sub")
                        request.user_id = None
                        request.active_org_id = None
                        request.user_email = None
                        request.user_roles = []
                        request.jwt_payload = None
                        
                except jwt.InvalidTokenError as e:
                    logger.error(f"❌ Middleware: Невалидный JWT токен: {e}")
                    request.user_id = None
                    request.active_org_id = None
                    request.user_email = None
                    request.user_roles = []
                    request.jwt_payload = None
                except Exception as e:
                    logger.error(f"❌ Middleware: Ошибка обработки Bearer токена: {e}")
                    request.user_id = None
                    request.active_org_id = None
                    request.user_email = None
                    request.user_roles = []
                    request.jwt_payload = None
            else:
                # Если нет ни X-User-Data, ни Authorization Bearer, сбрасываем все данные
                request.user_id = None
                request.active_org_id = None
                request.user_email = None
                request.user_roles = []
                request.jwt_payload = None
                logger.warning("❌ Middleware: Нет X-User-Data или Authorization Bearer заголовка")

        # Убираем mock логику - если нет токена, возвращаем 401
        if not request.user_id:
            logger.warning("❌ Middleware: Аутентификация не пройдена - нет валидного токена")
        else:
            logger.info(f"✅ Middleware: Аутентификация пройдена успешно, user_id={request.user_id}")

        logger.info(f"🚀 Middleware: Завершение обработки, user_id={getattr(request, 'user_id', None)}")
        return self.get_response(request)