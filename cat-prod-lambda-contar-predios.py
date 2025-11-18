"""
Lambda Function: Contar Predios
Obtiene el número de predios asociados al documento del usuario
"""
import json
import logging
import time
import requests
import boto3
import os
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente DynamoDB para obtener el token
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_NAME = 'cat-test-certification-session-tokens'

# URL base de la API
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://vmprocondock.catastrobogota.gov.co:3400/catia-auth')

# Configuración de reintentos con exponential backoff
MAX_RETRIES = 10
INITIAL_BACKOFF = 1  # segundos
MAX_BACKOFF = 60  # segundos

def lambda_handler(event, context):
    """
    Obtiene el conteo de predios del usuario autenticado
    
    Input esperado:
    {
        "documento": "1234567890"
    }
    
    Output:
    {
        "success": true/false,
        "message": "descripción",
        "data": {
            "cantidadPredios": 5,
            "documento": "1234567890"
        },
        "errorCode": "código de error (opcional)"
    }
    """
    logger.info("=== Lambda: Contar Predios ===")
    logger.info(f"Event: {json.dumps(event)}")
    
    try:
        # Extraer datos del evento - Bedrock Agent envía en requestBody
        logger.info("Extrayendo parámetros del evento de Bedrock Agent")
        if 'requestBody' in event and 'content' in event['requestBody']:
            content = event['requestBody']['content']
            if 'application/json' in content:
                # Bedrock Agent envía properties como array de objetos
                properties = content['application/json']['properties']
                body = {prop['name']: prop['value'] for prop in properties}
                documento = body.get('documento', '')
            else:
                documento = ''
        else:
            # Formato directo para testing
            documento = event.get('documento', '')
        
        session_id = event.get('sessionId', 'N/A')
        logger.info(f"Parámetros extraídos - Documento: {documento[:3] if documento else ''}***, SessionId: {session_id}")
        
        # Validación de inputs
        if not documento:
            logger.error("Documento vacío")
            return format_bedrock_response(
                event=event,
                status_code=400,
                body={
                    "success": False,
                    "message": "El documento es requerido",
                    "data": {},
                    "errorCode": "MISSING_DOCUMENTO"
                }
            )
        
        logger.info("Parámetros validados correctamente")
        logger.info(f"Contando predios para documento: {documento[:3]}***")
        
        # Validar token 
        logger.info("Validando token")

        validate_token_response = validate_token(documento)
        if not validate_token_response['success']:
            logger.error(f"Token inválido: {validate_token_response.get('message')}")
            return format_bedrock_response(
                event=event,
                status_code=401,
                body={
                    "success": False,
                    "message": "Tu sesión ha expirado. Por favor, valida tu identidad nuevamente",
                    "data": {},
                    "errorCode": "TOKEN_EXPIRED"
                }
            )
        
        logger.info("Token validado exitosamente")
        
        # Obtener token de DynamoDB
        logger.info("Iniciando recuperación de token desde DynamoDB")
        token_dict= get_token_from_dynamodb(documento)
        token = token_dict.get('token', '') if token_dict else ''
        
        if not token:
            logger.error("Token no encontrado en DynamoDB")
            return format_bedrock_response(
                event=event,
                status_code=401,
                body={
                    "success": False,
                    "message": "Sesión no autenticada. Por favor, valida tu identidad primero",
                    "data": {},
                    "errorCode": "TOKEN_NOT_FOUND"
                }
            )
        
        logger.info("Token recuperado de DynamoDB")

        
        # Llamar a la API de conteo de predios
        logger.info("Iniciando llamada al API externo")
        api_response = call_contar_predios_api(token)
        
        # Procesar respuesta

        if api_response['status_code'] == 200:
            logger.info("API respondió exitosamente con status 200")
            response_data = api_response['data']
            logger.info(f"Datos de respuesta del API: {json.dumps(response_data)}")
            
            response = {
                "success": response_data.get('success', True),
                "message": response_data.get('message', 'Predios consultados exitosamente'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', '')
            }
            
            logger.info(f"Resultado mapeado: {json.dumps(response)}")
            logger.info("=== Lambda completado exitosamente ===")
            return format_bedrock_response(event=event, status_code=200, body=response)
        
        elif api_response['status_code'] == 405:
            # Usuario no activo - Keep 405 (4xx codes work in Bedrock)
            logger.warning("⚠️ Usuario no se encuentra activo")
            response_data = api_response.get('data', {})
            
            response = {
                "success": False,
                "message": response_data.get('message', 'El usuario no se encuentra activo'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', 'USER_INACTIVE')
            }
            
            return format_bedrock_response(event=event, status_code=405, body=response)
        
        elif api_response['status_code'] == 406:
            # Preguntas de seguridad no diligenciadas - Keep 406 (4xx codes work in Bedrock)
            logger.warning("⚠️ Usuario no ha diligenciado preguntas de seguridad")
            response_data = api_response.get('data', {})
            
            response = {
                "success": False,
                "message": response_data.get('message', 'El usuario no ha diligenciado las preguntas de seguridad'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', 'SECURITY_QUESTIONS_PENDING')
            }
            
            return format_bedrock_response(event=event, status_code=406, body=response)
        
        elif api_response['status_code'] == 401:
            # Token inválido o expirado - Keep 401 (4xx codes work in Bedrock)
            logger.error("❌ Token inválido o expirado")
            
            response = {
                "success": False,
                "message": "Tu sesión ha expirado. Por favor, valida tu identidad nuevamente",
                "data": {},
                "errorCode": "TOKEN_EXPIRED"
            }
            
            return format_bedrock_response(event=event, status_code=401, body=response)
        
        elif api_response['status_code'] in [503, 504]:
            # Network errors: Return 200 with error body so Bedrock can handle it
            logger.warning(f"⚠️ Error de red (status {api_response['status_code']}), retornando 200 con error en body")
            
            response = {
                "success": False,
                "message": "Error de conectividad con el servicio. Por favor, intenta nuevamente.",
                "data": {},
                "errorCode": "NETWORK_ERROR"
            }
            
            return format_bedrock_response(event=event, status_code=200, body=response)
        
        elif api_response['status_code'] == 500:
            # Server errors: Return 200 with error body so Bedrock can handle it
            logger.warning(f"⚠️ Error del servidor (status 500), retornando 200 con error en body")
            
            response = {
                "success": False,
                "message": "Error al consultar los predios. Por favor, intenta nuevamente más tarde.",
                "data": {},
                "errorCode": "INTERNAL_SERVER_ERROR"
            }
            
            return format_bedrock_response(event=event, status_code=200, body=response)
        
        else:
            # Otro error - Return 200 with error body
            logger.error(f"❌ Error en API - Status: {api_response['status_code']}")
            error_message = api_response.get('error', 'Error al consultar los predios')
            
            response = {
                "success": False,
                "message": error_message,
                "data": {},
                "errorCode": "API_ERROR"
            }
            
            return format_bedrock_response(event=event, status_code=200, body=response)
        
    except requests.exceptions.Timeout:
        logger.error("Timeout llamando a la API de predios")
        logger.warning("⚠️ Retornando 200 con error en body para que Bedrock pueda procesarlo")
        return format_bedrock_response(
            event=event,
            status_code=200,
            body={
                "success": False,
                "message": "El servicio está tardando demasiado en responder. Por favor, intenta nuevamente.",
                "data": {},
                "errorCode": "TIMEOUT"
            }
        )
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de red llamando a la API: {str(e)}")
        logger.warning("⚠️ Retornando 200 con error en body para que Bedrock pueda procesarlo")
        return format_bedrock_response(
            event=event,
            status_code=200,
            body={
                "success": False,
                "message": "Error de conexión con el servicio. Por favor, intenta nuevamente.",
                "data": {},
                "errorCode": "NETWORK_ERROR"
            }
        )
        
    except Exception as e:
        logger.exception(f"Error inesperado en handler: {str(e)}")
        logger.warning("⚠️ Retornando 200 con error en body para que Bedrock pueda procesarlo")
        return format_bedrock_response(
            event=event,
            status_code=200,
            body={
                "success": False,
                "message": "Ocurrió un error inesperado. Por favor, intenta nuevamente.",
                "data": {},
                "errorCode": "INTERNAL_ERROR"
            }
        )


def get_token_from_dynamodb(documento):
    """
    Recupera el token JWT desde DynamoDB usando el sessionId
    
    Args:
        documento: Numero de documento del usuario
    
    Returns:
        dict: Item de DynamoDB con el token o None si no se encuentra
    """
    if not documento:
        logger.warning("Documento vacío, no se puede recuperar token")
        return None
    
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        logger.info(f"Buscando token en DynamoDB para documento: {documento}")
        response = table.get_item(Key={'documento': documento})
        
        if 'Item' in response:
            token_dict = response['Item']
            token = response['Item'].get('token', '')
            logger.info(f"✅ Token encontrado en DynamoDB para documento: {documento}")
            logger.debug(f"Token (primeros 20 chars): {token[:20]}...")
            return token_dict
        else:
            logger.warning(f"⚠️ No se encontró token para documento: {documento}")
            return None
            
    except ClientError as e:
        logger.error(f"Error de DynamoDB: {e.response['Error']['Message']}")
        return None
    except Exception as e:
        logger.error(f"Error recuperando token: {str(e)}", exc_info=True)
        return None


def call_contar_predios_api(token):
    """
    Llama a la API externa para obtener el conteo de predios
    Implementa exponential backoff para manejar intermitencias de red
    
    Endpoint: POST /properties/count
    Headers: Authorization: Bearer {token}
    Body: {}
    
    Args:
        token: JWT token de autenticación
    
    Returns:
        dict con {status_code, data (opcional), error (opcional)}
    """
    URL = f"{API_BASE_URL}/properties/count"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    # Body vacío según especificación
    payload = {}
    
    logger.info(f"=== Llamando API de Conteo de Predios (con exponential backoff) ===")
    logger.info(f"Endpoint: GET {URL}")
    logger.info(f"Headers: {dict((k, v[:20] + '...' if k == 'Authorization' else v) for k, v in headers.items())}")
    logger.info(f"Payload: {json.dumps(payload)}")
    logger.info(f"Max reintentos: {MAX_RETRIES}, Backoff inicial: {INITIAL_BACKOFF}s")
    
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"--- Intento {attempt + 1}/{MAX_RETRIES} ---")
            
            # Timeout de 15 segundos
            resp = requests.get(URL, json=payload, headers=headers, timeout=15)
            
            logger.info(f"Respuesta recibida - Status Code: {resp.status_code}")
            logger.info(f"Response headers: {dict(resp.headers)}")
            logger.info(f"Response content length: {len(resp.content)} bytes")
            
            # Verificar si la respuesta está vacía
            if not resp.content or len(resp.content) == 0:
                logger.error("Respuesta vacía del API")
                
                # Si es el último intento, retornar error
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 500,
                        'error': 'El API retornó una respuesta vacía después de múltiples intentos'
                    }
                
                # Aplicar backoff y reintentar
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"Respuesta vacía. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Verificar Content-Type
            content_type = resp.headers.get('Content-Type', '')
            logger.info(f"Content-Type de respuesta: {content_type}")
            
            if 'application/json' not in content_type.lower():
                logger.warning(f"Content-Type no es JSON: {content_type}")
                logger.warning(f"Respuesta completa: {resp.text[:500]}")
            
            # Intentar parsear JSON
            try:
                response_data = resp.json()
                logger.info(f"Response body parseado exitosamente: {json.dumps(response_data)}")
            except ValueError as json_err:
                logger.error(f"Respuesta no es JSON: {resp.text[:500]}")
                logger.error(f"Error al parsear: {str(json_err)}")
                
                # Si es el último intento, retornar error
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 500,
                        'error': f'Respuesta del API no es un JSON válido. Content-Type: {content_type}'
                    }
                
                # Aplicar backoff y reintentar
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Si llegamos aquí, la petición fue exitosa
            logger.info(f"✅ Llamada al API completada exitosamente en intento {attempt + 1}")
            
            return {
                'status_code': resp.status_code,
                'data': response_data
            }
            
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f"Timeout en intento {attempt + 1}/{MAX_RETRIES} (15 segundos)")
            
            # Si es el último intento, lanzar excepción
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Timeout después de {MAX_RETRIES} intentos")
                raise
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f"Error de conexión en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, lanzar excepción
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Error de conexión después de {MAX_RETRIES} intentos")
                raise
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.RequestException as e:
            last_exception = e
            logger.error(f"Error de red en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, lanzar excepción
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Error de red después de {MAX_RETRIES} intentos")
                raise
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except Exception as e:
            # Para errores inesperados, no reintentar
            logger.error(f"Error inesperado en call_contar_predios_api: {str(e)}", exc_info=True)
            return {
                'status_code': 500,
                'error': f'Error inesperado: {str(e)}'
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f"Falló después de {MAX_RETRIES} intentos")
    return {
        'status_code': 500,
        'error': f'Error después de {MAX_RETRIES} intentos: {str(last_exception)}'
    }


def calculate_backoff(attempt):
    """
    Calcula el tiempo de espera usando exponential backoff
    
    Formula: min(INITIAL_BACKOFF * (2 ^ attempt), MAX_BACKOFF)
    
    Args:
        attempt: Número de intento (0-indexed)
    
    Returns:
        float: Tiempo de espera en segundos
    """
    backoff = INITIAL_BACKOFF * (2 ** attempt)
    return min(backoff, MAX_BACKOFF)

#============================
#  Validate token logic
# =========================== 

def validate_token(documento):
    """
    Valida si un token es válido y lo refresca si es necesario
    Args:
        token: JWT token
    Returns:
        dict: {
            'status_code': int,
            'success': bool,
            'message': str
        }
    """
    VALIDATE_TOKEN_URL = f"{API_BASE_URL}/auth/validate-token"


    token_dict = get_token_from_dynamodb(documento)
    token = token_dict.get('token', '') if token_dict else ''

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    last_exception = None

    for attempt in range(MAX_RETRIES):
        try:
            #Llamar al endpoint de validación de token
            logger.info(f"Validando token en intento {attempt + 1}/{MAX_RETRIES}")
            response = requests.get(VALIDATE_TOKEN_URL, headers=headers, timeout=10)
            logger.info(f"Respuesta de validación de token - Status Code: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            logger.info(f"Response content length: {len(response.content)} bytes")

            try:
                response_data = response.json()
                logger.info(f"Response body parseado exitosamente: {json.dumps(response_data)}")
            except json.JSONDecodeError as json_err:
                logger.error(f"Respuesta no es JSON: {response.text[:500]}")
                logger.error(f"Error al parsear: {str(json_err)}")
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 200,
                        'success': False,
                        'message': 'Error al parsear JSON de la respuesta del API'
                    }
                
                # Aplicar backoff y reintentar
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue

            data = response_data.get('data', {})
            is_valid = data.get('valid', False)
            token_info = data.get('tokenInfo', {})
            time_to_expire = token_info.get('timeToExpire', 0)  # Tiempo en segundos para expirar
            logger.info(f"Token válido: {is_valid}, Tiempo para expirar: {time_to_expire}ms")
            
            if is_valid and time_to_expire > 2000:
                logger.info("Token es válido y no está por expirar")
                return  {
                    'status_code': 200,
                    'success': True,
                    'message': 'Token es válido'
                }
            else:
                logger.info("Token inválido o por expirar, iniciando refresh de token")
                refresh_token_response = refresh_token_for_document(token_dict)

                if refresh_token_response['success']:
                    logger.info("Token refrescado exitosamente")
                    return {
                        'status_code': 200,
                        'success': True,
                        'message': 'Token refrescado exitosamente'
                    }
                else:
                    logger.error(f"Error refrescando token: {refresh_token_response.get('message')}")
                    return {
                        'status_code': 200,
                        'success': False,
                        'message': refresh_token_response.get('message', 'Error al refrescar el token')
                    }

        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f"Timeout en intento {attempt + 1}/{MAX_RETRIES} (30 segundos)")
            
            # Si es el último intento, retornar error
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    'status_code': 200,
                    'success': False,
                    'message': f'Tiempo de espera agotado al conectar con el API: {str(e)}'  
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f"Error de conexión en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, retornar error
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Error de conexión después de {MAX_RETRIES} intentos")
                return {
                    'status_code': 200,
                    'success': False,
                    'message': 'No se pudo conectar con el API'
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.RequestException as e:

            last_exception = e
            logger.error(f"Error en la solicitud HTTP en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, retornar error
            if attempt == MAX_RETRIES - 1:

                logger.error(f"Error en solicitud HTTP después de {MAX_RETRIES} intentos")
                return {
                    'status_code': 200,
                    'success': False,
                    'message': 'Error en la solicitud HTTP al conectar con el API'
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except Exception as e:
            # Para errores inesperados, no reintentar
            logger.exception(f"Error inesperado en call_identity_validation_api: {str(e)}")
            return {
                'status_code': 200,
                'success': False,
                'message': f'Error inesperado al conectar con el API: {str(e)}'
            }
 

#============================
#  Refresh token logic
# =========================== 
def refresh_token_for_document(token_dict):
    """
    Refresca el token JWT para un documento específico
    
    Flujo:
    1. Obtiene el refresh token desde DynamoDB usando el documento
    2. Llama al API para obtener un nuevo token
    3. Actualiza DynamoDB con el nuevo token y refresh token
    
    Args:
        token_dict: Item de dynamoDB del usuario
    
    Returns:
        dict: {
            'success': bool,
            'message': str,
            'error_code': str (opcional)
        }
    """
    
    # 1. Obtener refresh token desde DynamoDB
    logger.info("Paso 1: Obteniendo refresh token desde DynamoDB")
    #refresh_token = get_refresh_token_from_dynamodb(documento)
    documento = token_dict.get('documento', '') if token_dict else ''

    logger.info(f"=== Iniciando refresh de token para documento: {documento[:3]}*** ===")

    refresh_token = token_dict.get('refreshToken', '') if token_dict else ''
    
    if not refresh_token:
        logger.error("No se encontró refresh token en DynamoDB")
        return {
            'success': False,
            'message': 'No se encontró refresh token. Por favor, inicia sesión nuevamente.'
        }
    
    logger.info("Refresh token recuperado de DynamoDB")
    
    # 2. Llamar al API para refrescar el token
    logger.info("Paso 2: Llamando al API para refrescar el token")
    api_response = call_refresh_token_api(refresh_token)
    response_data = api_response['data']
    
    if not response_data.get('success'):
        logger.error(f"API respondió con success=false: {response_data.get('message')}")
        return {
            'success': False,
            'message': response_data.get('message', 'Error al refrescar el token')
        }
    
    # 3. Extraer nuevo token y refresh token
    data = response_data.get('data', {})
    new_token = data.get('token', '')
    new_refresh_token = data.get('refreshToken', '')
    token_type = data.get('tokenType', 'Bearer')
    expires_in = data.get('expiresIn', 86400)
    
    if not new_token:
        logger.error("API no devolvió un nuevo token")
        return {
            'success': False,
            'message': 'No se pudo obtener un nuevo token'
        }
    
    logger.info("✅ Nuevo token obtenido del API")
    
    # 4. Actualizar DynamoDB con el nuevo token
    logger.info("Paso 3: Actualizando DynamoDB con nuevo token")
    update_success = update_token_in_dynamodb(
        documento=documento,
        token=new_token,
        refresh_token=new_refresh_token,
        token_type=token_type,
        expires_in=expires_in
    )
    
    if not update_success:
        logger.warning("No se pudo actualizar DynamoDB, pero el token es válido")
        return {
            'success': False,
            # 'token': new_token,
            'message': 'Token refrescado exitosamente (advertencia: no se actualizó en DynamoDB)'
        }
    
    logger.info("✅ Token actualizado en DynamoDB")
    logger.info("=== Refresh de token completado exitosamente ===")
    
    return {
        'success': True,
        'message': 'Token refrescado exitosamente'
    }


def get_refresh_token_from_dynamodb(documento):
    """
    Recupera el refresh token desde DynamoDB usando el documento
    
    Args:
        documento: Número de documento del usuario
    
    Returns:
        str: Refresh token o None si no se encuentra
    """
    if not documento:
        logger.warning("Documento vacío, no se puede recuperar refresh token")
        return None
    
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        logger.info(f"Buscando refresh token en DynamoDB para documento: {documento[:3]}***")
        response = table.get_item(Key={'documento': documento})
        
        if 'Item' in response:
            refresh_token = response['Item'].get('refreshToken', '')
            if refresh_token:
                logger.info(f"Refresh token encontrado para documento: {documento[:3]}***")
                logger.debug(f"Refresh token (primeros 20 chars): {refresh_token[:20]}...")
                return refresh_token
            else:
                logger.warning(f"Item encontrado pero sin refreshToken para documento: {documento[:3]}***")
                return None
        else:
            logger.warning(f"⚠️ No se encontró item para documento: {documento[:3]}***")
            return None
            
    except ClientError as e:
        logger.error(f"Error de DynamoDB: {e.response['Error']['Message']}")
        return None
    except Exception as e:
        logger.error(f"Error recuperando refresh token: {str(e)}", exc_info=True)
        return None


def call_refresh_token_api(refresh_token):
    """
    Llama al API para refrescar el token JWT
    Implementa exponential backoff para manejar intermitencias de red
    
    Endpoint: POST /auth/refresh-token
    Body: {
        "refreshToken": "..."
    }
    
    Response esperado:
    {
        "success": true,
        "message": "Token refrescado exitosamente",
        "data": {
            "token": "nuevo_jwt_token",
            "refreshToken": "nuevo_refresh_token",
            "tokenType": "Bearer",
            "expiresIn": 86400
        }
    }
    
    Args:
        refresh_token: Refresh token para obtener un nuevo JWT
    
    Returns:
        dict: {
            'status_code': int,
            'data' (opcional)}
    """
    REFRESH_TOKEN_URL = f"{API_BASE_URL}/auth/refresh"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        "refreshToken": refresh_token
    }
    
    logger.info(f"=== Llamando API de Refresh Token (con exponential backoff) ===")
    logger.info(f"Endpoint: POST {REFRESH_TOKEN_URL}")
    logger.info(f"Payload: refreshToken con {len(refresh_token)} caracteres")
    logger.info(f"Max reintentos: {MAX_RETRIES}, Backoff inicial: {INITIAL_BACKOFF}s")
    
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"--- Intento {attempt + 1}/{MAX_RETRIES} ---")
            
            # Timeout de 15 segundos
            resp = requests.post(REFRESH_TOKEN_URL, json=payload, headers=headers, timeout=15)
            
            logger.info(f"Respuesta recibida - Status Code: {resp.status_code}")
            logger.info(f"Response headers: {dict(resp.headers)}")
            logger.info(f"Response content length: {len(resp.content)} bytes")
            
            # Verificar si la respuesta está vacía
            if not resp.content or len(resp.content) == 0:
                logger.error("Respuesta vacía del API")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 500,
                        'error': 'El API retornó una respuesta vacía después de múltiples intentos'
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"Respuesta vacía. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Verificar Content-Type
            content_type = resp.headers.get('Content-Type', '')
            logger.info(f"Content-Type de respuesta: {content_type}")
            
            if 'application/json' not in content_type.lower():
                logger.warning(f"Content-Type no es JSON: {content_type}")
                logger.warning(f"Respuesta completa: {resp.text[:500]}")
            
            # Intentar parsear JSON
            try:
                response_data = resp.json()
                logger.info(f"Response body parseado exitosamente: {json.dumps(response_data)}")
            except ValueError as json_err:
                logger.error(f"Respuesta no es JSON: {resp.text[:500]}")
                logger.error(f"Error al parsear: {str(json_err)}")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 500,
                        'error': f'Respuesta del API no es un JSON válido. Content-Type: {content_type}'
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Si llegamos aquí, la petición fue exitosa
            logger.info(f"✅ Llamada al API completada exitosamente en intento {attempt + 1}")
            
            return {
                'status_code': resp.status_code,
                'data': response_data
            }
            
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f"Timeout en intento {attempt + 1}/{MAX_RETRIES} (30 segundos)")
            
            # Si es el último intento, retornar error
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    'status_code': 200,
                    'data': {
                        'success': False,
                        'message': 'Tiempo de espera agotado al conectar con el API'
                    },
                    'error': f'No se pudo conectar con el API después de múltiples intentos debido a timeout: {str(e)}'
                
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f"Error de conexión en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, retornar error
            if attempt == MAX_RETRIES - 1:
                logger.error(f"Error de conexión después de {MAX_RETRIES} intentos")
                return {
                    'status_code': 200,
                    'success': False,
                    'message': 'No se pudo conectar con el API'
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.RequestException as e:

            last_exception = e
            logger.error(f"Error en la solicitud HTTP en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, retornar error
            if attempt == MAX_RETRIES - 1:

                logger.error(f"Error en solicitud HTTP después de {MAX_RETRIES} intentos")
                return {

                    'status_code': 200,
                    'success': False,
                    'message': 'Error en la solicitud HTTP al conectar con el API'
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except Exception as e:
            # Para errores inesperados, no reintentar
            logger.exception(f"Error inesperado en call_identity_validation_api: {str(e)}")
            return {
                'status_code': 200,
                'success': False,
                'message': 'Error inesperado al conectar con el API'
            }
    
    
    logger.error(f"Falló después de {MAX_RETRIES} intentos")
    return {
        'status_code': 500,
        'error': f'Error después de {MAX_RETRIES} intentos: {str(last_exception)}'
    }


def update_token_in_dynamodb(documento, token, refresh_token, token_type='Bearer', expires_in=86400):
    """
    Actualiza el token y refresh token en DynamoDB
    
    Args:
        documento: Número de documento del usuario
        token: Nuevo JWT token
        refresh_token: Nuevo refresh token
        token_type: Tipo de token (default: Bearer)
        expires_in: Tiempo de expiración en segundos (default: 86400 = 24h)
    
    Returns:
        bool: True si se actualizó correctamente, False si hubo error
    """
    if not documento or not token:
        logger.warning("Documento o token vacío, no se actualiza DynamoDB")
        return False
    
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        # TTL: expires_in segundos desde ahora
        ttl_timestamp = int(time.time()) + expires_in
        
        # Actualizar solo los campos del token
        response = table.update_item(
            Key={'documento': documento},
            UpdateExpression='SET #token = :token, refreshToken = :refreshToken, tokenType = :tokenType, updatedAt = :updatedAt, #ttl = :ttl',
            ExpressionAttributeNames={
                '#token': 'token',
                '#ttl': 'ttl'
            },
            ExpressionAttributeValues={
                ':token': token,
                ':refreshToken': refresh_token,
                ':tokenType': token_type,
                ':updatedAt': int(time.time()),
                ':ttl': ttl_timestamp
            },
            ReturnValues='UPDATED_NEW'
        )
        
        logger.info(f"✅ Token actualizado en DynamoDB: documento={documento[:3]}***, ttl={ttl_timestamp}")
        logger.debug(f"Updated attributes: {response.get('Attributes', {})}")
        return True
        
    except ClientError as e:
        logger.error(f"Error de DynamoDB: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        logger.error(f"Error actualizando token: {str(e)}", exc_info=True)
        return False


def format_bedrock_response(event, status_code, body):
    """
    Construye la respuesta en el formato esperado por Bedrock Agent
    
    Args:
        event: Evento original de Bedrock Agent
        status_code: HTTP status code
        body: Dict con los datos de respuesta
    
    Returns:
        dict en formato Bedrock Agent
    """
    logger.info(f"Formateando respuesta para Bedrock Agent - Status: {status_code}")
    
    formatted_response = {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get('actionGroup', 'ContarPredios'),
            "apiPath": event.get('apiPath', '/contar-predios'),
            "httpMethod": event.get('httpMethod', 'GET'),
            "httpStatusCode": status_code,
            "responseBody": {
                "application/json": {
                    "body": json.dumps(body, ensure_ascii=False)
                }
            }
        }
    }
    
    logger.info(f"Respuesta formateada: {json.dumps(formatted_response, ensure_ascii=False)}")
    return formatted_response