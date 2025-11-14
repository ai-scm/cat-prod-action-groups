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
import time
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente DynamoDB para obtener el token
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_NAME = 'cat-test-certification-session-tokens'

# URL base de la API
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://vmprocondock.catastrobogota.gov.co:3400/catia-auth')

# Configuración de reintentos con exponential backoff
MAX_RETRIES = 3
INITIAL_BACKOFF = 1  # segundos
MAX_BACKOFF = 8  # segundos

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
    
    # Extraer datos del evento - Bedrock Agent envía en requestBody
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
    
    # Obtener sessionId para recuperar token
    #session_id = event.get('sessionId', '')
    
    # Validación de inputs
    if not documento:
        logger.error("Documento vacío")
        return build_response(event, {
            "success": False,
            "message": "El documento es requerido",
            "data": {},
            "errorCode": "MISSING_DOCUMENTO"
        }, 400)
    
    logger.info(f"Contando predios para documento: {documento[:3]}***")
    
    try:
        # Obtener token de DynamoDB
        token = get_token_from_dynamodb(documento)
        
        if not token:
            logger.error("Token no encontrado en DynamoDB")
            return build_response(event, {
                "success": False,
                "message": "Sesión no autenticada. Por favor, valida tu identidad primero",
                "data": {},
                "errorCode": "TOKEN_NOT_FOUND"
            }, 401)
        
        logger.info("✅ Token recuperado de DynamoDB")
        
        # Llamar a la API de conteo de predios
        api_response = call_contar_predios_api(token)
        
        # Procesar respuesta
        if api_response['status_code'] == 200:
            logger.info("✅ API respondió exitosamente")
            response_data = api_response['data']
            
            response = {
                "success": response_data.get('success', True),
                "message": response_data.get('message', 'Predios consultados exitosamente'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', '')
            }
            
            logger.info(f"Response: {json.dumps(response)}")
            return build_response(event, response, 200)
        
        elif api_response['status_code'] == 405:
            # Usuario no activo
            logger.warning("⚠️ Usuario no se encuentra activo")
            response_data = api_response.get('data', {})
            
            response = {
                "success": False,
                "message": response_data.get('message', 'El usuario no se encuentra activo'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', 'USER_INACTIVE')
            }
            
            return build_response(event, response, 405)
        
        elif api_response['status_code'] == 406:
            # Preguntas de seguridad no diligenciadas
            logger.warning("⚠️ Usuario no ha diligenciado preguntas de seguridad")
            response_data = api_response.get('data', {})
            
            response = {
                "success": False,
                "message": response_data.get('message', 'El usuario no ha diligenciado las preguntas de seguridad'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', 'SECURITY_QUESTIONS_PENDING')
            }
            
            return build_response(event, response, 406)
        
        elif api_response['status_code'] == 401:
            # Token inválido o expirado
            logger.error("❌ Token inválido o expirado")
            
            response = {
                "success": False,
                "message": "Tu sesión ha expirado. Por favor, valida tu identidad nuevamente",
                "data": {},
                "errorCode": "TOKEN_EXPIRED"
            }
            
            return build_response(event, response, 401)
        
        else:
            # Otro error
            logger.error(f"❌ Error en API - Status: {api_response['status_code']}")
            error_message = api_response.get('error', 'Error al consultar los predios')
            
            response = {
                "success": False,
                "message": error_message,
                "data": {},
                "errorCode": "API_ERROR"
            }
            
            return build_response(event, response, api_response['status_code'])
        
    except requests.exceptions.Timeout:
        logger.error("Timeout llamando a la API de predios")
        return build_response(event, {
            "success": False,
            "message": "Error técnico: timeout al consultar predios",
            "data": {},
            "errorCode": "TIMEOUT"
        }, 502)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de red llamando a la API: {str(e)}")
        return build_response(event, {
            "success": False,
            "message": "Error técnico al consultar predios",
            "data": {},
            "errorCode": "NETWORK_ERROR"
        }, 502)
        
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}", exc_info=True)
        return build_response(event, {
            "success": False,
            "message": "Error interno al procesar la consulta",
            "data": {},
            "errorCode": "INTERNAL_ERROR"
        }, 500)


def get_token_from_dynamodb(documento):
    """
    Recupera el token JWT desde DynamoDB usando el documento de identidad
    
    Args:
        documento: ID de sesión del Bedrock Agent
    
    Returns:
        str: Token JWT o None si no se encuentra
    """
    if not documento:
        logger.warning("documento vacío, no se puede recuperar token")
        return None
    
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        response = table.get_item(Key={'documento': documento})
        
        if 'Item' in response:
            token = response['Item'].get('token', '')
            logger.info(f"Token encontrado en DynamoDB para documento: {documento}")
            logger.debug(f"Token (primeros 20 chars): {token[:20]}...")
            return token
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
    
    Endpoint: GET /properties/count
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
            
            logger.info(f"Status Code: {resp.status_code}")
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
                        'error': f'Respuesta del API no es un JSON válido después de múltiples intentos. Content-Type: {content_type}'
                    }
                
                # Aplicar backoff y reintentar
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Si llegamos aquí, la petición fue exitosa
            logger.info(f"Llamada al API completada exitosamente en intento {attempt + 1}")
            
            return {
                'status_code': resp.status_code,
                'data': response_data
            }
            
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f"Timeout en intento {attempt + 1}/{MAX_RETRIES} (15 segundos)")
            
            # Si es el último intento, lanzar excepción
            if attempt == MAX_RETRIES - 1:
                logger.error(f"❌ Timeout después de {MAX_RETRIES} intentos")
                raise
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f"Error de conexión en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, lanzar excepción
            if attempt == MAX_RETRIES - 1:
                logger.error(f"❌ Error de conexión después de {MAX_RETRIES} intentos")
                raise
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.RequestException as e:
            last_exception = e
            logger.error(f"Error de red en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, lanzar excepción
            if attempt == MAX_RETRIES - 1:
                logger.error(f"❌ Error de red después de {MAX_RETRIES} intentos")
                raise
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except Exception as e:
            # Para errores inesperados, no reintentar
            logger.error(f"Error inesperado en call_contar_predios_api: {str(e)}", exc_info=True)
            return {
                'status_code': 500,
                'error': f'Error inesperado: {str(e)}'
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f"❌ Falló después de {MAX_RETRIES} intentos")
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


def build_response(event, response_data, status_code=200):
    """
    Construye la respuesta en el formato esperado por Bedrock Agent
    
    Args:
        event: Evento original de Bedrock Agent
        response_data: Dict con los datos de respuesta
        status_code: HTTP status code (default: 200)
    
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
                    "body": json.dumps(response_data, ensure_ascii=False)
                }
            }
        }
    }
    
    logger.info(f"Respuesta formateada: {json.dumps(formatted_response, ensure_ascii=False)}")
    return formatted_response