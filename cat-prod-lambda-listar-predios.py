"""
Lambda Function: Listar Predios
Lista todos los predios asociados a un ciudadano cuando tiene entre 1 y 10 predios.
Este es el PASO 5 del flujo de Bedrock Agent.
El usuario podrá ver la lista completa y seleccionar hasta 3 predios para generar certificados.
"""
import json
import logging
import requests
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_NAME = 'cat-test-certification-session-tokens'

# Base URL de la API
API_BASE_URL = "http://vmprocondock.catastrobogota.gov.co:3400/catia-auth"


def handler(event, context):
    """
    Lista todos los predios asociados a un ciudadano (cuando tiene entre 1 y 10 predios).
    
    Input esperado:
    {
        "documento": "1234567890",  // REQUERIDO - Para recuperar token JWT
        "sessionId": "xxx"  // Opcional - Metadata
    }
    
    Output:
    {
        "success": true/false,
        "message": "Se encontraron X predios",
        "total": 5,
        "predios": [
            {
                "chip": "AAA-001-0001-0000-000",
                "direccion": "CRA 7 # 32-16",
                "matricula": "50C-12345",
                "tipo": "Urbano",
                "avaluoCatastral": 150000000,
                "area": 120.5,
                ...
            },
            ...
        ]
    }
    """
    logger.info("=== Lambda: Listar Predios ===")
    logger.info(f"📋 Event recibido: {json.dumps(event, ensure_ascii=False)}")
    
    # Extraer parámetros - Bedrock Agent envía en requestBody
    if 'requestBody' in event and 'content' in event['requestBody']:
        content = event['requestBody']['content']
        if 'application/json' in content:
            properties = content['application/json']['properties']
            body = {prop['name']: prop['value'] for prop in properties}
            documento = body.get('documento', '')
            session_id = body.get('sessionId', event.get('sessionId', ''))
        else:
            documento = ''
            session_id = event.get('sessionId', '')
    else:
        # Formato directo para testing
        documento = event.get('documento', '')
        session_id = event.get('sessionId', '')
    
    # Log de parámetros extraídos
    logger.info("📊 Parámetros extraídos del evento:")
    logger.info(f"  - documento (PK): {documento[:5] if documento else '[VACÍO]'}*** (longitud: {len(documento)})")
    logger.info(f"  - sessionId (metadata): {session_id[:15] if session_id else '[VACÍO]'}***")
    
    # Validación de inputs
    if not documento:
        logger.error("❌ Documento vacío")
        return build_response(event, {
            "success": False,
            "message": "Documento es requerido para recuperar el token de autenticación"
        }, 400)
    
    logger.info(f"📝 Listando predios para documento: {documento[:3]}***")
    
    try:
        # 1. Obtener token JWT de DynamoDB
        logger.info("🔐 PASO 1: Recuperando token JWT de DynamoDB...")
        token = get_token_from_dynamodb(documento)
        
        if not token:
            logger.error("❌ Token no encontrado en DynamoDB")
            logger.error("  - Posibles causas:")
            logger.error("    1. Token expiró (TTL de 10 minutos)")
            logger.error("    2. Documento incorrecto")
            logger.error("    3. Usuario no completó validación OTP")
            return build_response(event, {
                "success": False,
                "message": "Token de autenticación no encontrado o expirado. Por favor reinicia el proceso."
            }, 401)
        
        # 2. Listar predios desde la API
        logger.info(f"📋 PASO 2: Obteniendo lista de predios desde la API...")
        api_response = listar_predios_api(token)
        
        # 3. Procesar respuesta
        logger.info(f"✅ PASO 3: Procesando respuesta de la API...")
        
        if api_response.get('success'):
            predios = api_response.get('data', [])
            total = len(predios)
            
            logger.info(f"✅ Predios obtenidos exitosamente")
            logger.info(f"  - Total de predios: {total}")
            
            # Log de los primeros predios (para debugging)
            if predios and len(predios) > 0:
                logger.info(f"  - Primer predio: {predios[0].get('chip', 'N/A')}")
                if len(predios) > 1:
                    logger.info(f"  - Último predio: {predios[-1].get('chip', 'N/A')}")
            
            response = {
                "success": True,
                "message": f"Se encontraron {total} predio(s) asociados a tu documento",
                "total": total,
                "predios": predios
            }
            
            return build_response(event, response, 200)
        else:
            # Error en la API
            error_code = api_response.get('errorCode', 'API_ERROR')
            message = api_response.get('message', 'Error al obtener la lista de predios')
            
            logger.error(f"❌ Error en la API")
            logger.error(f"  - Error Code: {error_code}")
            logger.error(f"  - Mensaje: {message}")
            
            response = {
                "success": False,
                "message": message,
                "total": 0,
                "predios": []
            }
            
            return build_response(event, response, 200)
        
    except requests.exceptions.Timeout:
        logger.error("❌ TIMEOUT: API no respondió a tiempo")
        return build_response(event, {
            "success": False,
            "message": "Error técnico: timeout al obtener la lista de predios. Por favor intenta nuevamente.",
            "total": 0,
            "predios": []
        }, 502)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ ERROR DE RED")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        return build_response(event, {
            "success": False,
            "message": "Error técnico al obtener la lista de predios. Verifica tu conexión.",
            "total": 0,
            "predios": []
        }, 502)
        
    except Exception as e:
        logger.error(f"❌ ERROR INESPERADO")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return build_response(event, {
            "success": False,
            "message": "Error interno al procesar la solicitud.",
            "total": 0,
            "predios": []
        }, 500)


def get_token_from_dynamodb(documento):
    """
    Recupera el token JWT desde DynamoDB usando el documento.
    
    Args:
        documento: Número de documento del ciudadano (PK en DynamoDB)
    
    Returns:
        str: Token JWT o None si no se encuentra
    """
    if not documento:
        logger.warning("⚠️ Documento vacío")
        return None
    
    logger.info("💾 Recuperando token de DynamoDB...")
    logger.info(f"  - Tabla: {TABLE_NAME}")
    logger.info(f"  - Documento (PK): {documento[:3]}*** (longitud: {len(documento)})")
    
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        response = table.get_item(Key={'documento': documento})
        
        if 'Item' not in response:
            logger.warning(f"⚠️ No se encontró token en DynamoDB")
            logger.warning(f"  - Documento: {documento[:3]}***")
            return None
        
        item = response['Item']
        token = item.get('token', '')
        
        if not token:
            logger.warning("⚠️ Token vacío en DynamoDB")
            return None
        
        logger.info(f"✅ Token recuperado exitosamente")
        logger.info(f"  - Token (longitud): {len(token)} caracteres")
        logger.info(f"  - Token (primeros 30 chars): {token[:30]}***")
        logger.info(f"  - Documento: {documento[:3]}***")
        
        return token
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"❌ Error de DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        logger.error(f"  - Documento: {documento[:3]}***")
        return None
    except Exception as e:
        logger.error(f"❌ Error inesperado obteniendo token")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return None


def listar_predios_api(token):
    """
    Obtiene la lista completa de predios asociados al usuario desde la API.
    
    Endpoint: GET /properties
    Ejemplo: http://vmprocondock.catastrobogota.gov.co:3400/catia-auth/properties
    
    Args:
        token: JWT token de autenticación
    
    Returns:
        dict con {success, message, data (array de predios), errorCode (opcional)}
    """
    URL = f"{API_BASE_URL}/properties"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    logger.info(f"📞 Llamando API para listar predios:")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
    logger.info(f"  - Timeout: 15 segundos")
    
    try:
        resp = requests.get(URL, headers=headers, timeout=15)
        
        logger.info(f"📥 Respuesta recibida:")
        logger.info(f"  - Status Code: {resp.status_code}")
        logger.info(f"  - Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        logger.info(f"  - Content-Length: {len(resp.content)} bytes")
        
        # Validar respuesta vacía
        if not resp.content or len(resp.content) == 0:
            logger.error("❌ API retornó respuesta vacía")
            return {
                "success": False,
                "message": "El servidor retornó una respuesta vacía",
                "data": [],
                "errorCode": "EMPTY_RESPONSE"
            }
        
        # Validar Content-Type
        content_type = resp.headers.get('Content-Type', '')
        if 'application/json' not in content_type.lower():
            logger.warning(f"⚠️ Content-Type no es JSON: {content_type}")
        
        # Parsear JSON
        try:
            response_data = resp.json()
            logger.info(f"✅ JSON parseado exitosamente")
            logger.info(f"  - Claves: {list(response_data.keys())}")
        except ValueError as ve:
            logger.error(f"❌ Respuesta no es JSON válido")
            logger.error(f"  - Error: {str(ve)}")
            logger.error(f"  - Respuesta (primeros 300 chars): {resp.text[:300]}")
            return {
                "success": False,
                "message": "Respuesta inválida del servidor",
                "data": [],
                "errorCode": "INVALID_JSON"
            }
        
        # Procesar respuesta según status code
        if resp.status_code == 200:
            logger.info("✅ Status 200 - Predios obtenidos exitosamente")
            
            # Extraer array de predios del campo 'data'
            predios = response_data.get('data', [])
            
            # Asegurar que sea una lista
            if not isinstance(predios, list):
                logger.warning(f"⚠️ 'data' no es un array, es: {type(predios)}")
                predios = []
            
            logger.info(f"  - Total de predios en respuesta: {len(predios)}")
            
            return {
                "success": response_data.get('success', True),
                "message": response_data.get('message', f'Se encontraron {len(predios)} predio(s)'),
                "data": predios,
                "errorCode": response_data.get('errorCode', '')
            }
        
        elif resp.status_code == 404:
            logger.warning("⚠️ Status 404 - No se encontraron predios")
            return {
                "success": False,
                "message": response_data.get('message', 'No se encontraron predios asociados a tu documento'),
                "data": [],
                "errorCode": response_data.get('errorCode', 'NO_PROPERTIES_FOUND')
            }
        
        elif resp.status_code == 401:
            logger.error("❌ Status 401 - Token inválido")
            return {
                "success": False,
                "message": "Token de autenticación inválido o expirado",
                "data": [],
                "errorCode": "TOKEN_INVALID"
            }
        
        else:
            logger.error(f"❌ Status {resp.status_code} - Error inesperado")
            return {
                "success": False,
                "message": response_data.get('message', 'Error al obtener la lista de predios'),
                "data": [],
                "errorCode": response_data.get('errorCode', 'API_ERROR')
            }
        
    except requests.exceptions.Timeout:
        logger.error("❌ Timeout al listar predios")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error de red al listar predios: {str(e)}")
        raise


def build_response(event, response_data, status_code=200):
    """
    Construye la respuesta en el formato esperado por Bedrock Agent.
    
    Args:
        event: Evento original de Bedrock Agent
        response_data: Dict con los datos de respuesta
        status_code: HTTP status code (default: 200)
    
    Returns:
        dict en formato Bedrock Agent
    """
    logger.info(f"🔧 Construyendo respuesta para Bedrock Agent:")
    logger.info(f"  - Status Code: {status_code}")
    logger.info(f"  - Action Group: {event.get('actionGroup', 'ListarPredios')}")
    
    # Log preview de la respuesta (sin incluir array completo de predios para no saturar logs)
    preview_data = response_data.copy()
    if 'predios' in preview_data and isinstance(preview_data['predios'], list) and len(preview_data['predios']) > 2:
        preview_data['predios'] = f"[{len(preview_data['predios'])} predios]"
    logger.info(f"  - Response Body (preview): {json.dumps(preview_data, ensure_ascii=False)[:200]}...")
    
    formatted_response = {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get('actionGroup', 'ListarPredios'),
            "apiPath": event.get('apiPath', '/listar-predios'),
            "httpMethod": event.get('httpMethod', 'POST'),
            "httpStatusCode": status_code,
            "responseBody": {
                "application/json": {
                    "body": json.dumps(response_data, ensure_ascii=False)
                }
            }
        }
    }
    
    logger.info("✅ Respuesta formateada correctamente")
    return formatted_response
