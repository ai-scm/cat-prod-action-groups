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
import time
import random
import os
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_NAME = 'cat-test-certification-session-tokens'
MOCK_USERS_TABLE = 'cat-test-mock-users'

# Base URL de la API
API_BASE_URL = "http://vmprocondock.catastrobogota.gov.co:3400/catia-auth"

# Configuración de reintentos con exponential backoff
MAX_RETRIES = 10
INITIAL_BACKOFF = 1  # segundos
MAX_BACKOFF = 60  # segundos

# ============================================================
# CONFIGURACIÓN DE MODO MOCK
# ============================================================
ENABLE_MOCK = os.environ.get('ENABLE_MOCK', 'false').lower() == 'true'

# Usuarios mock para testing (2 usuarios configurables)
MOCK_USERS = {
    "123456789": {
        "nombre": "Juan Carlos",
        "apellido": "Rodríguez",
        "email": "juan.rodriguez@catastro.test",
        "prediosCount": 3,
        "predios": [
            {
                "chip": "AAA0000001ABC",
                "direccion": "KR 7 6 16 SUR IN 3 AP 301",
                "matricula": "50C-123456",
                "tipo": "Urbano",
                "avaluoCatastral": 150000000,
                "area": 85.5,
                "destino": "Apartamento",
                "estrato": 3
            },
            {
                "chip": "AAA0000002DEF",
                "direccion": "CL 45 23 10 APTO 501",
                "matricula": "50C-654321",
                "tipo": "Urbano",
                "avaluoCatastral": 200000000,
                "area": 120.0,
                "destino": "Apartamento",
                "estrato": 4
            },
            {
                "chip": "AAA0000003GHI",
                "direccion": "KR 15 80 30 CASA",
                "matricula": "50C-789012",
                "tipo": "Urbano",
                "avaluoCatastral": 350000000,
                "area": 180.5,
                "destino": "Casa",
                "estrato": 5
            }
        ]
    },
    "987654321": {
        "nombre": "María Elena",
        "apellido": "González",
        "email": "maria.gonzalez@catastro.test",
        "prediosCount": 15,
        "predios": [
            {
                "chip": f"BBB000000{i:01d}XYZ",
                "direccion": f"CALLE {10 + i} # {5 + i}-{20 + i}",
                "matricula": f"50C-{100000 + i}",
                "tipo": "Urbano",
                "avaluoCatastral": 100000000 + (i * 10000000),
                "area": 50.0 + (i * 5.5),
                "destino": "Local" if i % 2 == 0 else "Apartamento",
                "estrato": 2 + (i % 4)
            }
            for i in range(1, 16)  # 15 predios para simular flujo BuscarPredios
        ]
    }
}

logger.info(f"[MOCK CONFIG] ENABLE_MOCK = {ENABLE_MOCK}")
if ENABLE_MOCK:
    logger.info(f"[MOCK CONFIG] Usuarios mock configurados: {list(MOCK_USERS.keys())}")
    logger.info(f"[MOCK CONFIG] Usuario 1 tiene {MOCK_USERS['123456789']['prediosCount']} predios")
    logger.info(f"[MOCK CONFIG] Usuario 2 tiene {MOCK_USERS['987654321']['prediosCount']} predios")


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


def get_mock_predios_list(documento):
    """
    Genera una respuesta simulada para testing sin llamar al API externo.
    Retorna la lista de predios configurada para el usuario mock.
    
    Args:
        documento: Número de documento del usuario
    
    Returns:
        dict: Respuesta simulada del API con lista de predios
    """
    logger.info("[MOCK] 🎭 Generando respuesta mock para ListarPredios")
    logger.info(f"[MOCK] Documento: {documento[:3]}***")
    
    # Simular delay realista del API (0.5s - 2s)
    delay = random.uniform(0.5, 2.0)
    dynamo_mock_table = dynamodb.Table(MOCK_USERS_TABLE)
    logger.info(f"[MOCK] Simulando delay de {delay:.2f} segundos...")
    time.sleep(delay)
    
    response = dynamo_mock_table.get_item(Key={'documento': documento})
    mock_user = response.get('Item', None)
    num_predios = int(mock_user.get('numPredios', 0)) if mock_user else 0
    logger.info(f"[MOCK] Usuario mock en DynamoDB tiene numPredios = {num_predios}")

    predios = []
    for i in range(1, num_predios + 1):
        predio = {
            "chip": f"MOCKCHIP{i:04d}",
            "direccion": f"CALLE MOCK {i} # {i*10}-{i*5}",
            "matricula": f"50C-MOCK{i:05d}",
            "tipo": "Urbano",
            "avaluoCatastral": 100000000 + (i * 5000000),
            "area": 60.0 + (i * 2.5),
            "destino": "Apartamento" if i % 2 == 0 else "Casa",
            "estrato": 3 + (i % 3)
        }
        predios.append(predio)
    
    return {
        'status_code': 200,
        'data': {
            "success": True,
            "message": f"Se encontraron {len(predios)} predio(s) asociado(s) a tu documento (MOCK - DynamoDB)",
            "data": {
                "total": len(predios),
                "predios": predios,
                "mockMode": True
            }
        }
    }


    # Verificar si el usuario existe en MOCK_USERS
    if documento in MOCK_USERS:
        user_data = MOCK_USERS[documento]
        predios = user_data["predios"]
        
        logger.info(f"[MOCK] ✅ Usuario encontrado en MOCK_USERS")
        logger.info(f"[MOCK] Nombre: {user_data['nombre']} {user_data['apellido']}")
        logger.info(f"[MOCK] Predios configurados: {len(predios)}")
        
        return {
            'status_code': 200,
            'data': {
                "success": True,
                "message": f"Se encontraron {len(predios)} predio(s) asociado(s) a tu documento (MOCK)",
                "data": {
                    "total": len(predios),
                    "predios": predios,
                    "mockMode": True
                }
            }
        }
    else:
        # Usuario no existe en MOCK - Retornar default de 2 predios genéricos
        logger.info(f"[MOCK] ⚠️ Usuario NO encontrado en MOCK_USERS")
        logger.info(f"[MOCK] Usando 2 predios genéricos por defecto")
        
        predios_default = [
            {
                "chip": "XXX0000001DEF",
                "direccion": "CALLE 100 # 10-20 APTO 101",
                "matricula": "50C-999001",
                "tipo": "Urbano",
                "avaluoCatastral": 180000000,
                "area": 95.0,
                "destino": "Apartamento",
                "estrato": 4
            },
            {
                "chip": "XXX0000002GHI",
                "direccion": "KR 50 # 25-30 CASA",
                "matricula": "50C-999002",
                "tipo": "Urbano",
                "avaluoCatastral": 250000000,
                "area": 150.0,
                "destino": "Casa",
                "estrato": 5
            }
        ]
        
        return {
            'status_code': 200,
            'data': {
                "success": True,
                "message": f"Se encontraron {len(predios_default)} predio(s) asociado(s) a tu documento (MOCK - usuario genérico)",
                "data": {
                    "total": len(predios_default),
                    "predios": predios_default,
                    "mockMode": True,
                    "advertencia": "Este usuario no está en MOCK_USERS, usando predios genéricos"
                }
            }
        }


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
    logger.info(f" Event recibido: {json.dumps(event, ensure_ascii=False)}")
    
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
    logger.info(" Parámetros extraídos del evento:")
    logger.info(f"  - documento (PK): {documento[:5] if documento else '[VACÍO]'}*** (longitud: {len(documento)})")
    logger.info(f"  - sessionId (metadata): {session_id[:15] if session_id else '[VACÍO]'}***")
    
    # Validación de inputs
    if not documento:
        logger.error("Documento vacío")
        return build_response(event, {
            "success": False,
            "message": "Documento es requerido para recuperar el token de autenticación"
        }, 400)
    
    logger.info(f" Listando predios para documento: {documento[:3]}***")
    
    try:
        if ENABLE_MOCK:
            logger.info("[MOCK] 🎭 MODO MOCK ACTIVADO - Saltando validación de token")
            logger.info("[MOCK] No se validará token ni se recuperará de DynamoDB")
            logger.info("[MOCK] Generando respuesta simulada directamente...")
            
            api_response = get_mock_predios_list(documento)
            
        else:
            # MODO REAL: Validar token y llamar al API externo
            logger.info("📡 MODO REAL - Validando token y llamando API externo")
            
            # 0. Validar y refrescar token si es necesario
            logger.info("Validando token")
            validate_token_response = validate_token(documento)

            if not validate_token_response['success']:
                logger.error(f"Token inválido: {validate_token_response.get('message')}")
                return build_response(
                    event=event,
                    body={
                        "success": False,
                        "message": "Tu sesión ha expirado. Por favor, valida tu identidad nuevamente",
                        "data": {},
                        "errorCode": "TOKEN_EXPIRED"
                    },
                    status_code=401
                )
            
            logger.info("Token validado exitosamente")

            # 1. Obtener token JWT de DynamoDB
            logger.info("Iniciando recuperación de token desde DynamoDB")
            token_dict = get_token_from_dynamodb(documento)
            token = token_dict.get('token', '') if token_dict else ''
            
            if not token:
                logger.error("Token no encontrado en DynamoDB")
                logger.error("  - Posibles causas:")
                logger.error("    1. Token expiró (TTL de 10 minutos)")
                logger.error("    2. Documento incorrecto")
                logger.error("    3. Usuario no completó validación OTP")
                return build_response(event, {
                    "success": False,
                    "message": "Token de autenticación no encontrado o expirado. Por favor reinicia el proceso."
                }, 401)
            
            logger.info("Token recuperado de DynamoDB")

            # 2. Listar predios desde la API REAL
            logger.info("📡 Llamando API externa REAL")
            api_response = listar_predios_api(token)
        
        # 3. Procesar respuesta
        logger.info(f" PASO 3: Procesando respuesta de la API...")
        
        # Normalizar respuesta (MOCK retorna {status_code, data}, API real puede retornar directo)
        if api_response.get('status_code') == 200:
            # Formato con status_code (MOCK o API con wrapper)
            response_data = api_response.get('data', {})
            success = response_data.get('success', True)
            if success:
                # Extraer predios del objeto data anidado
                data_obj = response_data.get('data', {})
                predios = data_obj.get('predios', [])
            else:
                predios = []
        else:
            # Formato directo (legacy o error)
            success = api_response.get('success', False)
            predios = api_response.get('data', [])
        
        if success and predios:
            total = len(predios)
            
            logger.info(f" Predios obtenidos exitosamente")
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
            # Extraer mensaje de error según el formato
            if api_response.get('status_code'):
                response_data = api_response.get('data', {})
                error_code = response_data.get('errorCode', 'API_ERROR')
                message = response_data.get('message', 'Error al obtener la lista de predios')
            else:
                error_code = api_response.get('errorCode', 'API_ERROR')
                message = api_response.get('message', 'Error al obtener la lista de predios')
            
            logger.error(f" Error en la API")
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
        logger.error(" TIMEOUT: API no respondió a tiempo")
        return build_response(event, {
            "success": False,
            "message": "Error técnico: timeout al obtener la lista de predios. Por favor intenta nuevamente.",
            "total": 0,
            "predios": []
        }, 200)
        
    except requests.exceptions.RequestException as e:
        logger.error(f" ERROR DE RED")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        return build_response(event, {
            "success": False,
            "message": "Error técnico al obtener la lista de predios. Verifica tu conexión.",
            "total": 0,
            "predios": []
        }, 200)
        
    except Exception as e:
        logger.error(f" ERROR INESPERADO")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return build_response(event, {
            "success": False,
            "message": "Error interno al procesar la solicitud.",
            "total": 0,
            "predios": []
        }, 200)

def listar_predios_api(token):
    """
    Obtiene la lista completa de predios asociados al usuario desde la API.
    Implementa exponential backoff para manejar intermitencias de red.
    
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
    
    logger.info(f" Llamando API para listar predios (con exponential backoff):")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
    logger.info(f"  - Timeout: 15 segundos")
    logger.info(f"  - Max reintentos: {MAX_RETRIES}, Backoff inicial: {INITIAL_BACKOFF}s")
    
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"--- Intento {attempt + 1}/{MAX_RETRIES} ---")
            
            resp = requests.get(URL, headers=headers, timeout=15)
            
            logger.info(f" Respuesta recibida:")
            logger.info(f"  - Status Code: {resp.status_code}")
            logger.info(f"  - Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
            logger.info(f"  - Content-Length: {len(resp.content)} bytes")
            
            # Validar respuesta vacía
            if not resp.content or len(resp.content) == 0:
                logger.error(" API retornó respuesta vacía")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        "success": False,
                        "message": "El servidor retornó una respuesta vacía después de múltiples intentos",
                        "data": [],
                        "errorCode": "EMPTY_RESPONSE"
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f" Respuesta vacía. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Validar Content-Type
            content_type = resp.headers.get('Content-Type', '')
            if 'application/json' not in content_type.lower():
                logger.warning(f" Content-Type no es JSON: {content_type}")
            
            # Parsear JSON
            try:
                response_data = resp.json()
                logger.info(f" JSON parseado exitosamente")
                logger.info(f"  - Claves: {list(response_data.keys())}")
            except ValueError as ve:
                logger.error(f" Respuesta no es JSON válido")
                logger.error(f"  - Error: {str(ve)}")
                logger.error(f"  - Respuesta (primeros 300 chars): {resp.text[:300]}")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        "success": False,
                        "message": "Respuesta inválida del servidor después de múltiples intentos",
                        "data": [],
                        "errorCode": "INVALID_JSON"
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f" Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Si llegamos aquí, la petición fue exitosa
            logger.info(f" Llamada al API completada exitosamente en intento {attempt + 1}")
            
            # Procesar respuesta según status code
            if resp.status_code == 200:
                logger.info("Status 200 - Predios obtenidos exitosamente")
                
                # Extraer array de predios del campo 'data'
                predios = response_data.get('data', [])
                
                # Asegurar que sea una lista
                if not isinstance(predios, list):
                    logger.warning(f" 'data' no es un array, es: {type(predios)}")
                    predios = []
                
                logger.info(f"  - Total de predios en respuesta: {len(predios)}")
                
                return {
                    "success": response_data.get('success', True),
                    "message": response_data.get('message', f'Se encontraron {len(predios)} predio(s)'),
                    "data": predios,
                    "errorCode": response_data.get('errorCode', '')
                }
            
            # elif resp.status_code == 404:
            #     logger.warning("⚠️ Status 404 - No se encontraron predios")
            #     return {
            #         "success": False,
            #         "message": response_data.get('message', 'No se encontraron predios asociados a tu documento'),
            #         "data": [],
            #         "errorCode": response_data.get('errorCode', 'NO_PROPERTIES_FOUND')
            #     }
            
            # elif resp.status_code == 401:
            #     logger.error("❌ Status 401 - Token inválido")
            #     return {
            #         "success": False,
            #         "message": "Token de autenticación inválido o expirado",
            #         "data": [],
            #         "errorCode": "TOKEN_INVALID"
            #     }
            
            else:
                logger.error(f" Status {resp.status_code} - Error inesperado")
                return {
                    "success": False,
                    "message": response_data.get('message', 'Error al obtener la lista de predios'),
                    "data": [],
                    "errorCode": response_data.get('errorCode', 'API_ERROR')
                }
        
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f" Timeout en intento {attempt + 1}/{MAX_RETRIES} (15 segundos)")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "Tiempo de espera agotado al obtener la lista de predios",
                    "data": [],
                    "errorCode": "TIMEOUT"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f" Error de conexión en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f"❌ Error de conexión después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "No se pudo conectar con el servidor",
                    "data": [],
                    "errorCode": "CONNECTION_ERROR"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except requests.exceptions.RequestException as e:
            last_exception = e
            logger.error(f" Error en la solicitud HTTP en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Error en solicitud HTTP después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "Error en la solicitud HTTP al obtener la lista de predios",
                    "data": [],
                    "errorCode": "REQUEST_ERROR"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except Exception as e:
            logger.exception(f" Error inesperado en intento {attempt + 1}: {str(e)}")
            return {
                "success": False,
                "message": "Error inesperado al obtener la lista de predios",
                "data": [],
                "errorCode": "UNEXPECTED_ERROR"
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f" Falló después de {MAX_RETRIES} intentos")
    return {
        "success": False,
        "message": f"Error después de {MAX_RETRIES} intentos: {str(last_exception)}",
        "data": [],
        "errorCode": "MAX_RETRIES_EXCEEDED"
    }


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
    logger.info(f" Construyendo respuesta para Bedrock Agent:")
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
    
    logger.info(" Respuesta formateada correctamente")
    return formatted_response


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
