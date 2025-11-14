"""
Lambda Function: Buscar Predios
Busca predios específicos usando diferentes métodos: CHIP, Dirección o Matrícula.
Se utiliza cuando el usuario tiene más de 10 predios registrados (PASO 6 del flujo).
"""
import json
import logging
import requests
import boto3
from botocore.exceptions import ClientError
from urllib.parse import quote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_NAME = 'cat-test-certification-session-tokens'

# Base URL de la API
API_BASE_URL = "http://vmprocondock.catastrobogota.gov.co:3400/catia-auth"

# Mapeo de zonas a códigos de círculo registral
# Nota: este mapeo es el prefijo que siempre va antes del numero de la matrícula por ejemplo "050C00012345"
ZONA_TO_CIRCULO = {
    "NORTE": "050N",
    "CENTRO": "050C",
    "SUR": "050S"
}


def handler(event, context):
    """
    Busca un predio específico usando CHIP, Dirección o Matrícula.
    
    Input esperado:
    {
        "sessionId": "xxx",
        "metodo": "CHIP" | "DIRECCION" | "MATRICULA",
        "valor": "AAA-001-0001-0000-000" | "CRA 7 # 32-16" | "50C-12345",
        "zona": "Norte" | "Centro" | "Sur"  // Solo para MATRICULA
    }
    
    Output:
    {
        "success": true/false,
        "mensaje": "descripción",
        "predio": {
            "chip": "AAA-001-0001-0000-000",
            "direccion": "CRA 7 # 32-16",
            "matricula": "50C-12345",
            "tipo": "Urbano",
            "avaluo": 150000000,
            "area": 120.5,
            ...
        }  // Solo si success = true
    }
    """
    logger.info("=== Lambda: Buscar Predios ===")
    logger.info(f" Event recibido: {json.dumps(event, ensure_ascii=False)}")
    
    # Extraer parámetros - Bedrock Agent envía en requestBody
    if 'requestBody' in event and 'content' in event['requestBody']:
        content = event['requestBody']['content']
        if 'application/json' in content:
            properties = content['application/json']['properties']
            body = {prop['name']: prop['value'] for prop in properties}
            session_id = body.get('sessionId', event.get('sessionId', ''))
            metodo = body.get('metodo', '')
            valor = body.get('valor', '')
            zona = body.get('zona', '')
        else:
            session_id = event.get('sessionId', '')
            metodo = ''
            valor = ''
            zona = ''
    else:
        # Formato directo para testing
        session_id = event.get('sessionId', '')
        metodo = event.get('metodo', '')
        valor = event.get('valor', '')
        zona = event.get('zona', '')
    
    # Log de parámetros extraídos
    logger.info(" Parámetros extraídos del evento:")
    logger.info(f"  - sessionId: {session_id[:15] if session_id else '[VACÍO]'}***")
    logger.info(f"  - metodo: {metodo if metodo else '[VACÍO]'}")
    logger.info(f"  - valor: {valor if valor else '[VACÍO]'}")
    logger.info(f"  - zona: {zona if zona else '[N/A - no requerido para CHIP/DIRECCION]'}")
    
    # Validación de inputs
    if not session_id:
        logger.error("❌ SessionId vacío")
        return build_response(event, {
            "success": False,
            "mensaje": "SessionId es requerido"
        }, 400)
    
    if not metodo:
        logger.error("❌ Método de búsqueda vacío")
        return build_response(event, {
            "success": False,
            "mensaje": "Método de búsqueda es requerido (CHIP, DIRECCION, MATRICULA)"
        }, 400)
    
    if not valor:
        logger.error("❌ Valor de búsqueda vacío")
        return build_response(event, {
            "success": False,
            "mensaje": "Valor de búsqueda es requerido"
        }, 400)
    
    # Normalizar método a mayúsculas
    metodo = metodo.upper().strip()
    
    # Validar método
    metodos_validos = ["CHIP", "DIRECCION", "MATRICULA"]
    if metodo not in metodos_validos:
        logger.error(f"❌ Método inválido: {metodo}")
        return build_response(event, {
            "success": False,
            "mensaje": f"Método inválido. Debe ser uno de: {', '.join(metodos_validos)}"
        }, 400)
    
    # Validar zona si método es MATRICULA
    if metodo == "MATRICULA":
        if not zona:
            logger.error("❌ Zona requerida para búsqueda por MATRICULA")
            return build_response(event, {
                "success": False,
                "mensaje": "Zona es requerida para búsqueda por matrícula (Norte, Centro, Sur)"
            }, 400)
        
        # Normalizar zona
        zona = zona.upper().strip()
        
        if zona not in ZONA_TO_CIRCULO:
            logger.error(f"❌ Zona inválida: {zona}")
            return build_response(event, {
                "success": False,
                "mensaje": f"Zona inválida. Debe ser: Norte, Centro o Sur"
            }, 400)
    
    logger.info(f"🔍 Buscando predio por {metodo}: {valor[:20]}...")
    
    try:
        # 1. Obtener token JWT de DynamoDB
        logger.info(" PASO 1: Recuperando token JWT de DynamoDB...")
        token = get_token_from_dynamodb(session_id)
        
        if not token:
            logger.error("❌ Token no encontrado en DynamoDB")
            logger.error("  - Posibles causas:")
            logger.error("    1. Token expiró (TTL de 10 minutos)")
            logger.error("    2. SessionId incorrecto")
            logger.error("    3. Usuario no completó validación OTP")
            return build_response(event, {
                "success": False,
                "mensaje": "Token de autenticación no encontrado o expirado. Por favor reinicia el proceso."
            }, 401)
        
        # 2. Buscar predio en API según método
        logger.info(f"🔍 PASO 2: Buscando predio por {metodo}...")
        
        if metodo == "CHIP":
            api_response = buscar_por_chip(token, valor)
        elif metodo == "DIRECCION":
            api_response = buscar_por_direccion(token, valor)
        elif metodo == "MATRICULA":
            api_response = buscar_por_matricula(token, valor, zona)
        
        # 3. Procesar respuesta
        logger.info(f" PASO 3: Procesando respuesta de la API...")
        
        if api_response.get('success'):
            logger.info("✅ Predio encontrado exitosamente")
            
            predio_data = api_response.get('data', {})
            
            # Log de información del predio
            logger.info(" Información del predio encontrado:")
            for key in list(predio_data.keys())[:5]:  # Primeras 5 claves
                logger.info(f"  • {key}: {str(predio_data[key])[:50]}...")
            
            response = {
                "success": True,
                "mensaje": "Predio encontrado exitosamente",
                "predio": predio_data
            }
            
            return build_response(event, response, 200)
        else:
            # No se encontró el predio
            error_code = api_response.get('errorCode', 'PROPERTY_NOT_FOUND')
            mensaje = api_response.get('message', 'No se encontró el predio con los criterios especificados')
            
            logger.warning(f"⚠️ Predio no encontrado")
            logger.warning(f"  - Método: {metodo}")
            logger.warning(f"  - Valor: {valor}")
            logger.warning(f"  - Error: {error_code}")
            
            response = {
                "success": False,
                "mensaje": mensaje
            }
            
            return build_response(event, response, 200)  # 200 porque es un resultado válido (no encontrado)
        
    except requests.exceptions.Timeout:
        logger.error("❌ TIMEOUT: API no respondió a tiempo")
        return build_response(event, {
            "success": False,
            "mensaje": "Error técnico: timeout al buscar el predio. Por favor intenta nuevamente."
        }, 502)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ ERROR DE RED")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        return build_response(event, {
            "success": False,
            "mensaje": "Error técnico al buscar el predio. Verifica tu conexión."
        }, 502)
        
    except Exception as e:
        logger.error(f"❌ ERROR INESPERADO")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return build_response(event, {
            "success": False,
            "mensaje": "Error interno al procesar la búsqueda."
        }, 500)


def get_token_from_dynamodb(session_id):
    """
    Recupera el token JWT desde DynamoDB usando el sessionId.
    
    Args:
        session_id: ID de sesión del Bedrock Agent
    
    Returns:
        str: Token JWT o None si no se encuentra
    """
    if not session_id:
        logger.warning("⚠️ SessionId vacío")
        return None
    
    logger.info(" Recuperando token de DynamoDB...")
    logger.info(f"  - Tabla: {TABLE_NAME}")
    logger.info(f"  - SessionId: {session_id[:15]}***")
    
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        response = table.get_item(Key={'sessionId': session_id})
        
        if 'Item' not in response:
            logger.warning(f"⚠️ No se encontró token en DynamoDB")
            logger.warning(f"  - SessionId: {session_id[:15]}***")
            return None
        
        item = response['Item']
        token = item.get('token', '')
        
        if not token:
            logger.warning("⚠️ Token vacío en DynamoDB")
            return None
        
        logger.info(f"✅ Token recuperado exitosamente")
        logger.info(f"  - Token (longitud): {len(token)} caracteres")
        logger.info(f"  - Token (primeros 30 chars): {token[:30]}***")
        
        return token
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"❌ Error de DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        return None
    except Exception as e:
        logger.error(f"❌ Error inesperado obteniendo token")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return None


def buscar_por_chip(token, chip):
    """
    Busca un predio por su código CHIP.
    
    Endpoint: GET /properties/chip/{chip}
    Ejemplo: http://vmprocondock.catastrobogota.gov.co:3400/catia-auth/properties/chip/AAA1234ABCD
    
    Args:
        token: JWT token de autenticación
        chip: Código CHIP del predio (ej: "AAA-001-0001-0000-000")
    
    Returns:
        dict con {success, message, data (opcional), errorCode (opcional)}
    """
    # Limpiar CHIP (remover guiones si los tiene)
    chip_limpio = chip.replace("-", "").strip()
    
    URL = f"{API_BASE_URL}/properties/chip/{chip_limpio}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    logger.info(f" Llamando API de búsqueda por CHIP:")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - CHIP: {chip_limpio}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
    logger.info(f"  - Timeout: 15 segundos")
    
    try:
        resp = requests.get(URL, headers=headers, timeout=15)
        
        logger.info(f" Respuesta recibida:")
        logger.info(f"  - Status Code: {resp.status_code}")
        logger.info(f"  - Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        logger.info(f"  - Content-Length: {len(resp.content)} bytes")
        
        # Validar respuesta vacía
        if not resp.content or len(resp.content) == 0:
            logger.error("❌ API retornó respuesta vacía")
            return {
                "success": False,
                "message": "El servidor retornó una respuesta vacía",
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
            logger.error(f"  - Respuesta: {resp.text[:300]}")
            return {
                "success": False,
                "message": "Respuesta inválida del servidor",
                "errorCode": "INVALID_JSON"
            }
        
        # Procesar respuesta según status code
        if resp.status_code == 200:
            logger.info("✅ Status 200 - Predio encontrado")
            return {
                "success": response_data.get('success', True),
                "message": response_data.get('message', 'Predio encontrado'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', '')
            }
        
        elif resp.status_code == 404:
            logger.warning("⚠️ Status 404 - Predio no encontrado")
            return {
                "success": False,
                "message": response_data.get('message', 'No se encontró predio con el CHIP especificado'),
                "errorCode": response_data.get('errorCode', 'PROPERTY_NOT_FOUND')
            }
        
        elif resp.status_code == 401:
            logger.error("❌ Status 401 - Token inválido")
            return {
                "success": False,
                "message": "Token de autenticación inválido o expirado",
                "errorCode": "TOKEN_INVALID"
            }
        
        else:
            logger.error(f"❌ Status {resp.status_code} - Error inesperado")
            return {
                "success": False,
                "message": response_data.get('message', 'Error al buscar el predio'),
                "errorCode": response_data.get('errorCode', 'API_ERROR')
            }
        
    except requests.exceptions.Timeout:
        logger.error("❌ Timeout en búsqueda por CHIP")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error de red en búsqueda por CHIP: {str(e)}")
        raise


def buscar_por_direccion(token, direccion):
    """
    Busca un predio por su dirección.
    
    Endpoint: GET /properties/address/{address}
    Ejemplo: http://vmprocondock.catastrobogota.gov.co:3400/catia-auth/properties/address/CALLE%20123%20%23%2045-67
    
    Args:
        token: JWT token de autenticación
        direccion: Dirección del predio (ej: "CRA 7 # 32-16")
    
    Returns:
        dict con {success, message, data (opcional), errorCode (opcional)}
    """
    # URL encode de la dirección
    direccion_encoded = quote(direccion.strip())
    
    URL = f"{API_BASE_URL}/properties/address/{direccion_encoded}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    logger.info(f"📡 Llamando API de búsqueda por DIRECCIÓN:")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - Dirección original: {direccion}")
    logger.info(f"  - Dirección encoded: {direccion_encoded}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
    logger.info(f"  - Timeout: 15 segundos")
    
    try:
        resp = requests.get(URL, headers=headers, timeout=15)
        
        logger.info(f"📡 Respuesta recibida:")
        logger.info(f"  - Status Code: {resp.status_code}")
        logger.info(f"  - Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        logger.info(f"  - Content-Length: {len(resp.content)} bytes")
        
        # Validar respuesta vacía
        if not resp.content or len(resp.content) == 0:
            logger.error("❌ API retornó respuesta vacía")
            return {
                "success": False,
                "message": "El servidor retornó una respuesta vacía",
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
            logger.error(f"  - Respuesta: {resp.text[:300]}")
            return {
                "success": False,
                "message": "Respuesta inválida del servidor",
                "errorCode": "INVALID_JSON"
            }
        
        # Procesar respuesta según status code
        if resp.status_code == 200:
            logger.info("✅ Status 200 - Predio encontrado")
            return {
                "success": response_data.get('success', True),
                "message": response_data.get('message', 'Predio encontrado'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', '')
            }
        
        elif resp.status_code == 404:
            logger.warning("⚠️ Status 404 - Predio no encontrado")
            return {
                "success": False,
                "message": response_data.get('message', 'No se encontró información para la dirección especificada'),
                "errorCode": response_data.get('errorCode', 'ADDRESS_NOT_FOUND')
            }
        
        elif resp.status_code == 401:
            logger.error("❌ Status 401 - Token inválido")
            return {
                "success": False,
                "message": "Token de autenticación inválido o expirado",
                "errorCode": "TOKEN_INVALID"
            }
        
        else:
            logger.error(f"❌ Status {resp.status_code} - Error inesperado")
            return {
                "success": False,
                "message": response_data.get('message', 'Error al buscar el predio'),
                "errorCode": response_data.get('errorCode', 'API_ERROR')
            }
        
    except requests.exceptions.Timeout:
        logger.error("❌ Timeout en búsqueda por DIRECCIÓN")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error de red en búsqueda por DIRECCIÓN: {str(e)}")
        raise


def buscar_por_matricula(token, matricula, zona):
    """
    Busca un predio por su matrícula y zona (círculo registral).
    
    Endpoint: GET /properties/matricula/{idCirculo}/{matricula}
    Ejemplo: http://vmprocondock.catastrobogota.gov.co:3400/catia-auth/properties/matricula/CENTRO/1234
    
    Args:
        token: JWT token de autenticación
        matricula: Matrícula del predio (ej: "50C-12345" o "1234")
        zona: Zona del predio ("NORTE", "CENTRO", "SUR")
    
    Returns:
        dict con {success, message, data (opcional), errorCode (opcional)}
    
    Notas:
        - Círculos válidos: CENTRO (050C), NORTE (050N), SUR (050S)
        - La matrícula se convierte automáticamente (ej: 1234 → 00001234)
        - Código compuesto: 050C00001234
    """
    # Obtener ID del círculo
    id_circulo = zona  # Ya viene normalizado en mayúsculas del handler
    
    # Limpiar matrícula (remover prefijos de círculo si vienen)
    matricula_limpia = matricula.strip()
    for codigo in ["050C", "050N", "050S"]:
        if matricula_limpia.startswith(codigo):
            matricula_limpia = matricula_limpia[len(codigo):]
            break
    
    # Remover guiones si los tiene
    matricula_limpia = matricula_limpia.replace("-", "")
    
    URL = f"{API_BASE_URL}/properties/matricula/{id_circulo}/{matricula_limpia}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    logger.info(f" Llamando API de búsqueda por MATRÍCULA:")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - Matrícula original: {matricula}")
    logger.info(f"  - Matrícula limpia: {matricula_limpia}")
    logger.info(f"  - Zona: {zona}")
    logger.info(f"  - ID Círculo: {id_circulo}")
    logger.info(f"  - Código círculo esperado: {ZONA_TO_CIRCULO.get(zona, 'N/A')}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
    logger.info(f"  - Timeout: 15 segundos")
    
    try:
        resp = requests.get(URL, headers=headers, timeout=15)
        
        logger.info(f" Respuesta recibida:")
        logger.info(f"  - Status Code: {resp.status_code}")
        logger.info(f"  - Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
        logger.info(f"  - Content-Length: {len(resp.content)} bytes")
        
        # Validar respuesta vacía
        if not resp.content or len(resp.content) == 0:
            logger.error("❌ API retornó respuesta vacía")
            return {
                "success": False,
                "message": "El servidor retornó una respuesta vacía",
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
            logger.error(f"  - Respuesta: {resp.text[:300]}")
            return {
                "success": False,
                "message": "Respuesta inválida del servidor",
                "errorCode": "INVALID_JSON"
            }
        
        # Procesar respuesta según status code
        if resp.status_code == 200:
            logger.info("✅ Status 200 - Predio encontrado")
            return {
                "success": response_data.get('success', True),
                "message": response_data.get('message', 'Predio encontrado'),
                "data": response_data.get('data', {}),
                "errorCode": response_data.get('errorCode', '')
            }
        
        elif resp.status_code == 404:
            logger.warning("⚠️ Status 404 - Predio no encontrado")
            return {
                "success": False,
                "message": response_data.get('message', 'No se encontró predio con la matrícula especificada'),
                "errorCode": response_data.get('errorCode', 'PROPERTY_NOT_FOUND')
            }
        
        elif resp.status_code == 401:
            logger.error("❌ Status 401 - Token inválido")
            return {
                "success": False,
                "message": "Token de autenticación inválido o expirado",
                "errorCode": "TOKEN_INVALID"
            }
        
        else:
            logger.error(f"❌ Status {resp.status_code} - Error inesperado")
            return {
                "success": False,
                "message": response_data.get('message', 'Error al buscar el predio'),
                "errorCode": response_data.get('errorCode', 'API_ERROR')
            }
        
    except requests.exceptions.Timeout:
        logger.error("❌ Timeout en búsqueda por MATRÍCULA")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error de red en búsqueda por MATRÍCULA: {str(e)}")
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
    logger.info(f" Construyendo respuesta para Bedrock Agent:")
    logger.info(f"  - Status Code: {status_code}")
    logger.info(f"  - Action Group: {event.get('actionGroup', 'BuscarPredios')}")
    logger.info(f"  - Response Body (preview): {json.dumps(response_data, ensure_ascii=False)[:200]}...")
    
    formatted_response = {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get('actionGroup', 'BuscarPredios'),
            "apiPath": event.get('apiPath', '/buscar-predios'),
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
