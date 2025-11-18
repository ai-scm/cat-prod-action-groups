"""
Lambda Function: Validar OTP
Valida el código OTP ingresado por el usuario y guarda el token JWT en DynamoDB
"""
import json
import logging
import time
import re
import requests
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_NAME = 'cat-test-certification-session-tokens'

# Configuración de reintentos con exponential backoff
MAX_RETRIES = 10
INITIAL_BACKOFF = 1  # segundos
MAX_BACKOFF = 60  # segundos


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


def handler(event, context):
    """
    Valida código OTP con máximo 3 intentos y guarda el token JWT en DynamoDB
    
    Input esperado:
    {
        "documento": "1234567890",
        "codigo": "1233"
    }
    
    Output:
    {
        "success": true/false,
        "intentosRestantes": 2,
        "message": "descripción"
    }
    """
    logger.info("=== Lambda: Validar OTP ===")
    logger.info(f"Event: {json.dumps(event)}")
    
    # Extraer datos del evento - Bedrock Agent envía en requestBody
    if 'requestBody' in event and 'content' in event['requestBody']:
        content = event['requestBody']['content']
        if 'application/json' in content:
            # Bedrock Agent envía properties como array de objetos
            properties = content['application/json']['properties']
            body = {prop['name']: prop['value'] for prop in properties}
            documento = body.get('documento', '')
            codigo = body.get('codigo', '')
            tipo_documento = body.get('tipoDocumento', '')
        else:
            documento = ''
            codigo = ''
            tipo_documento = ''
    else:
        # Formato directo para testing
        documento = event.get('documento', '')
        codigo = event.get('codigo', '')
        tipo_documento = event.get('tipoDocumento', '')
    
    # Obtener sessionId para guardar token
    session_id = event.get('sessionId', '')
    
    # Log de parámetros extraídos
    logger.info("Parámetros extraídos del evento:")
    logger.info(f"  - sessionId: {session_id if session_id else '[VACÍO]'}")
    logger.info(f"  - documento: {documento[:3] if documento else '[VACÍO]'}*** (longitud: {len(documento)})")
    logger.info(f"  - codigo: {codigo[:2] if codigo else '[VACÍO]'}**** (longitud: {len(codigo)})")
    logger.info(f"  - tipoDocumento: {tipo_documento if tipo_documento else '[VACÍO]'}")
    
    # Validación de inputs
    if not documento or not codigo:
        logger.error("Documento o código vacío")
        return build_response(event, {
            "success": False,
            "intentosRestantes": 0,
            "message": "Documento y código son requeridos"
        }, 200)
    
    # Tipo de documento ya fue validado en lambda "ValidarIdentidad" (Paso 2)
    # Si viene vacío, usar 'CC' como fallback para compatibilidad
    if not tipo_documento:
        logger.warning("Tipo documento no proporcionado, usando 'CC' por defecto")
        tipo_documento = 'CC'
    
    logger.info(f"Validando OTP para documento: {tipo_documento}-{documento[:3]}***")
    logger.debug(f"Código OTP (debug): {codigo[:2]}****")
    
    try:
        # Llamar a la API de validación OTP
        api_response = call_validar_otp(documento, codigo, tipo_documento)
        
        # Procesar respuesta
        if api_response.get('success'):
            # OTP CORRECTO
            logger.info(" OTP validado correctamente")
            
            # Guardar token en DynamoDB
            token_saved = save_token_to_dynamodb(
                session_id=session_id,
                token=api_response.get('token'),
                documento=documento,
                tipo_documento=tipo_documento,
                usuario=api_response.get('usuario', {})
            )
            
            if token_saved:
                logger.info(f"Token guardado en DynamoDB para session: {session_id}")
            else:
                logger.warning("No se pudo guardar token en DynamoDB")
            
            response = {
                "success": True,
                "intentosRestantes": 3,  # Resetea intentos si fue exitoso
                "message": " Código OTP válido"
            }
        else:
            # OTP INCORRECTO
            intentos_restantes = api_response.get('intentosRestantes', 2)
            message = api_response.get('message', 'Código incorrecto')
            
            logger.warning(f" OTP incorrecto. Intentos restantes: {intentos_restantes}")
            
            response = {
                "success": False,
                "intentosRestantes": intentos_restantes,
                "message": message
            }
        
        logger.info(f"Response: {json.dumps(response)}")
        return build_response(event, response, 200)
        
    except requests.exceptions.Timeout:
        logger.error("Timeout llamando a la API de validación OTP")
        return build_response(event, {
            "success": False,
            "intentosRestantes": 0,
            "message": "Error técnico: timeout al validar código"
        }, 200)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de red llamando a la API: {str(e)}")
        return build_response(event, {
            "success": False,
            "intentosRestantes": 0,
            "message": "Error técnico al validar el código"
        }, 200)
        
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}", exc_info=True)
        return build_response(event, {
            "success": False,
            "intentosRestantes": 0,
            "message": "Error interno al procesar la validación"
        }, 200)


def call_validar_otp(documento, codigo, tipo_documento):
    """
    Llama a la API externa para validar el código OTP
    Implementa exponential backoff para manejar intermitencias de red.
    
    Endpoint: POST http://vmprocondock.catastrobogota.gov.co:3400/catia-auth/auth/login
    Endpoint: POST http://10.34.116.98:3400/catia-auth/auth/login

    Body:
    {
        "tipoDocumento": "CC | CE | NIT | PAS | etc.",
        "numeroDocumento": "12345678",
        "claveTemporal": "1234",
        "validInput": true
    }
    
    Args:
        documento: Número de documento del ciudadano
        codigo: Código OTP de 4 dígitos
        tipo_documento: Tipo de documento (CC, CE, NIT, PAS, etc.) - REQUERIDO
    
    Returns:
        dict con {valido, intentosRestantes, mensaje, token (opcional), usuario (opcional)}
    """
    URL = "http://vmprocondock.catastrobogota.gov.co:3400/catia-auth/auth/login"
    
    # Nota: tipo_documento ya fue validado en ValidarIdentidad (Paso 2)
    # Aquí solo lo usamos como dato heredado del flujo anterior
    
    payload = {
        "tipoDocumento": tipo_documento,
        "numeroDocumento": documento,
        "claveTemporal": codigo,
        "validInput": True
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    logger.info(f" Llamando API para validar OTP (con exponential backoff):")
    logger.info(f"  - Endpoint: POST {URL}")
    logger.info(f"  - Tipo documento: {tipo_documento} (heredado de ValidarIdentidad)")
    logger.info(f"  - Timeout: 10 segundos")
    logger.info(f"  - Max reintentos: {MAX_RETRIES}, Backoff inicial: {INITIAL_BACKOFF}s")
    logger.debug(f"  - Payload: {json.dumps(payload)}")
    
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"--- Intento {attempt + 1}/{MAX_RETRIES} ---")
            
            # Timeout de 10 segundos
            resp = requests.post(URL, json=payload, headers=headers, timeout=10)
            
            logger.info(f"  Respuesta recibida del API:")
            logger.info(f"  - Status Code: {resp.status_code}")
            logger.info(f"  - Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
            logger.info(f"  - Content-Length: {len(resp.content)} bytes")
            logger.info(f"  - Response (primeros 300 chars): {resp.text[:300]}")
            
            # Validar respuesta vacía
            if not resp.content or len(resp.content) == 0:
                logger.error(" API retornó respuesta vacía")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        "success": False,
                        "intentosRestantes": 0,
                        "message": "Error: El servidor retornó una respuesta vacía después de múltiples intentos"
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f" Respuesta vacía. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Validar Content-Type
            content_type = resp.headers.get('Content-Type', '')
            if 'application/json' not in content_type.lower():
                logger.warning(f" Content-Type no es JSON: {content_type}")
                logger.warning(f"Respuesta completa: {resp.text[:500]}")
            
            # Intentar parsear JSON
            try:
                response_data = resp.json()
                logger.info(f" JSON parseado exitosamente")
            except ValueError as ve:
                logger.error(f" Respuesta no es JSON válido: {resp.text[:200]}")
                logger.error(f"  - Error: {str(ve)}")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        "success": False,
                        "intentosRestantes": 0,
                        "message": "Error en la respuesta del servidor después de múltiples intentos"
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f" Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Si llegamos aquí, la petición fue exitosa
            logger.info(f" Llamada al API completada exitosamente en intento {attempt + 1}")
            
            # CASO 1: 200 OK - OTP CORRECTO
            if resp.status_code == 200 and response_data.get('success'):
                data = response_data.get('data', {})
                usuario = data.get('usuario', {})
                token = data.get('token', '')
                
                logger.info(" OTP VÁLIDO - API respondió exitosamente")
                logger.info(f" Token JWT recibido:")
                logger.info(f"  - Longitud: {len(token)} caracteres")
                logger.info(f"  - Primeros 30 chars: {token[:30]}...")
                logger.info(f"  - Tipo: {data.get('tokenType', 'Bearer')}")
                logger.info(f"  - Expira en: {data.get('expiresIn', 86400)} segundos")
                logger.info(f" Usuario:")
                logger.info(f"  - Nombre: {usuario.get('nombre', 'N/A')} {usuario.get('apellido', 'N/A')}")
                logger.info(f"  - Email: {usuario.get('email', 'N/A')}")
                
                return {
                    "success": True,
                    "intentosRestantes": 3,
                    "message": "Código OTP válido",
                    "token": token,
                    "refreshToken": data.get('refreshToken', ''),
                    "tokenType": data.get('tokenType', 'Bearer'),
                    "expiresIn": data.get('expiresIn', 86400),
                    "usuario": {
                        "nombre": usuario.get('nombre', ''),
                        "apellido": usuario.get('apellido', ''),
                        "email": usuario.get('email', ''),
                        "numeroDocumento": usuario.get('numeroDocumento', documento)
                    }
                }
            
            # CASO 2: 400/401 - OTP INCORRECTO (con intentos restantes)
            elif resp.status_code in [200, 200]:
                message = response_data.get('message', '')
                
                # Extraer intentos restantes del mensaje
                intentos = extract_intentos_from_message(message)
                
                # Si no se pudo extraer, verificar campo directo
                if intentos is None:
                    intentos = response_data.get('intentosRestantes', 2)
                
                return {
                    "success": False,
                    "intentosRestantes": intentos,
                    "message": f" Código incorrecto. Te quedan {intentos} intento(s)"
                }
            
            # CASO 3: 403 - CUENTA BLOQUEADA (0 intentos)
            elif resp.status_code == 200:
                message = response_data.get('message', 'Ha agotado los intentos')
                return {
                    "success": False,
                    "intentosRestantes": 0,
                    "message": " Has agotado los 3 intentos. Por seguridad, debes reiniciar el proceso"
                }
            
            # CASO 4: OTP EXPIRADO
            elif 'expirado' in response_data.get('message', '').lower():
                return {
                    "success": False,
                    "intentosRestantes": 0,
                    "message": " El código ha expirado. Debes solicitar uno nuevo"
                }
            
            # CASO 5: OTRO ERROR
            else:
                logger.error(f" Status code inesperado: {resp.status_code}, Body: {response_data}")
                return {
                    "success": False,
                    "intentosRestantes": 0,
                    "message": "Error al validar el código"
                }
        
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f" Timeout en intento {attempt + 1}/{MAX_RETRIES} (10 segundos)")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "intentosRestantes": 0,
                    "message": "Tiempo de espera agotado al validar el código OTP"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f" Error de conexión en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Error de conexión después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "intentosRestantes": 0,
                    "message": "No se pudo conectar con el servidor de validación"
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
                    "intentosRestantes": 0,
                    "message": "Error en la solicitud al validar el código OTP"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except Exception as e:
            logger.exception(f" Error inesperado en intento {attempt + 1}: {str(e)}")
            return {
                "success": False,
                "intentosRestantes": 0,
                "message": "Error inesperado al validar el código OTP"
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f" Falló después de {MAX_RETRIES} intentos")
    return {
        "success": False,
        "intentosRestantes": 0,
        "message": f"Error después de {MAX_RETRIES} intentos al validar el código OTP: {str(last_exception)}"
    }


def extract_intentos_from_message(message):
    """
    Extrae el número de intentos restantes del mensaje de error
    
    Ejemplos:
    - "Código incorrecto. Le quedan 2 intentos" → 2
    - "quedan 1 intento" → 1
    - "agotado los intentos" → 0
    
    Returns:
        int o None si no se puede extraer
    """
    if not message:
        return None
    
    # Buscar patrón: "quedan X intento(s)"
    match = re.search(r'quedan?\s+(\d+)\s+intento', message.lower())
    if match:
        return int(match.group(1))
    
    # Buscar patrón: "agotado"
    if 'agotado' in message.lower() or 'bloqueado' in message.lower():
        return 0
    
    return None


def save_token_to_dynamodb(session_id, token, documento, tipo_documento, usuario):
    """
    Guarda el token JWT en DynamoDB con TTL de 10 minutos
    
    Args:
        session_id: ID de sesión del Bedrock Agent
        token: JWT token de autenticación
        documento: Número de documento del ciudadano
        tipo_documento: Tipo de documento (CC, CE, NIT, PAS, etc.)
        usuario: Dict con datos del usuario (nombre, apellido, email)
    
    Returns:
        bool: True si se guardó correctamente, False si hubo error
    """
    logger.info(" Intentando guardar token en DynamoDB...")
    logger.info(f"  - Tabla: {TABLE_NAME}")
    logger.info(f"  - SessionId: {session_id if session_id else '[VACÍO]'}")
    logger.info(f"  - Token (longitud): {len(token) if token else 0} caracteres")
    logger.info(f"  - Documento: {tipo_documento}-{documento[:3] if documento else ''}***")
    
    if not session_id or not token:
        logger.error(" FALLO al guardar token - Validación de entrada")
        logger.error(f"  - SessionId vacío: {not session_id}")
        logger.error(f"  - Token vacío: {not token}")
        return False
    
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        # TTL: 10 minutos (600 segundos) - Alineado con Session TTL del Agent
        ttl_timestamp = int(time.time()) + 600
        
        item = {
            'documento': documento,           # PK - Primary Key
            'sessionId': session_id,          # Metadata de Bedrock Agent
            'token': token,
            'tipoDocumento': tipo_documento,
            'tokenType': 'Bearer',
            'createdAt': int(time.time()),
            'ttl': ttl_timestamp
        }
        
        # Agregar datos del usuario si existen
        if usuario:
            item['usuario'] = {
                'nombre': usuario.get('nombre', ''),
                'apellido': usuario.get('apellido', ''),
                'email': usuario.get('email', ''),
                'numeroDocumento': usuario.get('numeroDocumento', documento)
            }
        
        table.put_item(Item=item)
        
        logger.info(" Token guardado exitosamente en DynamoDB")
        logger.info(f"  - SessionId: {session_id}")
        logger.info(f"  - TTL: {ttl_timestamp} ({600} segundos = 10 minutos)")
        logger.info(f"  - Documento: {tipo_documento}-{documento[:3]}***")
        logger.info(f"  - Usuario: {usuario.get('nombre', 'N/A')} {usuario.get('apellido', 'N/A')}")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f" Error de DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        logger.error(f"  - Tabla: {TABLE_NAME}")
        logger.error(f"  - SessionId: {session_id}")
        return False
    except Exception as e:
        logger.error(f" Error inesperado guardando token")
        logger.error(f"  - Tipo de error: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return False


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
    logger.info(f" Construyendo respuesta para Bedrock Agent:")
    logger.info(f"  - Status Code: {status_code}")
    logger.info(f"  - Action Group: {event.get('actionGroup', 'ValidarOTP')}")
    logger.info(f"  - Response Body: {json.dumps(response_data, ensure_ascii=False)[:200]}...")
    
    formatted_response = {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get('actionGroup', 'ValidarOTP'),
            "apiPath": event.get('apiPath', '/validar-otp'),
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
