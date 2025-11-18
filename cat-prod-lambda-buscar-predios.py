"""
Lambda Function: Buscar Predios
Busca predios específicos usando diferentes métodos: CHIP, Dirección o Matrícula.
Se utiliza cuando el usuario tiene más de 10 predios registrados (PASO 6 del flujo).
"""
import json
import logging
import requests
import boto3
import time
from botocore.exceptions import ClientError
from urllib.parse import quote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_NAME = 'cat-test-certification-session-tokens'

# Base URL de la API
API_BASE_URL = "http://vmprocondock.catastrobogota.gov.co:3400/catia-auth"

# Configuración de reintentos con exponential backoff
MAX_RETRIES = 10
INITIAL_BACKOFF = 1  # segundos
MAX_BACKOFF = 60  # segundos

# Mapeo de zonas a códigos de círculo registral
# Nota: este mapeo es el prefijo que siempre va antes del numero de la matrícula por ejemplo "050C00012345"
ZONA_TO_CIRCULO = {
    "NORTE": "050N",
    "CENTRO": "050C",
    "SUR": "050S"
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


def handler(event, context):
    """
    Busca un predio específico usando CHIP, Dirección o Matrícula.
    
    Input esperado:
    {
        "documento": "1234567890",  // REQUERIDO - Para recuperar token JWT
        "sessionId": "xxx",  // Opcional - Metadata
        "metodo": "CHIP" | "DIRECCION" | "MATRICULA",
        "valor": "AAA-001-0001-0000-000" | "CRA 7 # 32-16" | "50C-12345",
        "zona": "Norte" | "Centro" | "Sur"  // Solo para MATRICULA
    }
    
    Output:
    {
        "success": true/false,
        "message": "descripción",
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
            documento = body.get('documento', '')
            metodo = body.get('metodo', '')
            valor = body.get('valor', '')
            zona = body.get('zona', '')
        else:
            session_id = event.get('sessionId', '')
            documento = ''
            metodo = ''
            valor = ''
            zona = ''
    else:
        # Formato directo para testing
        session_id = event.get('sessionId', '')
        documento = event.get('documento', '')
        metodo = event.get('metodo', '')
        valor = event.get('valor', '')
        zona = event.get('zona', '')
    
    # Log de parámetros extraídos
    # "Borrar cuando se pase a producción :D"
    logger.info(" Parámetros extraídos del evento:")
    logger.info(f"  - documento (PK): {documento[:5] if documento else '[VACÍO]'}*** (longitud: {len(documento)})")
    logger.info(f"  - sessionId (metadata): {session_id[:15] if session_id else '[VACÍO]'}***")
    logger.info(f"  - metodo: {metodo if metodo else '[VACÍO]'}")
    logger.info(f"  - valor: {valor if valor else '[VACÍO]'}")
    logger.info(f"  - zona: {zona if zona else '[N/A - no requerido para CHIP/DIRECCION]'}")
    
    # Validación de inputs
    if not documento:
        logger.error(" Documento vacío")
        return build_response(event, {
            "success": False,
            "message": "Documento es requerido para recuperar el token de autenticación"
        }, 200)
    
    if not metodo:
        logger.error(" Método de búsqueda vacío")
        return build_response(event, {
            "success": False,
            "message": "Método de búsqueda es requerido (CHIP, DIRECCION, MATRICULA)"
        }, 200)
    
    if not valor:
        logger.error(" Valor de búsqueda vacío")
        return build_response(event, {
            "success": False,
            "message": "Valor de búsqueda es requerido"
        }, 200)
    
    # Normalizar método a mayúsculas
    metodo = metodo.upper().strip()
    
    # Validar método
    metodos_validos = ["CHIP", "DIRECCION", "MATRICULA"]
    if metodo not in metodos_validos:
        logger.error(f" Método inválido: {metodo}")
        return build_response(event, {
            "success": False,
            "message": f"Método inválido. Debe ser uno de: {', '.join(metodos_validos)}"
        }, 200)
    
    # Validar zona si método es MATRICULA
    if metodo == "MATRICULA":
        if not zona:
            logger.error(" Zona requerida para búsqueda por MATRICULA")
            return build_response(event, {
                "success": False,
                "message": "Zona es requerida para búsqueda por matrícula (Norte, Centro, Sur)"
            }, 200)
        
        # Normalizar zona
        zona = zona.upper().strip()
        
        if zona not in ZONA_TO_CIRCULO:
            logger.error(f" Zona inválida: {zona}")
            return build_response(event, {
                "success": False,
                "message": f"Zona inválida. Debe ser: Norte, Centro o Sur"
            }, 200)
    
    logger.info(f" Buscando predio por {metodo}: {valor[:20]}...")
    
    try:
        
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

        # 1. Obtener token JWT de DynamoDB
        logger.info(" PASO 1: Recuperando token JWT de DynamoDB...")
        token_dict = get_token_from_dynamodb(documento)
        token = token_dict.get('token', '') if token_dict else ''
        
        if not token:
            logger.error(" Token no encontrado en DynamoDB")
            logger.error("  - Posibles causas:")
            logger.error("    1. Token expiró (TTL de 10 minutos)")
            logger.error("    2. Documento incorrecto")
            logger.error("    3. Usuario no completó validación OTP")
            return build_response(event, {
                "success": False,
                "message": "Token de autenticación no encontrado o expirado. Por favor reinicia el proceso."
            }, 200)
        
        # 2. Buscar predio en API según método
        logger.info(f" PASO 2: Buscando predio por {metodo}...")
        
        if metodo == "CHIP":
            api_response = buscar_por_chip(token, valor)
        elif metodo == "DIRECCION":
            api_response = buscar_por_direccion(token, valor)
        elif metodo == "MATRICULA":
            api_response = buscar_por_matricula(token, valor, zona)
        
        # 3. Procesar respuesta
        logger.info(f" PASO 3: Procesando respuesta de la API...")
        
        if api_response.get('success'):
            logger.info(" Predio encontrado exitosamente")
            
            predio_data = api_response.get('data', {})
            
            # Log de información del predio
            logger.info(" Información del predio encontrado:")
            for key in list(predio_data.keys())[:5]:  # Primeras 5 claves
                logger.info(f"  • {key}: {str(predio_data[key])[:50]}...")
            
            # Extraer CHIP del predio encontrado
            chip_encontrado = predio_data.get('chip', valor if metodo == 'CHIP' else '')
            
            if not chip_encontrado:
                logger.warning(" No se pudo extraer CHIP del predio")
                logger.warning(f"  - Keys disponibles: {list(predio_data.keys())}")
            
            logger.info(f" PASO 4: Guardando CHIP en DynamoDB...")
            logger.info(f"  - CHIP a guardar: {chip_encontrado}")
            
            # Guardar CHIP en DynamoDB (máximo 3)
            resultado_chips = actualizar_chips_seleccionados_dynamodb(documento, chip_encontrado)
            
            # Construir respuesta completa
            response = {
                "success": True,
                "message": resultado_chips.get('message', 'Predio encontrado exitosamente'),
                "predio": predio_data,
                "chipAgregado": chip_encontrado,
                "totalSeleccionados": resultado_chips.get('total', 0),
                "chipsSeleccionados": resultado_chips.get('chips', []),
                "limiteAlcanzado": resultado_chips.get('limiteAlcanzado', False)
            }
            
            logger.info(f" Respuesta completa construida:")
            logger.info(f"  - CHIP agregado: {chip_encontrado}")
            logger.info(f"  - Total seleccionados: {resultado_chips.get('total', 0)}/3")
            logger.info(f"  - Límite alcanzado: {resultado_chips.get('limiteAlcanzado', False)}")
            
            return build_response(event, response, 200)
        else:
            # No se encontró el predio
            error_code = api_response.get('errorCode', 'PROPERTY_NOT_FOUND')
            mensaje = api_response.get('message', 'No se encontró el predio con los criterios especificados')
            
            logger.warning(f" Predio no encontrado")
            logger.warning(f"  - Método: {metodo}")
            logger.warning(f"  - Valor: {valor}")
            logger.warning(f"  - Error: {error_code}")
            
            response = {
                "success": False,
                "message": mensaje
            }
            
            return build_response(event, response, 200)  # 200 porque es un resultado válido (no encontrado)
        
    except requests.exceptions.Timeout:
        logger.error(" TIMEOUT: API no respondió a tiempo")
        return build_response(event, {
            "success": False,
            "message": "Error técnico: timeout al buscar el predio. Por favor intenta nuevamente."
        }, 200)
        
    except requests.exceptions.RequestException as e:
        logger.error(f" ERROR DE RED")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        return build_response(event, {
            "success": False,
            "message": "Error técnico al buscar el predio. Verifica tu conexión."
        }, 200)
        
    except Exception as e:
        logger.error(f" ERROR INESPERADO")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return build_response(event, {
            "success": False,
            "message": "Error interno al procesar la búsqueda."
        }, 200)


def actualizar_chips_seleccionados_dynamodb(documento, nuevo_chip):
    """
    Agrega un CHIP a la lista de CHIPs seleccionados en DynamoDB.
    Máximo 3 CHIPs permitidos.
    
    Args:
        documento: Número de documento del ciudadano (PK en DynamoDB)
        nuevo_chip: CHIP del predio a agregar
    
    Returns:
        dict con {success, message, chips, total, limiteAlcanzado}
    """
    if not documento or not nuevo_chip:
        logger.warning(" Documento o CHIP vacío")
        return {
            "success": False,
            "message": "Documento y CHIP son requeridos",
            "chips": [],
            "total": 0,
            "limiteAlcanzado": False
        }
    
    logger.info(f" Actualizando CHIPs seleccionados en DynamoDB...")
    logger.info(f"  - Tabla: {TABLE_NAME}")
    logger.info(f"  - Documento (PK): {documento[:3]}***")
    logger.info(f"  - Nuevo CHIP: {nuevo_chip}")
    
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        # Obtener item actual
        response = table.get_item(Key={'documento': documento})
        
        if 'Item' not in response:
            logger.error(" Token no encontrado en DynamoDB")
            logger.error(f"  - Documento: {documento[:3]}***")
            return {
                "success": False,
                "message": "Sesión no encontrada. Por favor valida tu identidad nuevamente.",
                "chips": [],
                "total": 0,
                "limiteAlcanzado": False
            }
        
        item = response['Item']
        chips_actuales = item.get('chipsSeleccionados', [])
        
        logger.info(f" CHIPs actuales en DynamoDB: {len(chips_actuales)}")
        logger.info(f"  - CHIPs: {chips_actuales}")
        
        # Validar duplicado
        if nuevo_chip in chips_actuales:
            logger.info(f" CHIP {nuevo_chip} ya estaba seleccionado")
            return {
                "success": True,
                "message": f"El predio ya estaba en tu selección ({len(chips_actuales)}/3)",
                "chips": chips_actuales,
                "total": len(chips_actuales),
                "limiteAlcanzado": len(chips_actuales) >= 3
            }
        
        # Validar límite de 3
        if len(chips_actuales) >= 3:
            logger.warning(f" Límite de 3 CHIPs alcanzado")
            logger.warning(f"  - CHIPs actuales: {chips_actuales}")
            return {
                "success": False,
                "message": "Has alcanzado el límite máximo de 3 predios.",
                "chips": chips_actuales,
                "total": len(chips_actuales),
                "limiteAlcanzado": True
            }
        
        # Agregar nuevo CHIP
        chips_actuales.append(nuevo_chip)
        
        logger.info(f" Agregando CHIP a la lista...")
        logger.info(f"  - Total después de agregar: {len(chips_actuales)}/3")
        
        # Actualizar DynamoDB
        table.update_item(
            Key={'documento': documento},
            UpdateExpression='SET chipsSeleccionados = :chips',
            ExpressionAttributeValues={':chips': chips_actuales}
        )
        
        logger.info(f" CHIP {nuevo_chip} agregado exitosamente a DynamoDB")
        logger.info(f"  - Total de CHIPs seleccionados: {len(chips_actuales)}/3")
        logger.info(f"  - Lista completa: {chips_actuales}")
        
        return {
            "success": True,
            "message": f"Predio agregado exitosamente. Tienes {len(chips_actuales)} predio(s) seleccionado(s)",
            "chips": chips_actuales,
            "total": len(chips_actuales),
            "limiteAlcanzado": len(chips_actuales) >= 3
        }
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f" Error de DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        logger.error(f"  - Documento: {documento[:3]}***")
        return {
            "success": False,
            "message": "Error técnico al guardar la selección",
            "chips": [],
            "total": 0,
            "limiteAlcanzado": False
        }
    except Exception as e:
        logger.error(f" Error inesperado actualizando CHIPs")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return {
            "success": False,
            "message": "Error interno al guardar la selección",
            "chips": [],
            "total": 0,
            "limiteAlcanzado": False
        }


def buscar_por_chip(token, chip):
    """
    Busca un predio por su código CHIP.
    Implementa exponential backoff para manejar intermitencias de red.
    
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
    
    logger.info(f"=== Llamando API de búsqueda por CHIP (con exponential backoff) ===")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - CHIP: {chip_limpio}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
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
                logger.error(f"  - Respuesta: {resp.text[:200]}")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        "success": False,
                        "message": "Respuesta inválida del servidor después de múltiples intentos",
                        "errorCode": "INVALID_JSON"
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"⏳ Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Si llegamos aquí, la petición fue exitosa
            logger.info(f" Llamada al API completada exitosamente en intento {attempt + 1}")
            
            # Procesar respuesta según status code
            if resp.status_code == 200:
                logger.info(" Status 200 - Predio encontrado")
                return {
                    "success": response_data.get('success', True),
                    "message": response_data.get('message', 'Predio encontrado'),
                    "data": response_data.get('data', {}),
                    "errorCode": response_data.get('errorCode', '')
                }
            else:
                logger.error(f" Status {resp.status_code} - Error inesperado")
                return {
                    "success": False,
                    "message": response_data.get('message', 'Error al buscar el predio'),
                    "errorCode": response_data.get('errorCode', 'API_ERROR')
                }
        
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f" Timeout en intento {attempt + 1}/{MAX_RETRIES} (15 segundos)")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "Tiempo de espera agotado al buscar el predio",
                    "errorCode": "TIMEOUT"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f" Error de conexión en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Error de conexión después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "No se pudo conectar con el servidor",
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
                    "message": "Error en la solicitud HTTP al buscar el predio",
                    "errorCode": "HTTP_ERROR"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except Exception as e:
            logger.exception(f" Error inesperado en búsqueda por CHIP: {str(e)}")
            return {
                "success": False,
                "message": "Error inesperado al buscar el predio",
                "errorCode": "UNEXPECTED_ERROR"
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f" Falló después de {MAX_RETRIES} intentos")
    return {
        "success": False,
        "message": f"Error después de {MAX_RETRIES} intentos: {str(last_exception)}",
        "errorCode": "MAX_RETRIES_EXCEEDED"
    }


def buscar_por_direccion(token, direccion):
    """
    Busca un predio por su dirección.
    Implementa exponential backoff para manejar intermitencias de red.
    
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
    
    logger.info(f"=== Llamando API de búsqueda por DIRECCIÓN (con exponential backoff) ===")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - Dirección original: {direccion}")
    logger.info(f"  - Dirección encoded: {direccion_encoded}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
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
                logger.error(f"  - Respuesta: {resp.text[:300]}")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        "success": False,
                        "message": "Respuesta inválida del servidor después de múltiples intentos",
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
                logger.info(" Status 200 - Predio encontrado")
                return {
                    "success": response_data.get('success', True),
                    "message": response_data.get('message', 'Predio encontrado'),
                    "data": response_data.get('data', {}),
                    "errorCode": response_data.get('errorCode', '')
                }
            else:
                logger.error(f" Status {resp.status_code} - Error inesperado")
                return {
                    "success": False,
                    "message": response_data.get('message', 'Error al buscar el predio'),
                    "errorCode": response_data.get('errorCode', 'API_ERROR')
                }
        
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f" Timeout en intento {attempt + 1}/{MAX_RETRIES} (15 segundos)")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "Tiempo de espera agotado al buscar el predio",
                    "errorCode": "TIMEOUT"
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
                    "message": "No se pudo conectar con el servidor",
                    "errorCode": "CONNECTION_ERROR"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except requests.exceptions.RequestException as e:
            last_exception = e
            logger.error(f" Error en la solicitud HTTP en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Error en solicitud HTTP después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "Error en la solicitud HTTP al buscar el predio",
                    "errorCode": "HTTP_ERROR"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except Exception as e:
            logger.exception(f" Error inesperado en búsqueda por DIRECCIÓN: {str(e)}")
            return {
                "success": False,
                "message": "Error inesperado al buscar el predio",
                "errorCode": "UNEXPECTED_ERROR"
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f" Falló después de {MAX_RETRIES} intentos")
    return {
        "success": False,
        "message": f"Error después de {MAX_RETRIES} intentos: {str(last_exception)}",
        "errorCode": "MAX_RETRIES_EXCEEDED"
    }


def buscar_por_matricula(token, matricula, zona):
    """
    Busca un predio por su matrícula y zona (círculo registral).
    Implementa exponential backoff para manejar intermitencias de red.
    
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
    
    logger.info(f"=== Llamando API de búsqueda por MATRÍCULA (con exponential backoff) ===")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - Matrícula original: {matricula}")
    logger.info(f"  - Matrícula limpia: {matricula_limpia}")
    logger.info(f"  - Zona: {zona}")
    logger.info(f"  - ID Círculo: {id_circulo}")
    logger.info(f"  - Código círculo esperado: {ZONA_TO_CIRCULO.get(zona, 'N/A')}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
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
                logger.error(f"  - Respuesta: {resp.text[:300]}")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        "success": False,
                        "message": "Respuesta inválida del servidor después de múltiples intentos",
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
                logger.info(" Status 200 - Predio encontrado")
                return {
                    "success": response_data.get('success', True),
                    "message": response_data.get('message', 'Predio encontrado'),
                    "data": response_data.get('data', {}),
                    "errorCode": response_data.get('errorCode', '')
                }
            else:
                logger.error(f" Status {resp.status_code} - Error inesperado")
                return {
                    "success": False,
                    "message": response_data.get('message', 'Error al buscar el predio'),
                    "errorCode": response_data.get('errorCode', 'API_ERROR')
                }
        
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f" Timeout en intento {attempt + 1}/{MAX_RETRIES} (15 segundos)")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "Tiempo de espera agotado al buscar el predio",
                    "errorCode": "TIMEOUT"
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
                    "message": "No se pudo conectar con el servidor",
                    "errorCode": "CONNECTION_ERROR"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except requests.exceptions.RequestException as e:
            last_exception = e
            logger.error(f" Error en la solicitud HTTP en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Error en solicitud HTTP después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "Error en la solicitud HTTP al buscar el predio",
                    "errorCode": "HTTP_ERROR"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except Exception as e:
            logger.exception(f" Error inesperado en búsqueda por MATRÍCULA: {str(e)}")
            return {
                "success": False,
                "message": "Error inesperado al buscar el predio",
                "errorCode": "UNEXPECTED_ERROR"
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f" Falló después de {MAX_RETRIES} intentos")
    return {
        "success": False,
        "message": f"Error después de {MAX_RETRIES} intentos: {str(last_exception)}",
        "errorCode": "MAX_RETRIES_EXCEEDED"
    }

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
    
    logger.info(" Respuesta formateada correctamente")
    return formatted_response
