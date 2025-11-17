"""
Lambda Function: Generar Certificados
Genera certificados de tradición y libertad para los predios seleccionados (máximo 3).
Los certificados son enviados automáticamente al correo electrónico del usuario.
PASO 8 (final) del flujo de Bedrock Agent.
"""
import json
import logging
import requests
import boto3
import uuid
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clientes AWS
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_TOKENS = 'cat-test-certification-session-tokens'
TABLE_AUDITORIA = 'cat-test-certification-data'

# Base URL de la API
API_BASE_URL = "http://vmprocondock.catastrobogota.gov.co:3400/catia-auth"

# Límite de certificados por solicitud
MAX_CERTIFICADOS = 3


def handler(event, context):
    """
    Genera certificados de tradición y libertad para los CHIPs seleccionados.
    
    Input esperado (OPCIÓN 1 - Con CHIPs explícitos, para flujo ListarPredios):
    {
        "documento": "1234567890",  // REQUERIDO - Para recuperar token JWT
        "tipoDocumento": "CC",  // REQUERIDO - Para auditoría
        "nombreCompleto": "Juan Pérez García",  // REQUERIDO - Para auditoría
        "chips": ["AAA1234", "BBB5678", "CCC9012"],  // OPCIONAL - Si se proporciona, se usa esto
        "sessionId": "xxx"  // Opcional - Metadata
    }
    
    Input esperado (OPCIÓN 2 - Sin CHIPs, para flujo BuscarPredios):
    {
        "documento": "1234567890",  // REQUERIDO - Para recuperar token JWT y CHIPs de DynamoDB
        "tipoDocumento": "CC",  // REQUERIDO - Para auditoría
        "nombreCompleto": "Juan Pérez García",  // REQUERIDO - Para auditoría
        "sessionId": "xxx"  // Opcional - Metadata
    }
    // En este caso, los CHIPs se leen automáticamente de DynamoDB (campo chipsSeleccionados)
    
    Output:
    {
        "success": true/false,
        "message": "Certificados generados y enviados al correo exitosamente",
        "certificados": [
            {
                "chip": "AAA1234",
                "success": true,
                "requestNumber": "1257322",
                "message": "Certificado generado exitosamente"
            },
            ...
        ],
        "totalExitosos": 3,
        "totalFallidos": 0
    }
    """
    logger.info("=== Lambda: Generar Certificados ===")
    logger.info(f"📋 Event recibido: {json.dumps(event, ensure_ascii=False)}")
    
    # Extraer parámetros - Bedrock Agent envía en requestBody
    if 'requestBody' in event and 'content' in event['requestBody']:
        content = event['requestBody']['content']
        if 'application/json' in content:
            properties = content['application/json']['properties']
            body = {prop['name']: prop['value'] for prop in properties}
            
            # Extraer parámetros requeridos
            documento = body.get('documento', '')
            tipo_documento = body.get('tipoDocumento', '')
            nombre_completo = body.get('nombreCompleto', '')
            
            # CHIPs puede venir como string separado por comas o como lista
            chips_raw = body.get('chips', '')
            if isinstance(chips_raw, str):
                # Si viene como string "AAA1234,BBB5678,CCC9012"
                chips = [chip.strip() for chip in chips_raw.split(',') if chip.strip()]
            elif isinstance(chips_raw, list):
                chips = chips_raw
            else:
                chips = []
            
            session_id = body.get('sessionId', event.get('sessionId', ''))
        else:
            documento = ''
            tipo_documento = ''
            nombre_completo = ''
            chips = []
            session_id = event.get('sessionId', '')
    else:
        # Formato directo para testing
        documento = event.get('documento', '')
        tipo_documento = event.get('tipoDocumento', '')
        nombre_completo = event.get('nombreCompleto', '')
        chips = event.get('chips', [])
        session_id = event.get('sessionId', '')
    
    # Log de parámetros extraídos
    logger.info(" Parámetros extraídos del evento:")
    logger.info(f"  - documento (PK): {documento[:5] if documento else '[VACÍO]'}*** (longitud: {len(documento)})")
    logger.info(f"  - tipoDocumento: {tipo_documento if tipo_documento else '[VACÍO]'}")
    logger.info(f"  - nombreCompleto: {nombre_completo[:20] if nombre_completo else '[VACÍO]'}...")
    logger.info(f"  - chips: {chips}")
    logger.info(f"  - cantidad de CHIPs: {len(chips)}")
    logger.info(f"  - sessionId (metadata): {session_id[:15] if session_id else '[VACÍO]'}***")
    
    # Validación de inputs
    if not documento:
        logger.error("❌ Documento vacío")
        return build_response(event, {
            "success": False,
            "message": "Documento es requerido para recuperar el token de autenticación"
        }, 400)
    
    if not tipo_documento:
        logger.error("❌ Tipo de documento vacío")
        return build_response(event, {
            "success": False,
            "message": "Tipo de documento es requerido para la auditoría"
        }, 400)
    
    if not nombre_completo:
        logger.error("❌ Nombre completo vacío")
        return build_response(event, {
            "success": False,
            "message": "Nombre completo es requerido para la auditoría"
        }, 400)
    
    # Si no se proporcionaron CHIPs como parámetro, leerlos de DynamoDB
    if not chips or len(chips) == 0:
        logger.info(" No se proporcionaron CHIPs en el parámetro, leyendo de DynamoDB...")
        chips = obtener_chips_seleccionados_desde_dynamo(documento)
        
        if not chips or len(chips) == 0:
            logger.error("❌ No se encontraron CHIPs seleccionados en DynamoDB")
            return build_response(event, {
                "success": False,
                "message": "No has seleccionado ningún predio. Por favor busca y selecciona al menos un predio antes de generar certificados."
            }, 400)
        
        logger.info(f"✅ CHIPs recuperados de DynamoDB: {chips}")
        logger.info(f"  - Total de CHIPs: {len(chips)}")
    else:
        logger.info(f" CHIPs proporcionados como parámetro: {chips}")
    
    # Validar límite de certificados
    if len(chips) > MAX_CERTIFICADOS:
        logger.warning(f"⚠️ Se solicitaron {len(chips)} certificados, pero el límite es {MAX_CERTIFICADOS}")
        logger.warning(f"  - Se procesarán solo los primeros {MAX_CERTIFICADOS} CHIPs")
        chips = chips[:MAX_CERTIFICADOS]
    
    logger.info(f" Generando certificados para {len(chips)} predio(s)...")
    logger.info(f"  - CHIPs a procesar: {chips}")
    
    try:
        # 1. Obtener token JWT de DynamoDB
        logger.info(" PASO 1: Recuperando token JWT de DynamoDB...")
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
        
        # 2. Generar certificados para cada CHIP
        logger.info(f" PASO 2: Generando certificados para {len(chips)} CHIP(s)...")
        
        resultados = []
        exitosos = 0
        fallidos = 0
        
        for idx, chip in enumerate(chips, 1):
            logger.info(f"\n--- Procesando CHIP {idx}/{len(chips)}: {chip} ---")
            
            resultado = generar_certificado(token, chip)
            
            if resultado.get('success'):
                exitosos += 1
                logger.info(f"✅ Certificado generado exitosamente para CHIP: {chip}")
                logger.info(f"  - Request Number: {resultado.get('requestNumber', 'N/A')}")
                
                # 3. Guardar auditoría en DynamoDB
                request_number = resultado.get('requestNumber', '')
                if request_number:
                    logger.info(f" PASO 3.{idx}: Guardando auditoría para CHIP {chip}...")
                    guardar_auditoria(
                        documento=documento,
                        tipo_documento=tipo_documento,
                        nombre_completo=nombre_completo,
                        chip=chip,
                        request_number=request_number
                    )
                else:
                    logger.warning(f" No se encontró requestNumber para CHIP {chip}, auditoría omitida")
            else:
                fallidos += 1
                logger.error(f" Error generando certificado para CHIP: {chip}")
                logger.error(f"  - Error: {resultado.get('mensaje', 'Error desconocido')}")
            
            resultados.append({
                "chip": chip,
                "success": resultado.get('success'),
                "requestNumber": resultado.get('requestNumber', ''),
                "message": resultado.get('mensaje', '')
            })
        
        # 4. Construir respuesta final
        logger.info(f"\n✅ PASO 4: Proceso completado")
        logger.info(f"  - Total CHIPs procesados: {len(chips)}")
        logger.info(f"  - Exitosos: {exitosos}")
        logger.info(f"  - Fallidos: {fallidos}")
        
        # Determinar si fue exitoso (al menos 1 certificado generado)
        success = exitosos > 0
        
        if success:
            if fallidos == 0:
                mensaje = f"Los {exitosos} certificado(s) fueron generados y enviados al correo electrónico exitosamente"
            else:
                mensaje = f"{exitosos} certificado(s) generados exitosamente, {fallidos} fallaron. Revisa los detalles."
        else:
            mensaje = "No se pudo generar ningún certificado. Por favor verifica los CHIPs e intenta nuevamente."
        
        response = {
            "success": success,
            "message": mensaje,
            "certificados": resultados,
            "totalExitosos": exitosos,
            "totalFallidos": fallidos
        }
        
        return build_response(event, response, 200 if success else 207)  # 207 = Multi-Status
        
    except requests.exceptions.Timeout:
        logger.error("❌ TIMEOUT: API no respondió a tiempo")
        return build_response(event, {
            "success": False,
            "message": "Error técnico: timeout al generar los certificados. Por favor intenta nuevamente."
        }, 502)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ ERROR DE RED")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        return build_response(event, {
            "success": False,
            "message": "Error técnico al generar los certificados. Verifica tu conexión."
        }, 502)
        
    except Exception as e:
        logger.error(f"❌ ERROR INESPERADO")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return build_response(event, {
            "success": False,
            "message": "Error interno al procesar la generación de certificados."
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
    logger.info(f"  - Tabla: {TABLE_TOKENS}")
    logger.info(f"  - Documento (PK): {documento[:3]}*** (longitud: {len(documento)})")
    
    try:
        table = dynamodb.Table(TABLE_TOKENS)
        
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


def obtener_chips_seleccionados_desde_dynamo(documento):
    """
    Obtiene la lista de CHIPs seleccionados desde DynamoDB.
    Lee el campo 'chipsSeleccionados' del item de sesión.
    
    Args:
        documento: Número de documento del ciudadano (PK en DynamoDB)
    
    Returns:
        list: Lista de CHIPs seleccionados, o lista vacía si no hay
    """
    if not documento:
        logger.warning("⚠️ Documento vacío")
        return []
    
    logger.info("💾 Recuperando CHIPs seleccionados de DynamoDB...")
    logger.info(f"  - Tabla: {TABLE_TOKENS}")
    logger.info(f"  - Documento (PK): {documento[:3]}*** (longitud: {len(documento)})")
    
    try:
        table = dynamodb.Table(TABLE_TOKENS)
        
        response = table.get_item(Key={'documento': documento})
        
        if 'Item' not in response:
            logger.warning(f"⚠️ No se encontró registro en DynamoDB")
            logger.warning(f"  - Documento: {documento[:3]}***")
            logger.warning(f"  - Posibles causas:")
            logger.warning(f"    1. Sesión expirada (TTL de 10 minutos)")
            logger.warning(f"    2. Usuario no completó validación OTP")
            logger.warning(f"    3. Usuario no buscó ningún predio")
            return []
        
        item = response['Item']
        chips_seleccionados = item.get('chipsSeleccionados', [])
        
        # Asegurar que sea una lista
        if not isinstance(chips_seleccionados, list):
            logger.warning(f"⚠️ chipsSeleccionados no es una lista, es: {type(chips_seleccionados)}")
            chips_seleccionados = []
        
        logger.info(f"✅ CHIPs seleccionados recuperados exitosamente")
        logger.info(f"  - Total de CHIPs: {len(chips_seleccionados)}")
        logger.info(f"  - CHIPs: {chips_seleccionados}")
        logger.info(f"  - Documento: {documento[:3]}***")
        
        return chips_seleccionados
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"❌ Error de DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        logger.error(f"  - Documento: {documento[:3]}***")
        return []
    except Exception as e:
        logger.error(f"❌ Error inesperado obteniendo CHIPs seleccionados")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return []


def generar_certificado(token, chip):
    """
    Genera un certificado de tradición y libertad para un CHIP específico.
    El certificado es enviado automáticamente al correo del usuario.
    
    Endpoint: GET /reports/certification/property/{chip}
    Ejemplo: http://vmprocondock.catastrobogota.gov.co:3400/catia-auth/reports/certification/property/AAA1234
    
    Args:
        token: JWT token de autenticación
        chip: Código CHIP del predio (ej: "AAA1234")
    
    Returns:
        dict con {success, mensaje, requestNumber (opcional)}
    """
    # Limpiar CHIP (remover guiones si los tiene)
    chip_limpio = chip.replace("-", "").strip()
    
    URL = f"{API_BASE_URL}/reports/certification/property/{chip_limpio}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    logger.info(f"📞 Llamando API de generación de certificado:")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - CHIP: {chip_limpio}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
    logger.info(f"  - Timeout: 30 segundos")
    
    try:
        resp = requests.get(URL, headers=headers, timeout=30)  # Mayor timeout para generación
        
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
                "requestNumber": ""
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
            logger.info(f"  - Success: {response_data.get('success', 'N/A')}")
            logger.info(f"  - Message: {response_data.get('message', 'N/A')}")
        except ValueError as ve:
            logger.error(f"❌ Respuesta no es JSON válido")
            logger.error(f"  - Error: {str(ve)}")
            logger.error(f"  - Respuesta (primeros 300 chars): {resp.text[:300]}")
            return {
                "success": False,
                "message": "Respuesta inválida del servidor",
                "requestNumber": ""
            }
        
        # Procesar respuesta según status code
        if resp.status_code == 200:
            logger.info("✅ Status 200 - Certificado generado exitosamente")
            
            # Extraer requestNumber de la data
            data = response_data.get('data', {})
            request_number = data.get('requestNumber', '')
            
            logger.info(f"  - Request Number: {request_number}")
            logger.info(f"  - Data keys: {list(data.keys()) if isinstance(data, dict) else 'No dict'}")
            
            return {
                "success": True,
                "message": response_data.get('message', 'Certificado generado y enviado al correo exitosamente'),
                "requestNumber": request_number
            }
        
        elif resp.status_code == 404:
            logger.warning("⚠️ Status 404 - CHIP no encontrado")
            return {
                "success": False,
                "message": response_data.get('message', 'No se encontró predio con el CHIP especificado'),
                "requestNumber": ""
            }
        
        elif resp.status_code == 401:
            logger.error("❌ Status 401 - Token inválido")
            return {
                "success": False,
                "message": "Token de autenticación inválido o expirado",
                "requestNumber": ""
            }
        
        elif resp.status_code == 400:
            logger.error("❌ Status 400 - Solicitud inválida")
            return {
                "success": False,
                "message": response_data.get('message', 'Error en la solicitud del certificado'),
                "requestNumber": ""
            }
        
        else:
            logger.error(f"❌ Status {resp.status_code} - Error inesperado")
            return {
                "success": False,
                "message": response_data.get('message', 'Error al generar el certificado'),
                "requestNumber": ""
            }
        
    except requests.exceptions.Timeout:
        logger.error("❌ Timeout en generación de certificado")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error de red en generación de certificado: {str(e)}")
        raise


def guardar_auditoria(documento, tipo_documento, nombre_completo, chip, request_number):
    """
    Guarda la auditoría de la generación del certificado en DynamoDB.
    
    Tabla: cat-test-certification-data
    
    Campos:
    - id (PK): UUID único
    - nombreCompleto: Nombre completo del ciudadano
    - tipoDocumento: Tipo de documento (CC, CE, etc.)
    - numeroIdentificacion: Número de documento
    - fechaHora: Timestamp de la solicitud (ISO 8601)
    - numeroRadicado: Número de radicado de la certificación
    - chip: CHIP del predio (adicional para referencia)
    
    Args:
        documento: Número de documento
        tipo_documento: Tipo de documento
        nombre_completo: Nombre completo del ciudadano
        chip: CHIP del predio
        request_number: Número de radicado de la certificación
    """
    logger.info(f"💾 Guardando auditoría en DynamoDB...")
    logger.info(f"  - Tabla: {TABLE_AUDITORIA}")
    
    try:
        table = dynamodb.Table(TABLE_AUDITORIA)
        
        # Generar ID único
        audit_id = str(uuid.uuid4())
        
        # Timestamp actual en formato ISO 8601
        fecha_hora = datetime.utcnow().isoformat() + 'Z'
        
        # Item de auditoría
        item = {
            'id': audit_id,  # PK
            'nombreCompleto': nombre_completo,
            'tipoDocumento': tipo_documento,
            'numeroIdentificacion': documento,
            'fechaHora': fecha_hora,
            'numeroRadicado': request_number,
            'chip': chip  # Campo adicional para referencia
        }
        
        logger.info(f"  - ID: {audit_id}")
        logger.info(f"  - Nombre: {nombre_completo[:30]}...")
        logger.info(f"  - Documento: {tipo_documento} {documento[:3]}***")
        logger.info(f"  - CHIP: {chip}")
        logger.info(f"  - Request Number: {request_number}")
        logger.info(f"  - Fecha/Hora: {fecha_hora}")
        
        # Guardar en DynamoDB
        table.put_item(Item=item)
        
        logger.info(f"✅ Auditoría guardada exitosamente")
        logger.info(f"  - ID de auditoría: {audit_id}")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"❌ Error al guardar auditoría en DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        return False
    except Exception as e:
        logger.error(f"❌ Error inesperado guardando auditoría")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return False


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
    logger.info(f"  - Action Group: {event.get('actionGroup', 'GenerarCertificados')}")
    logger.info(f"  - Response Body (preview): {json.dumps(response_data, ensure_ascii=False)[:200]}...")
    
    formatted_response = {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get('actionGroup', 'GenerarCertificados'),
            "apiPath": event.get('apiPath', '/generar-certificados'),
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
