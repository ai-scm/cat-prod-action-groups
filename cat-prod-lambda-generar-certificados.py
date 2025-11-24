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
import time
import random
import os
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

# Configuración de reintentos con exponential backoff
MAX_RETRIES = 10
INITIAL_BACKOFF = 1  # segundos
MAX_BACKOFF = 60  # segundos

# Límite de certificados por solicitud
MAX_CERTIFICADOS = 3

# ============================================================
# CONFIGURACIÓN DE MODO MOCK
# ============================================================
ENABLE_MOCK = os.environ.get('ENABLE_MOCK', 'false').lower() == 'true'

# Usuarios mock para testing (mismos que contar-predios y listar-predios)
MOCK_USERS = {
    "123456789": {
        "nombre": "Juan Carlos",
        "apellido": "Rodríguez",
        "email": "juan.rodriguez@catastro.test",
        "prediosCount": 3
    },
    "987654321": {
        "nombre": "María Elena",
        "apellido": "González",
        "email": "maria.gonzalez@catastro.test",
        "prediosCount": 15
    }
}

logger.info(f"[MOCK CONFIG] ENABLE_MOCK = {ENABLE_MOCK}")
if ENABLE_MOCK:
    logger.info(f"[MOCK CONFIG] Usuarios mock configurados: {list(MOCK_USERS.keys())}")


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


def get_mock_session_data(documento):
    """
    Genera datos de sesión simulados para testing sin acceder a DynamoDB.
    
    Args:
        documento: Número de documento del usuario
    
    Returns:
        dict: Datos de sesión simulados con token, usuario, y CHIPs
    """
    logger.info("[MOCK] 🎭 Generando datos de sesión mock")
    logger.info(f"[MOCK] Documento: {documento[:3]}***")
    
    if documento in MOCK_USERS:
        user_data = MOCK_USERS[documento]
        
        # CHIPs simulados según el usuario
        if documento == "123456789":
            # Usuario con 3 predios
            chips_seleccionados = ["AAA0000001ABC", "AAA0000002DEF", "AAA0000003GHI"]
        else:
            # Usuario con 15 predios (solo seleccionó 3)
            chips_seleccionados = ["BBB0000001XYZ", "BBB0000002XYZ", "BBB0000003XYZ"]
        
        logger.info(f"[MOCK] Usuario: {user_data['nombre']} {user_data['apellido']}")
        logger.info(f"[MOCK] CHIPs seleccionados: {chips_seleccionados}")
        
        return {
            'token': 'MOCK_JWT_TOKEN_12345',
            'usuario': {
                'nombre': user_data['nombre'],
                'apellido': user_data['apellido'],
                'email': user_data['email'],
                'numeroDocumento': documento
            },
            'chipsSeleccionados': chips_seleccionados,
            'documento': documento,
            'mockMode': True
        }
    else:
        logger.info(f"[MOCK] Usuario genérico")
        return {
            'token': 'MOCK_JWT_TOKEN_GENERIC',
            'usuario': {
                'nombre': 'Usuario',
                'apellido': 'Mock Genérico',
                'email': 'mock@catastro.test',
                'numeroDocumento': documento
            },
            'chipsSeleccionados': ["XXX0000001DEF", "XXX0000002GHI"],
            'documento': documento,
            'mockMode': True
        }


def get_mock_chip_por_direccion(direccion):
    """
    Simula la conversión de dirección a CHIP sin llamar al API.
    
    Args:
        direccion: Dirección del predio
    
    Returns:
        dict: Resultado simulado con CHIP
    """
    logger.info(f"[MOCK] 🎭 Convirtiendo dirección a CHIP (simulado)")
    logger.info(f"[MOCK] Dirección: {direccion[:30]}...")
    
    # Simular delay
    time.sleep(random.uniform(0.2, 0.8))
    
    # Generar CHIP mock basado en hash de la dirección
    import hashlib
    hash_dir = hashlib.md5(direccion.encode()).hexdigest()[:8].upper()
    chip_mock = f"MOCK{hash_dir}"
    
    logger.info(f"[MOCK] ✅ CHIP generado: {chip_mock}")
    
    return {
        "success": True,
        "chip": chip_mock,
        "message": "CHIP encontrado exitosamente (MOCK)"
    }


def get_mock_certificado_response(chip):
    """
    Simula la generación de un certificado sin llamar al API externo.
    
    Args:
        chip: CHIP del predio
    
    Returns:
        dict: Respuesta simulada de generación de certificado
    """
    logger.info(f"[MOCK] 🎭 Generando certificado mock para CHIP: {chip}")
    
    # Simular delay realista (1-3 segundos por certificado)
    delay = random.uniform(1.0, 3.0)
    logger.info(f"[MOCK] Simulando generación de certificado ({delay:.2f}s)...")
    time.sleep(delay)
    
    # Generar número de radicado mock
    request_number = f"MOCK-{random.randint(1000000, 9999999)}"
    
    logger.info(f"[MOCK] ✅ Certificado generado exitosamente")
    logger.info(f"[MOCK] Request Number: {request_number}")
    
    return {
        "success": True,
        "message": "Certificado generado y enviado al correo exitosamente (MOCK)",
        "requestNumber": request_number
    }


def handler(event, context):
    """
    Genera certificados de tradición y libertad para los predios seleccionados.
    Soporta 2 flujos diferentes según la cantidad de predios del usuario.
    
    ==================================================================================
    FLUJO 1: ListarPredios (1-10 predios)
    ==================================================================================
    Input esperado:
    {
        "documento": "1234567890",  // REQUERIDO - Para recuperar token JWT
        "tipoDocumento": "CC",  // REQUERIDO - Para auditoría
        "direcciones": ["KR 7 6 16 SUR IN 3 AP 301", "KR 7 6 16 SUR GJ 169"],  // REQUERIDO
        "sessionId": "xxx"  // Opcional - Metadata
    }
    
    Proceso:
    1. Recibe direcciones de los predios seleccionados
    2. Por cada dirección, llama GET /properties/address/{address} para obtener CHIP
    3. Con los CHIPs obtenidos, genera los certificados
    
    NOTA: ListarPredios NO retorna CHIPs, solo direcciones. La conversión es transparente.
    
    ==================================================================================
    FLUJO 2: BuscarPredios (>10 predios)
    ==================================================================================
    Input esperado:
    {
        "documento": "1234567890",  // REQUERIDO - Para recuperar token JWT y CHIPs de DynamoDB
        "tipoDocumento": "CC",  // REQUERIDO - Para auditoría
        "sessionId": "xxx"  // Opcional - Metadata
    }
    
    Proceso:
    1. NO recibe direcciones ni CHIPs
    2. Lee CHIPs desde DynamoDB (campo chipsSeleccionados)
    3. Estos CHIPs fueron guardados previamente por BuscarPredios
    4. Genera los certificados con los CHIPs recuperados
    
    ==================================================================================
    
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
    logger.info(f" Event recibido: {json.dumps(event, ensure_ascii=False)}")
    
    # Extraer parámetros - Bedrock Agent envía en requestBody
    if 'requestBody' in event and 'content' in event['requestBody']:
        content = event['requestBody']['content']
        if 'application/json' in content:
            properties = content['application/json']['properties']
            body = {prop['name']: prop['value'] for prop in properties}
            
            # Extraer parámetros requeridos
            documento = body.get('documento', '')
            tipo_documento = body.get('tipoDocumento', '')
            
            # Direcciones (solo para flujo ListarPredios)
            # Puede venir en varios formatos desde el Agent:
            # - lista real (list)
            # - string JSON: '["a","b"]'
            # - string doble-encoded: '"[\\"a\\",\\"b\\"]"'
            # - comma-separated string: 'a, b'
            direcciones_raw = body.get('direcciones', '')
            logger.info(f" Raw 'direcciones' recibido (tipo: {type(direcciones_raw).__name__}): {str(direcciones_raw)[:200]}")

            direcciones = []

            if isinstance(direcciones_raw, list):
                direcciones = direcciones_raw

            elif isinstance(direcciones_raw, str):
                logger.info("'direcciones' viene como string. Intentando normalizar a lista...")
                temp = direcciones_raw.strip()

                # Intentar parsear JSON hasta 3 niveles (maneja doble-encoding)
                for attempt in range(3):
                    if temp.startswith('[') and temp.endswith(']'):
                        try:
                            parsed = json.loads(temp)
                            if isinstance(parsed, list):
                                direcciones = parsed
                                logger.info(f"Parse exitoso a lista en attempt {attempt+1}")
                                break
                            # Si parsed es string, puede ser doble-encoded - repetir
                            if isinstance(parsed, str):
                                temp = parsed
                                logger.info("Parse devolvió string - posible doble encoding, intentando de nuevo")
                                continue
                            # Si no es lista ni string, romper
                            break
                        except (json.JSONDecodeError, ValueError) as e:
                            logger.warning(f"json.loads falló en attempt {attempt+1}: {e}")
                            break
                    else:
                        # No parece JSON array, salir del loop
                        break

                # Si aún no logramos una lista, intentar detectar formato con corchetes sin comillas
                if not direcciones:
                    # Caso común del bot intermedio: "[KR 7 6 16 SUR GJ 169]" (sin comillas internas)
                    if temp.startswith('[') and temp.endswith(']'):
                        inner = temp[1:-1].strip()
                        # Remover comillas envolventes si existen
                        if (inner.startswith('"') and inner.endswith('"')) or (inner.startswith("'") and inner.endswith("'")):
                            inner = inner[1:-1].strip()

                        if ',' in inner:
                            direcciones = [p.strip().strip('"').strip("'") for p in inner.split(',') if p.strip()]
                        elif inner:
                            direcciones = [inner]
                    else:
                        # fallback: split por comas sobre el valor original
                        direcciones = [d.strip() for d in direcciones_raw.split(',') if d.strip()]

            else:
                direcciones = []
            
            # Limpiar strings vacíos del array y asegurar que todos sean strings
            cleaned = []
            for d in direcciones:
                if isinstance(d, str) and d and d.strip():
                    cleaned.append(d.strip())
                else:
                    # Attempt to coerce non-string values
                    try:
                        coerced = str(d).strip()
                        if coerced:
                            cleaned.append(coerced)
                    except Exception:
                        continue
            direcciones = cleaned
            
            session_id = body.get('sessionId', event.get('sessionId', ''))
        else:
            documento = ''
            tipo_documento = ''
            direcciones = []
            session_id = event.get('sessionId', '')
    else:
        # Formato directo para testing
        documento = event.get('documento', '')
        tipo_documento = event.get('tipoDocumento', '')
        direcciones = event.get('direcciones', [])
        session_id = event.get('sessionId', '')
    
    # Log de parámetros extraídos
    logger.info(" Parámetros extraídos del evento:")
    logger.info(f"  - documento (PK): {documento[:5] if documento else '[VACÍO]'}*** (longitud: {len(documento)})")
    logger.info(f"  - tipoDocumento: {tipo_documento if tipo_documento else '[VACÍO]'}")
    logger.info(f"  - direcciones: {direcciones if direcciones else '[VACÍO - flujo BuscarPredios]'}")
    logger.info(f"  - cantidad de direcciones: {len(direcciones)}")
    logger.info(f"  - sessionId (metadata): {session_id[:15] if session_id else '[VACÍO]'}***")
    
    # Determinar flujo
    if direcciones and len(direcciones) > 0:
        logger.info(f" Flujo detectado: LISTAR PREDIOS (1-10 predios)")
    else:
        logger.info(f" Flujo detectado: BUSCAR PREDIOS (>10 predios, CHIPs en DynamoDB)")
    
    # Validación de inputs
    if not documento:
        logger.error("❌ Documento vacío")
        return build_response(event, {
            "success": False,
            "message": "Documento es requerido para recuperar el token de autenticación"
        }, 200)
    
    if not tipo_documento:
        logger.error("❌ Tipo de documento vacío")
        return build_response(event, {
            "success": False,
            "message": "Tipo de documento es requerido para la auditoría"
        }, 200)
    
    # FLUJOS POSIBLES:
    # 1. FLUJO ListarPredios (1-10 predios): Vienen DIRECCIONES → convertir a CHIPs internamente
    # 2. FLUJO BuscarPredios (>10 predios): CHIPs ya guardados en DynamoDB → leer de allí
    # NOTA: NUNCA vienen CHIPs directamente como parámetro, solo direcciones o nada
    
    if direcciones and len(direcciones) > 0:
        # ============================================================
        # FLUJO 1: ListarPredios - Usuario tiene 1-10 predios
        # ============================================================
        logger.info(f"🏘️ FLUJO LISTAR PREDIOS")
        logger.info(f"📍 Direcciones proporcionadas ({len(direcciones)}), convirtiendo a CHIPs...")
        logger.info(f"  - Direcciones: {direcciones}")
        
        # ============================================================
        # DECISIÓN: ¿Usar MOCK o API real para conversión?
        # ============================================================
        if ENABLE_MOCK:
            # MODO MOCK: Usar conversión simulada
            logger.info("[MOCK] 🎭 MODO MOCK - Convirtiendo direcciones con función mock")
            
            chips_convertidos = []
            errores_conversion = []
            
            for idx, direccion in enumerate(direcciones, 1):
                logger.info(f"\n[MOCK] --- Convirtiendo dirección {idx}/{len(direcciones)} ---")
                logger.info(f"[MOCK]   - Dirección: {direccion}")
                
                resultado = get_mock_chip_por_direccion(direccion)
                
                if resultado.get('success'):
                    chip = resultado.get('chip', '')
                    if chip:
                        chips_convertidos.append(chip)
                        logger.info(f"[MOCK] ✅ CHIP obtenido: {chip}")
                else:
                    logger.error(f"[MOCK] ❌ Error convirtiendo dirección: {direccion}")
                    errores_conversion.append(f"Dirección '{direccion}': Error simulado")
            
        else:
            # MODO REAL: Obtener token y usar API externa
            logger.info("📡 MODO REAL - Obteniendo token para conversión")
            
            # Obtener token para hacer las conversiones
            logger.info(" PASO 1A: Recuperando token JWT de DynamoDB para conversión...")
            session_data = get_session_data_from_dynamodb(documento)
            
            if not session_data:
                logger.error("❌ No se encontró token para conversión de direcciones")
                return build_response(event, {
                    "success": False,
                    "message": "Token de autenticación no encontrado. Por favor reinicia el proceso."
                }, 200)
            
            token = session_data.get('token', '')
            
            # Convertir cada dirección a CHIP usando API REAL
            chips_convertidos = []
            errores_conversion = []
            
            for idx, direccion in enumerate(direcciones, 1):
                logger.info(f"\n--- Convirtiendo dirección {idx}/{len(direcciones)} ---")
                logger.info(f"  - Dirección: {direccion}")
                
                resultado = obtener_chip_por_direccion(token, direccion)
                
                if resultado.get('success'):
                    chip = resultado.get('chip', '')
                    if chip:
                        chips_convertidos.append(chip)
                        logger.info(f"✅ CHIP obtenido: {chip}")
                    else:
                        logger.error(f"❌ API no retornó CHIP para: {direccion}")
                        errores_conversion.append(f"Dirección '{direccion}': No se obtuvo CHIP")
                else:
                    logger.error(f"❌ Error convirtiendo dirección: {direccion}")
                    logger.error(f"  - Error: {resultado.get('message', 'Error desconocido')}")
                    errores_conversion.append(f"Dirección '{direccion}': {resultado.get('message', 'Error desconocido')}")
        
        logger.info(f"\n📊 Resultado de conversión:")
        logger.info(f"  - Total direcciones: {len(direcciones)}")
        logger.info(f"  - CHIPs obtenidos: {len(chips_convertidos)}")
        logger.info(f"  - Errores: {len(errores_conversion)}")
        
        if len(chips_convertidos) == 0:
            logger.error("❌ No se pudo convertir ninguna dirección a CHIP")
            return build_response(event, {
                "success": False,
                "message": f"No se pudieron obtener CHIPs de las direcciones. Errores: {', '.join(errores_conversion)}"
            }, 200)
        
        # Si hubo errores parciales, avisar pero continuar con los exitosos
        if len(errores_conversion) > 0:
            logger.warning(f"⚠️ {len(errores_conversion)} direccion(es) fallaron en conversión")
        
        chips = chips_convertidos
        logger.info(f"✅ CHIPs convertidos exitosamente: {chips}")
        
    else:
        # ============================================================
        # FLUJO 2: BuscarPredios - Usuario tiene >10 predios
        # ============================================================
        logger.info(f" FLUJO BUSCAR PREDIOS")
        logger.info(" No se proporcionaron direcciones, leyendo CHIPs de DynamoDB...")
        logger.info("   (Usuario buscó predios y los guardó en DynamoDB con BuscarPredios)")
        
        # ============================================================
        # IMPORTANTE: SIEMPRE validar DynamoDB primero, incluso en MOCK
        # Esto previene que se generen certificados sin predios asociados
        # ============================================================
        logger.info("📊 PASO 1: Validando si usuario tiene predios en DynamoDB...")
        chips = obtener_chips_seleccionados_desde_dynamo(documento)
        
        if not chips or len(chips) == 0:
            logger.error("❌ No se encontraron CHIPs seleccionados en DynamoDB")
            logger.error(f"  - Documento: {documento[:3]}***")
            logger.error(f"  - Esto significa que el usuario NO ha buscado/seleccionado predios")
            logger.error(f"  - O que la sesión expiró (TTL de 10 minutos)")
            
            return build_response(event, {
                "success": False,
                "message": "No has seleccionado ningún predio. Por favor busca y selecciona al menos un predio antes de generar certificados."
            }, 200)
        
        logger.info(f"✅ CHIPs recuperados de DynamoDB: {chips}")
        logger.info(f"  - Total de CHIPs: {len(chips)}")
        logger.info(f"  - Usuario SÍ tiene predios seleccionados")
    
    # Validar límite de certificados
    if len(chips) > MAX_CERTIFICADOS:
        logger.warning(f"⚠️ Se solicitaron {len(chips)} certificados, pero el límite es {MAX_CERTIFICADOS}")
        logger.warning(f"  - Se procesarán solo los primeros {MAX_CERTIFICADOS} CHIPs")
        chips = chips[:MAX_CERTIFICADOS]
    
    logger.info(f" Generando certificados para {len(chips)} predio(s)...")
    logger.info(f"  - CHIPs a procesar: {chips}")
    
    try:
        # ============================================================
        # PASO: Recuperar datos de sesión desde DynamoDB
        # ============================================================
        # NOTA: Incluso en modo MOCK, leemos de DynamoDB para obtener
        # el usuario y validar que la sesión existe
        # ============================================================
        logger.info(" PASO: Recuperando datos de sesión de DynamoDB...")
        session_data = get_session_data_from_dynamodb(documento)
        
        if not session_data:
            logger.error("❌ Datos de sesión no encontrados en DynamoDB")
            logger.error("  - Posibles causas:")
            logger.error("    1. Token expiró (TTL de 10 minutos)")
            logger.error("    2. Documento incorrecto")
            logger.error("    3. Usuario no completó validación OTP")
            
            # En modo MOCK, si no hay datos en DynamoDB, usar datos simulados
            # pero SOLO si es un usuario conocido de MOCK_USERS
            if ENABLE_MOCK and documento in MOCK_USERS:
                logger.warning("[MOCK] ⚠️ Sesión no encontrada en DynamoDB, usando datos MOCK")
                session_data = get_mock_session_data(documento)
                token = session_data.get('token', '')
                usuario = session_data.get('usuario', {})
            else:
                return build_response(event, {
                    "success": False,
                    "message": "Token de autenticación no encontrado o expirado. Por favor reinicia el proceso."
                }, 200)
        else:
            token = session_data.get('token', '')
            usuario = session_data.get('usuario', {})
        
        # Construir nombre completo desde usuario.nombre + usuario.apellido
        nombre = usuario.get('nombre', '')
        apellido = usuario.get('apellido', '')
        nombre_completo = f"{nombre} {apellido}".strip()
        
        if not nombre_completo:
            logger.warning("⚠️ No se pudo construir nombre completo desde DynamoDB")
            nombre_completo = "Nombre no disponible"
        
        logger.info(f"✅ Datos de usuario recuperados:")
        logger.info(f"  - Nombre: {nombre}")
        logger.info(f"  - Apellido: {apellido}")
        logger.info(f"  - Nombre completo construido: {nombre_completo}")
        logger.info(f"  - Email: {usuario.get('email', 'N/A')}")
        
        # 2. Generar certificados para cada CHIP
        logger.info(f" PASO 2: Generando certificados para {len(chips)} CHIP(s)...")
        
        resultados = []
        exitosos = 0
        fallidos = 0
        
        for idx, chip in enumerate(chips, 1):
            logger.info(f"\n--- Procesando CHIP {idx}/{len(chips)}: {chip} ---")
            
            # ============================================================
            # DECISIÓN: ¿Usar MOCK o API real para cada certificado?
            # ============================================================
            if ENABLE_MOCK:
                logger.info(f"[MOCK] 🎭 Generando certificado mock para CHIP: {chip}")
                resultado = get_mock_certificado_response(chip)
            else:
                logger.info(f"📡 Generando certificado REAL para CHIP: {chip}")
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
                logger.error(f"  - Error: {resultado.get('message', 'Error desconocido')}")
            
            resultados.append({
                "chip": chip,
                "success": resultado.get('success'),
                "requestNumber": resultado.get('requestNumber', ''),
                "message": resultado.get('message', '')
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
        
        return build_response(event, response, 200 if success else 200)  # 207 = Multi-Status
        
    except requests.exceptions.Timeout:
        logger.error("❌ TIMEOUT: API no respondió a tiempo")
        return build_response(event, {
            "success": False,
            "message": "Error técnico: timeout al generar los certificados. Por favor intenta nuevamente."
        }, 200)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ ERROR DE RED")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        return build_response(event, {
            "success": False,
            "message": "Error técnico al generar los certificados. Verifica tu conexión."
        }, 200)
        
    except Exception as e:
        logger.error(f"❌ ERROR INESPERADO")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return build_response(event, {
            "success": False,
            "message": "Error interno al procesar la generación de certificados."
        }, 200)


def get_session_data_from_dynamodb(documento):
    """
    Recupera los datos completos de sesión desde DynamoDB usando el documento.
    Incluye: token JWT, datos de usuario (nombre, apellido, email), chipsSeleccionados, etc.
    
    Args:
        documento: Número de documento del ciudadano (PK en DynamoDB)
    
    Returns:
        dict: Datos completos de la sesión o None si no se encuentra
        {
            'token': 'JWT...',
            'usuario': {
                'nombre': 'Juan',
                'apellido': 'Pérez',
                'email': 'juan@example.com',
                'numeroDocumento': '1234567890'
            },
            'chipsSeleccionados': ['AAA1234', 'BBB5678'],
            'tipoDocumento': 'CC',
            ...
        }
    """
    if not documento:
        logger.warning(" Documento vacío")
        return None
    
    logger.info(" Recuperando datos de sesión de DynamoDB...")
    logger.info(f"  - Tabla: {TABLE_TOKENS}")
    logger.info(f"  - Documento (PK): {documento[:3]}*** (longitud: {len(documento)})")
    
    try:
        table = dynamodb.Table(TABLE_TOKENS)
        
        response = table.get_item(Key={'documento': documento})
        
        if 'Item' not in response:
            logger.warning(f" No se encontró sesión en DynamoDB")
            logger.warning(f"  - Documento: {documento[:3]}***")
            return None
        
        item = response['Item']
        token = item.get('token', '')
        
        if not token:
            logger.warning(" Token vacío en DynamoDB")
            return None
        
        logger.info(f" Datos de sesión recuperados exitosamente")
        logger.info(f"  - Token (longitud): {len(token)} caracteres")
        logger.info(f"  - Token (primeros 30 chars): {token[:30]}***")
        logger.info(f"  - Usuario presente: {'usuario' in item}")
        logger.info(f"  - CHIPs seleccionados: {len(item.get('chipsSeleccionados', []))}")
        logger.info(f"  - Documento: {documento[:3]}***")
        
        return item
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f" Error de DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        logger.error(f"  - Documento: {documento[:3]}***")
        return None
    except Exception as e:
        logger.error(f" Error inesperado obteniendo datos de sesión")
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
        logger.warning(" Documento vacío")
        return []
    
    logger.info(" Recuperando CHIPs seleccionados de DynamoDB...")
    logger.info(f"  - Tabla: {TABLE_TOKENS}")
    logger.info(f"  - Documento (PK): {documento[:3]}*** (longitud: {len(documento)})")
    
    try:
        table = dynamodb.Table(TABLE_TOKENS)
        
        response = table.get_item(Key={'documento': documento})
        
        if 'Item' not in response:
            logger.warning(f" No se encontró registro en DynamoDB")
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
            logger.warning(f" chipsSeleccionados no es una lista, es: {type(chips_seleccionados)}")
            chips_seleccionados = []
        
        logger.info(f" CHIPs seleccionados recuperados exitosamente")
        logger.info(f"  - Total de CHIPs: {len(chips_seleccionados)}")
        logger.info(f"  - CHIPs: {chips_seleccionados}")
        logger.info(f"  - Documento: {documento[:3]}***")
        
        return chips_seleccionados
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f" Error de DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        logger.error(f"  - Documento: {documento[:3]}***")
        return []
    except Exception as e:
        logger.error(f" Error inesperado obteniendo CHIPs seleccionados")
        logger.error(f"  - Tipo: {type(e).__name__}")
        logger.error(f"  - Mensaje: {str(e)}")
        logger.exception("Stack trace completo:")
        return []


def obtener_chip_por_direccion(token, direccion):
    """
    Obtiene el CHIP de un predio usando su dirección.
    Llama al endpoint GET /properties/address/{address} que retorna la información del predio.
    Implementa exponential backoff para manejar intermitencias de red.
    
    Endpoint: GET /properties/address/{address}
    Ejemplo: http://vmprocondock.catastrobogota.gov.co:3400/catia-auth/properties/address/KR%207%206%2016%20SUR%20IN%203%20AP%20301
    
    Args:
        token: JWT token de autenticación
        direccion: Dirección del predio (ej: "KR 7 6 16 SUR IN 3 AP 301")
    
    Returns:
        dict con {success: bool, chip: str, message: str}
        Ejemplo exitoso: {"success": True, "chip": "AAA000008KLF", "message": "CHIP encontrado"}
        Ejemplo error: {"success": False, "chip": "", "message": "No se encontró predio"}
    """
    from urllib.parse import quote
    
    # URL encode de la dirección
    direccion_encoded = quote(direccion.strip())
    
    URL = f"{API_BASE_URL}/properties/address/{direccion_encoded}"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    logger.info(f"=== Obteniendo CHIP por dirección (con exponential backoff) ===")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - Dirección original: {direccion}")
    logger.info(f"  - Dirección encoded: {direccion_encoded}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
    
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
                        "chip": "",
                        "message": "El servidor retornó una respuesta vacía después de múltiples intentos"
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f" Respuesta vacía. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
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
                        "chip": "",
                        "message": "Respuesta inválida del servidor después de múltiples intentos"
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
                
                # Extraer CHIP del response
                # Estructura esperada: {"success": true, "data": {"chipPredio": {"CHIP": "AAA000008KLF"}}}
                data = response_data.get('data', {})
                chip_predio = data.get('chipPredio', {})
                chip = chip_predio.get('CHIP', '')
                
                if not chip:
                    # Intentar buscar en otros posibles campos
                    chip = data.get('CHIP', '') or data.get('chip', '')
                
                logger.info(f"  - CHIP extraído: {chip}")
                
                if chip:
                    return {
                        "success": True,
                        "chip": chip,
                        "message": "CHIP encontrado exitosamente"
                    }
                else:
                    logger.error(" API no retornó CHIP en la respuesta")
                    return {
                        "success": False,
                        "chip": "",
                        "message": "No se encontró CHIP en la respuesta del API"
                    }
            
            elif resp.status_code == 404:
                logger.warning(" Status 404 - Predio no encontrado")
                return {
                    "success": False,
                    "chip": "",
                    "message": response_data.get('message', 'No se encontró predio con esa dirección')
                }
            
            else:
                logger.error(f" Status {resp.status_code} - Error inesperado")
                logger.error(f"  - Response completo: {json.dumps(response_data, ensure_ascii=False)[:500]}")
                mensaje_error = response_data.get('message', f'Error en el servidor (status {resp.status_code})')
                return {
                    "success": False,
                    "chip": "",
                    "message": mensaje_error
                }
        
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f" Timeout en intento {attempt + 1}/{MAX_RETRIES} (15 segundos)")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "chip": "",
                    "message": "Tiempo de espera agotado al obtener el CHIP"
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
                    "chip": "",
                    "message": "No se pudo conectar con el servidor"
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
                    "chip": "",
                    "message": "Error en la solicitud HTTP al obtener el CHIP"
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except Exception as e:
            logger.exception(f" Error inesperado obteniendo CHIP: {str(e)}")
            return {
                "success": False,
                "chip": "",
                "message": "Error inesperado al obtener el CHIP"
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f" Falló después de {MAX_RETRIES} intentos")
    return {
        "success": False,
        "chip": "",
        "message": f"Error después de {MAX_RETRIES} intentos: {str(last_exception)}"
    }


def generar_certificado(token, chip):
    """
    El certificado es enviado automáticamente al correo del usuario.
    Implementa exponential backoff para manejar intermitencias de red.
    
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
    
    logger.info(f"=== Llamando API de generación de certificado (con exponential backoff) ===")
    logger.info(f"  - Endpoint: GET {URL}")
    logger.info(f"  - CHIP: {chip_limpio}")
    logger.info(f"  - Authorization: Bearer {token[:30]}***")
    logger.info(f"  - Max reintentos: {MAX_RETRIES}, Backoff inicial: {INITIAL_BACKOFF}s")
    
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"--- Intento {attempt + 1}/{MAX_RETRIES} ---")
            
            resp = requests.get(URL, headers=headers, timeout=30)  # Mayor timeout para generación
            
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
                        "requestNumber": ""
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
                logger.info(f"  - Success: {response_data.get('success', 'N/A')}")
                logger.info(f"  - Message: {response_data.get('message', 'N/A')}")
            except ValueError as ve:
                logger.error(f" Respuesta no es JSON válido")
                logger.error(f"  - Error: {str(ve)}")
                logger.error(f"  - Respuesta (primeros 300 chars): {resp.text[:300]}")
                
                if attempt == MAX_RETRIES - 1:
                    return {
                        "success": False,
                        "message": "Respuesta inválida del servidor después de múltiples intentos",
                        "requestNumber": ""
                    }
                
                backoff_time = calculate_backoff(attempt)
                logger.warning(f" Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Si llegamos aquí, la petición fue exitosa
            logger.info(f" Llamada al API completada exitosamente en intento {attempt + 1}")
            
            # Procesar respuesta según status code
            if resp.status_code == 200:
                logger.info(" Status 200 - Certificado generado exitosamente")
                
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
            else:
                logger.error(f" Status {resp.status_code} - Error inesperado")
                return {
                    "success": False,
                    "message": response_data.get('message', 'Error al generar el certificado'),
                    "requestNumber": ""
                }
        
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f" Timeout en intento {attempt + 1}/{MAX_RETRIES} (30 segundos)")
            
            if attempt == MAX_RETRIES - 1:
                logger.error(f" Timeout después de {MAX_RETRIES} intentos")
                return {
                    "success": False,
                    "message": "Tiempo de espera agotado al generar el certificado",
                    "requestNumber": ""
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
                    "requestNumber": ""
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
                    "message": "Error en la solicitud HTTP al generar el certificado",
                    "requestNumber": ""
                }
            
            backoff_time = calculate_backoff(attempt)
            logger.warning(f" Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
        
        except Exception as e:
            logger.exception(f" Error inesperado en generación de certificado: {str(e)}")
            return {
                "success": False,
                "message": "Error inesperado al generar el certificado",
                "requestNumber": ""
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f" Falló después de {MAX_RETRIES} intentos")
    return {
        "success": False,
        "message": f"Error después de {MAX_RETRIES} intentos: {str(last_exception)}",
        "requestNumber": ""
    }


def guardar_auditoria(documento, tipo_documento, nombre_completo, chip, request_number):
    """
    Guarda la auditoría de la generación del certificado en DynamoDB.
    
    Tabla: cat-test-certification-data
    
    Campos:
    - id (PK): ID único
    - nombre: Nombre completo del ciudadano
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
    logger.info(f" Guardando auditoría en DynamoDB...")
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
        
        logger.info(f" Auditoría guardada exitosamente")
        logger.info(f"  - ID de auditoría: {audit_id}")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f" Error al guardar auditoría en DynamoDB: {error_code}")
        logger.error(f"  - Mensaje: {error_message}")
        return False
    except Exception as e:
        logger.error(f" Error inesperado guardando auditoría")
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
    logger.info(f" Construyendo respuesta para Bedrock Agent:")
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
    
    logger.info(" Respuesta formateada correctamente")
    return formatted_response
