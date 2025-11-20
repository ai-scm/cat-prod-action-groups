import json
import os
import requests
import logging
import time
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://vmprocondock.catastrobogota.gov.co:3400/auth-catia')
API_KEY = os.environ.get('API_KEY', '')

MAX_RETRIES = 8
INITIAL_BACKOFF = 1  # segundos
MAX_BACKOFF = 60  # segundos


MOCK_USERS = {
    "135791113": {
        "success": True,
        "message": "Clave temporal enviada exitosamente",
        "data": {
            "mensaje": "Clave temporal enviada exitosamente",
            "emailOfuscado": "j***@blend360.com",
            "tiempoExpiracion": 5
        },
    "timestamp": "2025-11-20T13:31:16.768Z"
    },
    "24681012": {
        "success": True,
        "message": "Clave temporal enviada exitosamente",
        "data": {
            "mensaje": "Clave temporal enviada exitosamente",
            "emailOfuscado": "j***@blend360.com",
            "tiempoExpiracion": 5
        },
    "timestamp": "2025-11-20T13:31:16.768Z"
    },
    }

ENABLE_MOCK = os.environ.get('ENABLE_MOCK', 'true').lower() == 'true'



def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Bedrock Agent - Validar Identidad
    
    Args:
        event: Event from Bedrock Agent containing requestBody
        context: Lambda context object
    
    Returns:
        Response formatted for Bedrock Agent
    """
    logger.info("=== Iniciando Lambda - Validar Identidad ===")
    logger.info(f"API_BASE_URL: {API_BASE_URL}")
    logger.info(f"Event recibido: {json.dumps(event)}")
    
    try:
        # Extract parameters from Bedrock Agent event
        logger.info("Extrayendo parámetros del evento de Bedrock Agent")
        request_body = event.get('requestBody', {})
        content = request_body.get('content', {})
        application_json = content.get('application/json', {})
        properties = application_json.get('properties', [])
        
        # Parse input parameters
        nombre = None
        documento = None
        tipo_documento = None
        
        for prop in properties:
            if prop.get('name') == 'documento':
                documento = prop.get('value')
            elif prop.get('name') == 'tipoDocumento':
                tipo_documento = prop.get('value')
            elif prop.get('name') == 'nombre':
                nombre = prop.get('value')
        
        logger.info(f"Parámetros extraídos - Nombre: {nombre}, Tipo: {tipo_documento}, Documento: {documento}")
        
        # Validate required parameters
        if not documento or not tipo_documento:
            logger.error("Validación fallida: Parámetros requeridos faltantes")
            return format_bedrock_response(
                status_code=400,
                body={
                    "success": False,
                    "message": "Parámetros requeridos faltantes: documento y tipoDocumento",
                    "errorCode": "MISSING_REQUIRED_PARAMS"
                },
                event=event
            )
        
        logger.info("Parámetros validados correctamente")
        
        # Call external API
        logger.info("Iniciando llamada al API externo")
        
        # Check if mock mode is enabled
        if ENABLE_MOCK:
            logger.warning("🎭 MOCK MODE ENABLED - Using test data instead of real API")
            api_response = get_mock_response(documento, tipo_documento)
        else:
            api_response = call_identity_validation_api(tipo_documento, documento)
        
        # Process API response
        if api_response['status_code'] == 200:
            logger.info("API respondió exitosamente con status 200")
            response_data = api_response['data']
            logger.info(f"Datos de respuesta del API: {json.dumps(response_data)}")
            
            # Map API response to expected schema
            validation_result = {
                "valido": response_data.get('success', False),
                "mensaje": response_data.get('data', {}).get('mensaje', ''),
                "correo_ofuscado": response_data.get('data', {}).get('emailOfuscado', ''),
                "correo": "",  # Not provided by API
                "nombre": nombre
            }
            
            logger.info(f"Resultado de validación mapeado: {json.dumps(validation_result)}")
            logger.info("=== Lambda completado exitosamente ===")
            
            return format_bedrock_response(
                status_code=200,
                body=validation_result,
                event=event
            )
        else:
            # Handle API errors
            logger.error(f"API respondió con error - Status: {api_response['status_code']}, Error: {api_response.get('error', 'Error desconocido')}")
            return format_bedrock_response(
                status_code=api_response['status_code'],
                body={
                    "valido": False,
                    "mensaje": f"Error en la validación: {api_response.get('error', 'Error desconocido')}"
                },
                event=event
            )
    
    except Exception as e:
        # Handle unexpected errors - Return 200 with error body so Bedrock can handle it
        logger.exception(f"Error inesperado en lambda_handler: {str(e)}")
        logger.warning("⚠️ Retornando 200 con error en body para que Bedrock pueda procesarlo")
        return format_bedrock_response(
            status_code=200,  # Changed from 500 to 200
            body={
                "success": False,
                "message": "Ocurrió un error inesperado. Por favor, intenta nuevamente.",
                "errorCode": "INTERNAL_SERVER_ERROR"
            },
            event=event
        )


def call_identity_validation_api(tipo_documento: str, numero_documento: str) -> Dict[str, Any]:
    """
    Call the identity validation API
    Implementa exponential backoff para manejar intermitencias de red
    
    Args:
        tipo_documento: Type of identity document
        numero_documento: Document number
    
    Returns:
        Dictionary with status_code, data, and optional error
    """
    logger.info(f"=== Llamando API de validación (con exponential backoff) ===")
    
    url = f"{API_BASE_URL}/auth/temp-key"
    
    payload = {
        "tipoDocumento": tipo_documento,
        "numeroDocumento": numero_documento,
        "validInput": True
    }
    
    logger.info(f"URL completa: {url}")
    logger.info(f"Payload a enviar: {json.dumps(payload)}")
    logger.info(f"Max reintentos: {MAX_RETRIES}, Backoff inicial: {INITIAL_BACKOFF}s")
    
    headers = {
        'Content-Type': 'application/json',
    }
    
    # Add API key if available
    if API_KEY:
        headers['Authorization'] = f'Bearer {API_KEY}'
        logger.info("API Key agregado al header de autorización")
    else:
        logger.warning("No se encontró API_KEY en las variables de entorno")
    
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"--- Intento {attempt + 1}/{MAX_RETRIES} ---")
            logger.info("Enviando petición POST al API")
            
            # Make HTTP request
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=30
            )
            
            logger.info(f"Respuesta recibida - Status Code: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            logger.info(f"Response content length: {len(response.content)} bytes")
            logger.info(f"Response raw content: {response.text[:500]}")  # Log first 500 chars
            
            # Check if response is empty
            if not response.content or len(response.content) == 0:
                logger.error("Respuesta vacía del API")
                
                # Si es el último intento, retornar error (matching OpenAPI 500 schema)
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 500,
                        'data': {
                            'success': False,
                            'message': 'El API retornó una respuesta vacía después de múltiples intentos',
                            'errorCode': 'EMPTY_RESPONSE'
                        }
                    }
                
                # Aplicar backoff y reintentar
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"Respuesta vacía. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Check content type
            content_type = response.headers.get('Content-Type', '')
            logger.info(f"Content-Type de respuesta: {content_type}")
            
            if 'application/json' not in content_type.lower():
                logger.warning(f"Content-Type no es JSON: {content_type}")
                logger.warning(f"Respuesta completa: {response.text}")
            
            # Try to parse response
            try:
                response_data = response.json()
                logger.info(f"Response body parseado exitosamente: {json.dumps(response_data)}")
            except json.JSONDecodeError as json_err:
                logger.error(f"Error al parsear JSON: {str(json_err)}")
                logger.error(f"Contenido que causó el error: {response.text}")
                
                # Si es el último intento, retornar error (matching OpenAPI 500 schema)
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 500,
                        'data': {
                            'success': False,
                            'message': f'Respuesta del API no es un JSON válido después de múltiples intentos',
                            'errorCode': 'INVALID_JSON_RESPONSE'
                        }
                    }
                
                # Aplicar backoff y reintentar
                backoff_time = calculate_backoff(attempt)
                logger.warning(f"Error parseando JSON. Reintentando en {backoff_time}s...")
                time.sleep(backoff_time)
                continue
            
            # Si llegamos aquí, la petición fue exitosa
            logger.info(f"✅ Llamada al API completada exitosamente en intento {attempt + 1}")
            
            return {
                'status_code': response.status_code,
                'data': response_data
            }
            
        except requests.exceptions.Timeout as e:
            last_exception = e
            logger.error(f"Timeout en intento {attempt + 1}/{MAX_RETRIES} (30 segundos)")
            
            # Si es el último intento, retornar error (matching OpenAPI 504 schema)
            if attempt == MAX_RETRIES - 1:
                logger.error(f"❌ Timeout después de {MAX_RETRIES} intentos")
                return {
                    'status_code': 504,
                    'data': {
                        'success': False,
                        'message': 'Tiempo de espera agotado al conectar con el API después de múltiples intentos'
                    }
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.ConnectionError as e:
            last_exception = e
            logger.error(f"Error de conexión en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, retornar error (matching OpenAPI 503 schema)
            if attempt == MAX_RETRIES - 1:
                logger.error(f"❌ Error de conexión después de {MAX_RETRIES} intentos")
                return {
                    'status_code': 503,
                    'data': {
                        'success': False,
                        'message': f'No se pudo conectar con el API después de múltiples intentos'
                    }
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except requests.exceptions.RequestException as e:
            last_exception = e
            logger.error(f"Error en la solicitud HTTP en intento {attempt + 1}/{MAX_RETRIES}: {str(e)}")
            
            # Si es el último intento, retornar error (matching OpenAPI 500 schema)
            if attempt == MAX_RETRIES - 1:
                logger.error(f"❌ Error en solicitud HTTP después de {MAX_RETRIES} intentos")
                return {
                    'status_code': 500,
                    'data': {
                        'success': False,
                        'message': f'Error en la solicitud HTTP después de múltiples intentos',
                        'errorCode': 'HTTP_REQUEST_ERROR'
                    }
                }
            
            # Aplicar exponential backoff
            backoff_time = calculate_backoff(attempt)
            logger.warning(f"⏳ Esperando {backoff_time}s antes de reintentar...")
            time.sleep(backoff_time)
            
        except Exception as e:
            # Para errores inesperados, no reintentar (matching OpenAPI 500 schema)
            logger.exception(f"Error inesperado en call_identity_validation_api: {str(e)}")
            return {
                'status_code': 500,
                'data': {
                    'success': False,
                    'message': f'Error inesperado: {str(e)}',
                    'errorCode': 'UNEXPECTED_ERROR'
                }
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos (matching OpenAPI 500 schema)
    logger.error(f"❌ Falló después de {MAX_RETRIES} intentos")
    return {
        'status_code': 500,
        'data': {
            'success': False,
            'message': f'Error después de {MAX_RETRIES} intentos: {str(last_exception)}',
            'errorCode': 'MAX_RETRIES_EXCEEDED'
        }
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


def get_mock_response(documento: str, tipo_documento: str) -> Dict[str, Any]:
    """
    Returns a mock API response based on test user data
    
    Args:
        documento: Document number
        tipo_documento: Document type
    
    Returns:
        Dictionary with status_code and data matching real API response format
    """
    logger.info(f"🎭 Getting mock response for documento: {documento[:3]}***")
    
    # Simulate network delay (random between 0.5-2 seconds)
    import random
    delay = random.uniform(0.5, 2.0)
    logger.info(f"🎭 Simulating API delay: {delay:.2f}s")
    time.sleep(delay)
    
    # Check if user exists in mock data
    if documento in MOCK_USERS:
        mock_data = MOCK_USERS[documento]
        logger.info(f"🎭 Mock user found: {documento[:3]}***")
        logger.info(f"🎭 Mock response: {json.dumps(mock_data)}")
        
        # Determine status code based on success field
        if mock_data.get('success', False):
            status_code = 200
        else:
            # Map error codes to appropriate status codes
            error_code = mock_data.get('errorCode', '')
            if error_code in ['USER_NOT_FOUND', 'NO_EMAIL']:
                status_code = 404
            elif error_code == 'USER_INACTIVE':
                status_code = 403
            elif error_code == 'INVALID_DOCUMENT_TYPE':
                status_code = 400
            else:
                status_code = 400
        
        return {
            'status_code': status_code,
            'data': mock_data
        }
    else:
        # User not in mock data - return user not found
        logger.warning(f"🎭 Mock user NOT found: {documento[:3]}*** - Returning USER_NOT_FOUND")
        return {
            'status_code': 404,
            'data': {
                'success': False,
                'message': f'Usuario con documento {documento} no encontrado en datos de prueba',
                'errorCode': 'USER_NOT_FOUND'
            }
        }


def format_bedrock_response(status_code: int, body: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format response for Bedrock Agent
    
    Args:
        status_code: HTTP status code
        body: Response body
        event: Original event from Bedrock Agent
    
    Returns:
        Formatted response for Bedrock Agent
    """
    logger.info(f"Formateando respuesta para Bedrock Agent - Status: {status_code}")
    
    formatted_response = {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': event.get('actionGroup', ''),
            'apiPath': event.get('apiPath', ''),
            'httpMethod': event.get('httpMethod', ''),
            'httpStatusCode': status_code,
            'responseBody': {
                'application/json': {
                    'body': json.dumps(body, ensure_ascii=False)
                }
            }
        }
    }
    
    logger.info(f"Respuesta formateada: {json.dumps(formatted_response, ensure_ascii=False)}")
    return formatted_response