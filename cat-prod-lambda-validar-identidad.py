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
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://vmprocondock.catastrobogota.gov.co:3400/catia-auth')
API_KEY = os.environ.get('API_KEY', '')

# Configuración de reintentos con exponential backoff
MAX_RETRIES = 8
INITIAL_BACKOFF = 1  # segundos
MAX_BACKOFF = 20  # segundos

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
        if not documento or not tipo_documento or not nombre:
            logger.error("Validación fallida: Parámetros requeridos faltantes")
            return format_bedrock_response(
                status_code=400,
                body={
                    "success": False,
                    "message": "Parámetros requeridos faltantes: nombre, documento y tipoDocumento"
                },
                event=event
            )
        
        logger.info("Parámetros validados correctamente")
        
        # Call external API
        logger.info("Iniciando llamada al API externo")
        api_response = call_identity_validation_api(tipo_documento, documento)
        
        # Process API response
        if api_response['status_code'] == 200 and api_response.get('data', {}).get('success', False) == True:
            logger.info("API respondió exitosamente con status 200")
            response_data = api_response['data']
            logger.info(f"Datos de respuesta del API: {json.dumps(response_data)}")
            
            # Map API response to expected schema
            validation_result = {
                "success": response_data.get('success', False),
                "message": response_data.get('data', {}).get('message', ''),
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
            print("API response:", api_response)
            # Handle API errors
            error_data = api_response.get('data', {})
            logger.error(f"API respondió con error - Status: {api_response['status_code']}, Error: {error_data.get('message', 'Error desconocido')}, Error Code: {error_data.get('errorCode', 'N/A')}")
            return format_bedrock_response(
                status_code=api_response['status_code'],
                body={
                    "success": error_data.get('success', False),
                    "message": f"Error en la validación: {error_data.get('message', 'Error desconocido')}"
                },
                event=event
            )
    
    except Exception as e:
        # Handle unexpected errors
        logger.exception(f"Error inesperado en lambda_handler: {str(e)}")
        return format_bedrock_response(
            status_code=500,
            body={
                "success": False,
                "message": f"Error interno del servidor: {str(e)}"
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
                
                # Si es el último intento, retornar error
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 200,
                        'data': {
                            'success': False,
                            'message': 'El API retornó una respuesta vacía después de múltiples intentos'
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
                
                # Si es el último intento, retornar error
                if attempt == MAX_RETRIES - 1:
                    return {
                        'status_code': 200,
                        'data': {
                            'success': False,
                            'message': 'Error al parsear JSON de la respuesta del API'
                        },
                        'error': f'Respuesta del API no es un JSON válido después de múltiples intentos. Content-Type: {content_type}, Content: {response.text[:200]}'
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
                    'data': {
                        'success': False,
                        'message': 'No se pudo conectar con el API'
                    },
                    'error': f'No se pudo conectar con el API después de múltiples intentos: {str(e)}'
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
                    'data': {
                        'success': False,
                        'message': 'Error en la solicitud HTTP al conectar con el API'
                    },
                        'error': f'Error en la solicitud HTTP después de múltiples intentos: {str(e)}'
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
                'data': {
                    'success': False,
                    'message': 'Error inesperado al conectar con el API'
                },
                'error': f'Error inesperado: {str(e)}'
            }
    
    # Si llegamos aquí, algo salió mal en todos los intentos
    logger.error(f"Falló después de {MAX_RETRIES} intentos")
    return {

        'status_code': 200,
        'message': f'Error después de {MAX_RETRIES} intentos: {str(last_exception)}'
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