import json
import os
import requests
import logging
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://vmprocondock.catastrobogota.gov.co:3400/auth-catia/')
API_KEY = os.environ.get('API_KEY', '')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for Bedrock Agent - Validar Identidad
    
    Args:
        event: Event from Bedrock Agent containing requestBody
        context: Lambda context object
    
    Returns:
        Response formatted for Bedrock Agent
    """
    print("API BASE:", API_BASE_URL)
    logger.info("=== Iniciando Lambda - Validar Identidad ===")
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
        
        logger.info(f"Parámetros extraídos - Tipo: {tipo_documento}, Documento: {documento}")
        
        # Validate required parameters
        if not documento or not tipo_documento or not nombre:
            logger.error("Validación fallida: Parámetros requeridos faltantes")
            return format_bedrock_response(
                status_code=400,
                body={
                    "valido": False,
                    "mensaje": "Parámetros requeridos faltantes: documento y tipoDocumento"
                },
                event=event
            )
        
        logger.info("Parámetros validados correctamente")
        
        # Call external API
        logger.info("Iniciando llamada al API externo")
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
                "nombre": nombre   # Not provided by API, may need additional call
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
        # Handle unexpected errors
        logger.exception(f"Error inesperado en lambda_handler: {str(e)}")
        return format_bedrock_response(
            status_code=500,
            body={
                "valido": False,
                "mensaje": f"Error interno del servidor: {str(e)}"
            },
            event=event
        )


def call_identity_validation_api(tipo_documento: str, numero_documento: str) -> Dict[str, Any]:
    """
    Call the identity validation API
    
    Args:
        tipo_documento: Type of identity document
        numero_documento: Document number
    
    Returns:
        Dictionary with status_code, data, and optional error
    """
    try:
        # Prepare API request
        url = f"{API_BASE_URL}/auth/temp-key"
        
        payload = {
            "tipoDocumento": tipo_documento,
            "numeroDocumento": numero_documento,
            "validInput": True  # Assuming input is validated
        }
        
        headers = {
            'Content-Type': 'application/json',
        }
        
        # Add API key if available
        if API_KEY:
            headers['Authorization'] = f'Bearer {API_KEY}'
        
        # Make HTTP request
        response = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        # Parse response
        response_data = response.json()
        
        return {
            'status_code': response.status_code,
            'data': response_data
        }
    
    except requests.exceptions.Timeout:
        return {
            'status_code': 504,
            'error': 'Tiempo de espera agotado al conectar con el API'
        }
    except requests.exceptions.ConnectionError:
        return {
            'status_code': 503,
            'error': 'No se pudo conectar con el API'
        }
    except requests.exceptions.RequestException as e:
        return {
            'status_code': 500,
            'error': f'Error en la solicitud HTTP: {str(e)}'
        }
    except json.JSONDecodeError:
        return {
            'status_code': 500,
            'error': 'Respuesta del API no es un JSON válido'
        }
    except Exception as e:
        return {
            'status_code': 500,
            'error': f'Error inesperado: {str(e)}'
        }


def format_bedrock_response(status_code: int, body: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format response for Bedrock Agent
    
    Args:
        status_code: HTTP status code
        body: Response body
    
    Returns:
        Formatted response for Bedrock Agent
    """
    return {
        'messageVersion': '1.0',
        'response': {
            'actionGroup': event.get('actionGroup', ''),
            'apiPath': event.get('apiPath', ''),
            'httpMethod': event.get('httpMethod', ''),
            'httpStatusCode': status_code,
            'responseBody': {
                'application/json': {
                    'body': json.dumps(body)
                }
            }
        }
    }