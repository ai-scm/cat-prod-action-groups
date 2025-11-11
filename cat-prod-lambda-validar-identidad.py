"""
Lambda Function: Validar Identidad

"""
import json
import logging
import os
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# URL = "http://vmprocondock.catastrobogota.gov.co:3400/auth/temp-key"
URL = "http://10.34.116.98:3400/auth/temp-key"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def call_validar_ciudadano(tipo_documento: str, numero_documento: str, valid_input: bool = True, timeout: int = 10):
    """
    Realiza un POST a la API CEL para validar la identidad.
    Body:
    {
      "tipoDocumento": "CC",
      "numeroDocumento": "123456",
      "validInput": true
    }
    """
    headers = {
        "Content-Type": "application/json"
    }

    payload = {
        "tipoDocumento": tipo_documento,
        "numeroDocumento": numero_documento,
        "validInput": bool(valid_input)
    }

    logger.info(f"POST {URL}")
    # Avoid logging sensitive values like API keys; logging payload minimally
    logger.debug(f"Payload: {json.dumps(payload)}")

    try:
        resp = requests.post(URL, json=payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling validar-ciudadano: {e}")
        # Re-raise to allow caller to handle or return structured error
        raise

    # Try parse json, fallback to raw text
    try:
        return resp.json()
    except ValueError:
        return {"raw_response": resp.text, "status_code": resp.status_code}

def handler(event, context):
    """
    Valida la identidad del ciudadano con la API externa

    Input esperado:
    {
        "documento": "1234567890"
    }

    Output:
    {
        "valido": true,
        "correo": "usuario@ejemplo.com",
        "correo_ofuscado": "u***@ejemplo.com",
        "nombre": "Juan Pérez"
    }
    """
    logger.info("=== Lambda: Validar Identidad ===")
    logger.info(f"Event: {json.dumps(event)}")

    # Extraer documento del evento - Bedrock Agent envía en requestBody
    if 'requestBody' in event and 'content' in event['requestBody']:
        # Formato de Bedrock Agent
        content = event['requestBody']['content']
        if 'application/json' in content:
            # Bedrock Agent envía properties como array de objetos
            properties = content['application/json']['properties']
            # Construir diccionario desde el array de properties
            body = {prop['name']: prop['value'] for prop in properties}
            documento = body.get('documento', '')
            tipo_documento = body.get('tipoDocumento', 'CC')
        else:
            documento = ''
            # tipo_documento = 'CC'
    else:
        # Formato directo para testing
        documento = event.get('documento', '')
        tipo_documento = event.get('tipoDocumento', 'CC')

    logger.info(f"Validando documento: {documento} (tipo: {tipo_documento})")

    try:
        api_result = call_validar_ciudadano(tipo_documento, documento, valid_input=True)
    except Exception as e:
        # Return a structured error to the caller (502 Bad Gateway semantics)
        error_response = {
            "valido": False,
            "mensaje": "Error al validar identidad",
            "error": str(e)
        }
        logger.info(f"Response error: {json.dumps(error_response)}")
        return {
            "messageVersion": "1.0",
            "response": {
                "actionGroup": event.get('actionGroup', 'ValidarIdentidad'),
                "apiPath": event.get('apiPath', '/validar-identidad'),
                "httpMethod": event.get('httpMethod', 'POST'),
                "httpStatusCode": 502,
                "responseBody": {
                    "application/json": {
                        "body": json.dumps(error_response)
                    }
                }
            }
        }

    # If the external API returned a dict, use it; otherwise nest raw_response
    response_body = api_result if isinstance(api_result, dict) else {"result": api_result}

    logger.info(f"✅ Response from API: {json.dumps(response_body)}")

    # Bedrock Agent espera el body directamente
    return {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get('actionGroup', 'ValidarIdentidad'),
            "apiPath": event.get('apiPath', '/validar-identidad'),
            "httpMethod": event.get('httpMethod', 'POST'),
            "httpStatusCode": 200,
            "responseBody": {
                "application/json": {
                    "body": json.dumps(response_body)
                }
            }
        }
    }