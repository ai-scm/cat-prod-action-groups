"""
Lambda Function: Validar Identidad
Mock implementation - solo prints por ahora
"""
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        else:
            documento = ''
    else:
        # Formato directo para testing
        documento = event.get('documento', '')
    
    logger.info(f"🔍 AQUI VA: GET a API externa para validar documento: {documento}")
    logger.info(f"📡 Endpoint: POST https://api-catastro.com/v1/validar-ciudadano")
    logger.info(f"📦 Payload: {{'documento': '{documento}'}}")
    
    # Mock response
    response = {
        "valido": True,
        "correo": "usuario@ejemplo.com",
        "correo_ofuscado": "u***@ejemplo.com",
        "nombre": "Juan Pérez",
        "mensaje": "Identidad validada correctamente"
    }
    
    logger.info(f"✅ Response mock: {json.dumps(response)}")
    
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
                    "body": json.dumps(response)
                }
            }
        }
    }
