"""
Lambda Function: Consultar Certificados
"""
import json
import logging
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_certificados(documento):
    """
    Función para consultar certificados desde un servicio externo.
    Actualmente no implementada, se usa mock en su lugar.
    """
    # Aquí iría la lógica para llamar a un servicio externo
    # Por ejemplo:
    url = f"http://10.34.116.98:4000/consultar?documento={documento}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error al consultar certificados: {response.status_code}")
        return []
    pass  # Implementación futura

def handler(event, context):
    """
    Consulta los certificados disponibles para un documento
    
    Input esperado:
    {
        "documento": "1234567890"
    }
    
    Output:
    {
        "certificados": [
            {
                "id": 1,
                "tipo": "Certificado de Tradición y Libertad",
                "direccion": "Calle 123 #45-67",
                "matricula": "50N-123456",
                "chip": "AAA0001234567"
            },
            ...
        ]
    }
    """
    logger.info("=== Lambda: Consultar Certificados ===")
    logger.info(f"Event: {json.dumps(event)}")
    
    # Extraer documento del evento - Bedrock Agent envía en requestBody
    if 'requestBody' in event and 'content' in event['requestBody']:
        content = event['requestBody']['content']
        if 'application/json' in content:
            # Bedrock Agent envía properties como array de objetos
            properties = content['application/json']['properties']
            body = {prop['name']: prop['value'] for prop in properties}
            documento = body.get('documento', '')
        else:
            documento = ''
    else:
        documento = event.get('documento', '')
    
    try:


    
    
    # Mock: generar certificados de prueba
    certificados_mock = [
        {
            "id": 1,
            "tipo": "Certificado de Tradición y Libertad",
            "direccion": "Calle 123 #45-67, Bogotá",
            "matricula": "50N-123456",
            "chip": "AAA0001234567"
        },
        {
            "id": 2,
            "tipo": "Paz y Salvo Predial",
            "direccion": "Carrera 7 #8-90, Bogotá",
            "matricula": "50N-789012",
            "chip": "AAA0007890123"
        },
        {
            "id": 3,
            "tipo": "Certificado Catastral",
            "direccion": "Calle 100 #15-20, Bogotá",
            "matricula": "50N-345678",
            "chip": "AAA0003456789"
        }
    ]
    
    response = {
        "certificados": certificados_mock,
        "total": len(certificados_mock)
    }
    
    logger.info(f"✅ Response mock: {json.dumps(response, indent=2)}")
    logger.info(f"📊 Total certificados encontrados: {len(certificados_mock)}")
    
    return {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get('actionGroup', 'ConsultarCertificados'),
            "apiPath": event.get('apiPath', '/consultar-certificados'),
            "httpMethod": event.get('httpMethod', 'POST'),
            "httpStatusCode": 200,
            "responseBody": {
                "application/json": {
                    "body": json.dumps(response)
                }
            }
        }
    }
