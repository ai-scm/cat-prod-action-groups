"""
Lambda Function: Contar Predios
Obtiene el número de predios asociados al documento del usuario
"""

import json
import logging
import time
import requests
import boto3
import os
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
TABLE_NAME = 'cat-test-certification-session-tokens'

# API destino
API_BASE_URL = os.environ.get(
    'API_BASE_URL',
    'http://vmprocondock.catastrobogota.gov.co:3400/catia-auth'
)

# Exponential backoff
MAX_RETRIES = 5
INITIAL_BACKOFF = 1
MAX_BACKOFF = 8


# ============================================================
#  MAIN HANDLER
# ============================================================

def lambda_handler(event, context):
    logger.info("=== Lambda: Contar Predios ===")
    logger.info(f"Event: {json.dumps(event)}")

    # Obtención del documento enviado por Bedrock Agent
    documento = extract_documento(event)

    if not documento:
        return api_error(
            event,
            message="El documento es requerido",
            code="MISSING_DOCUMENTO",
            status=400
        )

    logger.info(f"Consultando predios para documento: {documento[:3]}***")

    # Recuperar token de DynamoDB
    token = get_token_from_dynamodb(documento)
    if not token:
        return api_error(
            event,
            message="Sesión no autenticada. Por favor, valida tu identidad primero.",
            code="TOKEN_NOT_FOUND",
            status=401
        )

    logger.info("Token recuperado correctamente")

    # Llamar a la API externa
    try:
        api_response = call_contar_predios_api(token)
    except requests.exceptions.Timeout:
        return api_error(
            event,
            message="Error técnico: timeout al consultar predios",
            code="TIMEOUT",
            status=502
        )
    except requests.exceptions.RequestException:
        return api_error(
            event,
            message="Error técnico al consultar predios",
            code="NETWORK_ERROR",
            status=502
        )
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}", exc_info=True)
        return api_error(
            event,
            message="Error interno al procesar la solicitud",
            code="INTERNAL_ERROR",
            status=500
        )

    # Procesamiento de la respuesta según código HTTP
    status = api_response['status_code']
    data = api_response.get('data', {})
    message = data.get("message", "")

    if status == 200:
        return build_response(event, {
            "success": True,
            "message": message or "Consulta exitosa",
            "data": data.get("data", {}),
            "errorCode": ""
        }, 200)

    if status == 405:
        return api_error(event,
            message=message or "El usuario no se encuentra activo",
            code="USER_INACTIVE",
            status=405
        )

    if status == 406:
        return api_error(event,
            message=message or "El usuario no ha diligenciado las preguntas de seguridad",
            code="NO_SECURITY_QUESTIONS",
            status=406
        )

    if status == 401:
        return api_error(event,
            message="Tu sesión ha expirado. Por favor valida tu identidad nuevamente.",
            code="TOKEN_EXPIRED",
            status=401
        )

    # Cualquier otro error devuelto por el backend
    return api_error(event,
        message=data.get("message", "Error al consultar los predios"),
        code="API_ERROR",
        status=status
    )


# ============================================================
#  HELPERS
# ============================================================

def extract_documento(event):
    """
    Extrae el documento del payload enviado por Bedrock Agent.
    """
    if "requestBody" in event and "content" in event["requestBody"]:
        content = event["requestBody"]["content"]

        if "application/json" in content:
            properties = content["application/json"].get("properties", [])
            body = {p["name"]: p["value"] for p in properties}
            return body.get("documento", "")

    # Caso de pruebas locales
    return event.get("documento", "")


def get_token_from_dynamodb(documento):
    """
    Recupera el token asociado al documento.
    """
    try:
        table = dynamodb.Table(TABLE_NAME)
        resp = table.get_item(Key={"documento": documento})

        if "Item" in resp:
            token = resp["Item"].get("token")
            return token

        logger.warning(f"No se encontró token para documento {documento}")
        return None

    except Exception as e:
        logger.error(f"Error accediendo a DynamoDB: {str(e)}")
        return None


def call_contar_predios_api(token):
    """
    Llama al API externo con exponential backoff.
    """
    URL = f"{API_BASE_URL}/properties/count"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    payload = {}

    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Intento {attempt + 1}/{MAX_RETRIES}")

            resp = requests.get(
                URL,
                json=payload,
                headers=headers,
                timeout=15
            )

            logger.info(f"Status: {resp.status_code}")

            # Respuesta vacía → reintentar
            if not resp.content:
                if attempt == MAX_RETRIES - 1:
                    return {
                        "status_code": 500,
                        "error": "Respuesta vacía del API"
                    }

                time.sleep(calculate_backoff(attempt))
                continue

            try:
                data = resp.json()
            except ValueError:
                if attempt == MAX_RETRIES - 1:
                    return {
                        "status_code": 500,
                        "error": "Respuesta no es JSON válido"
                    }

                time.sleep(calculate_backoff(attempt))
                continue

            return {
                "status_code": resp.status_code,
                "data": data
            }

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == MAX_RETRIES - 1:
                raise

            time.sleep(calculate_backoff(attempt))

    return {
        "status_code": 500,
        "error": "Falló después de múltiples intentos"
    }


def calculate_backoff(attempt):
    return min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)


# ============================================================
#  FORMATO RESPUESTA
# ============================================================

def api_error(event, message, code, status):
    return build_response(event, {
        "success": False,
        "message": message,
        "data": {},
        "errorCode": code
    }, status)


def build_response(event, response_data, status_code=200):
    """
    Formato requerido por Bedrock Agents Runtime.
    """
    response = {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get("actionGroup", "ContarPredios"),
            "apiPath": event.get("apiPath", "/contar-predios"),
            "httpMethod": event.get("httpMethod", "GET"),
            "httpStatusCode": status_code,
            "responseBody": {
                "application/json": {
                    "body": json.dumps(response_data, ensure_ascii=False)
                }
            }
        }
    }

    logger.info(f"Response → {json.dumps(response, ensure_ascii=False)}")
    return response
