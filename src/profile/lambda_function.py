import json
import os
import boto3
from boto3.dynamodb.conditions import Key

# Cliente DynamoDB
dynamodb = boto3.resource("dynamodb")

def lambda_handler(event, context):
    """
    Lambda de consulta de perfil da Mini CDP
    Recebe um userId via path parameter e retorna o perfil consolidado
    """

    print("Evento recebido:", json.dumps(event))

    # Extrai o userId do path parameter
    # Ex: GET /profile/user_123 → pathParameters = {"userId": "user_123"}
    user_id = event.get("pathParameters", {}).get("userId")

    if not user_id:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "userId é obrigatório"
            })
        }

    # Consulta o perfil no DynamoDB
    profile = get_profile(user_id)

    if not profile:
        return {
            "statusCode": 404,
            "body": json.dumps({
                "error": f"Perfil não encontrado para userId: {user_id}"
            })
        }

    return {
        "statusCode": 200,
        "body": json.dumps(profile, default=str)
    }


def get_profile(user_id):
    """
    Busca o perfil do usuário no DynamoDB pelo userId
    Retorna o perfil ou None se não encontrado
    """

    table = dynamodb.Table(os.environ["PROFILES_TABLE"])

    response = table.get_item(
        Key={"userId": user_id}
    )

    # get_item retorna o item em response["Item"] se encontrado
    # Se não encontrado, a chave "Item" não existe no response
    return response.get("Item")