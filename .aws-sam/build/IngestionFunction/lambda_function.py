import json
import os
import boto3
from datetime import datetime, timezone
from decimal import Decimal

# Clientes AWS — criados fora do handler para reutilizar entre execuções
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

def lambda_handler(event, context):
    """
    Lambda de ingestão de eventos da Mini CDP
    Recebe um evento, salva no S3 e consolida perfil no DynamoDB
    """

    print("Evento recebido:", json.dumps(event))

    # Quando vem do API Gateway, o body chega como string dentro de event["body"]
    # Quando testamos localmente, o payload vem direto no event
    if "body" in event:
        body = json.loads(event["body"])
    else:
        body = event

    # Extrai os dados do evento
    user_id = body.get("userId")
    event_name = body.get("event")
    timestamp = body.get("timestamp")
    properties = body.get("properties", {})

    # Validação básica
    if not user_id or not event_name:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "userId e event são obrigatórios"
            })
        }

    # Monta o payload processado
    now = datetime.now(timezone.utc)
    processed_event = {
        "userId": user_id,
        "event": event_name,
        "timestamp": timestamp or now.isoformat(),
        "properties": properties,
        "status": "received",
        "receivedAt": now.isoformat()
    }

    # 1. Salva evento bruto no S3
    bucket_name = os.environ["S3_BUCKET"]
    s3_key = f"events/{now.year}/{now.month:02d}/{now.day:02d}/{user_id}_{event_name}_{int(now.timestamp())}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(processed_event),
        ContentType="application/json"
    )
    print(f"Evento salvo no S3: s3://{bucket_name}/{s3_key}")

    # 2. Consolida perfil no DynamoDB
    table = dynamodb.Table(os.environ["PROFILES_TABLE"])
    update_profile(table, user_id, event_name, properties, now)

    return {
        "statusCode": 200,
        "body": json.dumps({
            **processed_event,
            "s3Key": s3_key
        })
    }


def update_profile(table, user_id, event_name, properties, now):
    """
    Atualiza ou cria o perfil do usuário no DynamoDB
    Usa UpdateItem para atualizar campos específicos sem sobrescrever o perfil inteiro
    """

    # Valores a incrementar/atualizar
    revenue = Decimal(str(properties.get("value", 0)))
    is_purchase = event_name == "purchase"

    table.update_item(
        Key={"userId": user_id},
        UpdateExpression="""
            SET lastSeen = :lastSeen,
                lastEvent = :lastEvent,
                updatedAt = :updatedAt
            ADD totalEvents :one,
                totalPurchases :isPurchase,
                totalRevenue :revenue
        """,
        ExpressionAttributeValues={
            ":lastSeen": now.isoformat(),
            ":lastEvent": event_name,
            ":updatedAt": now.isoformat(),
            ":one": 1,
            ":isPurchase": 1 if is_purchase else 0,
            ":revenue": revenue
        }
    )

    print(f"Perfil atualizado no DynamoDB: {user_id}")