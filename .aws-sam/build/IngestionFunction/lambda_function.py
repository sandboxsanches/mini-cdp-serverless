import json

def lambda_handler(event, context):
    """
    Lambda de ingestão de eventos da Mini CDP
    Recebe um evento, valida e loga as informações
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
    processed_event = {
        "userId": user_id,
        "event": event_name,
        "timestamp": timestamp,
        "properties": properties,
        "status": "received"
    }
    
    print("Evento processado:", json.dumps(processed_event))
    
    return {
        "statusCode": 200,
        "body": json.dumps(processed_event)
    }