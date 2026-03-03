import json

def lambda_handler(event, context):
    """
    Lambda de ingestão de eventos da Mini CDP
    Recebe um evento, valida e loga as informações
    """
    
    print("Evento recebido:", json.dumps(event))
    
    # Extrai os dados do evento
    user_id = event.get("userId")
    event_name = event.get("event")
    timestamp = event.get("timestamp")
    properties = event.get("properties", {})
    
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