import json
from lambda_function import lambda_handler

# Simulando um evento que chegaria na Lambda
# Parecido com um hit do GA4
event_teste = {
    "userId": "user_123",
    "event": "purchase",
    "timestamp": "2024-01-15T10:30:00Z",
    "properties": {
        "product": "Camiseta Azul",
        "value": 89.90,
        "currency": "BRL"
    }
}

# Chama a Lambda com o evento de teste
resposta = lambda_handler(event_teste, None)

# Exibe o resultado
print("\n--- Resultado ---")
print("Status:", resposta["statusCode"])
print("Body:", json.loads(resposta["body"]))