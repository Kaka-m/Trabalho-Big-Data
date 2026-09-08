import json
import random
import time
import datetime
import os

def gera_log():
    status_codes = [200, 201, 400, 401, 404, 500]
    metodos = ["GET", "POST", "PUT", "DELETE"]
    endpoints = ["/login", "/produtos", "/carrinho", "/checkout", "/usuarios"]


    os.makedirs("/opt/flume/logs", exist_ok=True)


    while True:
        payload = {
            "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "metodo": random.choice(metodos),
            "endpoint": random.choice(endpoints),
            "status": random.choice(status_codes),
            "tempo_resposta_ms": random.randint(10, 1500),
            "ip_origem": f"192.168.1.{random.randint(1, 254)}"
        }

        with open("/opt/flume/logs/app.log", "a") as f:
            f.write(json.dumps(payload) + "\n")


        time.sleep(random.uniform(0.3, 1.5))



if __name__ == "__main__":
    gera_log()            