from fastapi import APIRouter, HTTPException, Header, Query, Request, status
from pydantic import BaseModel
from typing import Optional, Any
from motor.motor_asyncio import AsyncIOMotorClient
import os
import httpx
import time
import re

router = APIRouter(prefix="", tags=["Alarms"])

# Obtener la conexión a MongoDB y la colección
MONGO_URL = os.getenv("MONGO_URI", "mongodb+srv://pigomezschneider_db_user:4WvulXjWAN5QBEBL@cluster0.rcvykvz.mongodb.net/?appName=Cluster0")
client = AsyncIOMotorClient(MONGO_URL)
database = client.iot_db
alarm_rules_collection = database.get_collection("alarmrules")

# Helper para autenticar token JWT
from jose import jwt, JWTError

SECRET_KEY = "clave-secreta-super-segura-iot-gl"
ALGORITHM = "HS256"

def get_user_id_from_token(token: str = Header(None)) -> str:
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token no proporcionado")
    try:
        if token.startswith("Bearer "):
            token = token.split(" ")[1]
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Token inválido")
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")


# --- MODELOS PYDANTIC ---
class AlarmRuleBody(BaseModel):
    newRule: dict

class AlarmStatusUpdateBody(BaseModel):
    rule: dict


# --- FUNCIONES DE EMQX v5 ---

async def get_emqx_v5_token():
    host = os.getenv("EMQX_API_HOST")
    if not host:
        print("[EMQX-LOGIN-ERROR] EMQX_API_HOST no está definido en las variables de entorno.")
        return None, None
        
    host = host.rstrip("/")
    base_url = f"{host}/api/v5" if not host.endswith("/api/v5") else host
    login_url = f"{base_url}/login"
    
    username = os.getenv("EMQX_DASHBOARD_USER", "admin")
    password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")
    
    try:
        async with httpx.AsyncClient(verify=False, timeout=15.0) as client:
            res = await client.post(login_url, json={"username": username, "password": password})
            if res.status_code == 200:
                token = res.json().get("token") or res.json().get("access_token")
                return base_url, token
            else:
                print(f"[EMQX-LOGIN-ERROR] Status {res.status_code} Resp: {res.text}")
    except Exception as e:
        print(f"[EMQX-LOGIN-EXCEPTION] {e}")
    return None, None

# async def get_emqx_v5_token():
#     host = os.getenv("EMQX_API_HOST")
#     if not host:
#         print("[EMQX-LOGIN-ERROR] EMQX_API_HOST no está definido en las variables de entorno.")
#         return None, None
        
#     host = host.rstrip("/")
#     base_url = f"{host}/api/v5" if not host.endswith("/api/v5") else host
#     login_url = f"{base_url}/login"
    
#     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
#     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")
    
#     try:
#         async with httpx.AsyncClient(verify=False, timeout=15.0) as client:
#             # Correcto para EMQX v5: Usar Basic Auth
#             res = await client.post(login_url, auth=(username, password))
#             if res.status_code == 200:
#                 token = res.json().get("token") or res.json().get("access_token")
#                 return base_url, token
#             else:
#                 print(f"[EMQX-LOGIN-ERROR] Falló el login en alarms.py: Status {res.status_code} Resp: {res.text}")
#     except Exception as e:
#         print(f"[EMQX-LOGIN-EXCEPTION] {e}")
#     return None, None


async def create_alarm_rule_in_emqx(new_alarm: dict) -> Optional[str]:
    try:
        base_url, token = await get_emqx_v5_token()
        if not token:
            print("[EMQX-ALARM-ERROR] No se pudo autenticar con la API de EMQX v5")
            return None

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
            
            public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")
            connector_id = "http_conn_alarm"
            bridge_id = "alarm_webhook_bridge"
            
            # 1. Crear el Conector HTTP primero
            connector_payload = {
                "name": connector_id,
                "type": "http",
                "config": {
                    "url": public_api_url,
                    "connect_timeout": "15s",
                    "pool_size": 8
                }
            }
            conn_res = await httpx_client.post(f"{base_url}/connectors", json=connector_payload, headers=headers)
            if conn_res.status_code not in [200, 201]:
                await httpx_client.put(f"{base_url}/connectors/{connector_id}", json=connector_payload, headers=headers)

            # 2. Configurar el Bridge HTTP enlazado al conector
            bridge_payload = {
                "name": bridge_id,
                "type": "http",
                "connector": connector_id,
                "config": {
                    "url": f"{public_api_url}/alarm-webhook",
                    "method": "post",
                    "headers": {"token": os.getenv("EMQX_API_TOKEN", "tu_token_secreto_emqx")},
                    "body": "${.}",
                    "connect_timeout": "15s",
                    "pool_size": 8
                }
            }
            
            bridge_res = await httpx_client.post(f"{base_url}/bridges", json=bridge_payload, headers=headers)
            if bridge_res.status_code not in [200, 201]:
                bridge_res = await httpx_client.put(f"{base_url}/bridges/http:{bridge_id}", json=bridge_payload, headers=headers)
            
            print(f"🌉 [EMQX-ALARM-BRIDGE] Status: {bridge_res.status_code} Resp: {bridge_res.text}")

            # 3. Construir la regla SQL de EMQX v5
            topic = f"{new_alarm.get('userId')}/{new_alarm.get('dId')}/{new_alarm.get('variable')}/sdata"
            safe_var = re.sub(r'[^a-zA-Z0-9_]', '_', str(new_alarm.get('variable', 'var')))
            rule_id = f"alarm_rule_{int(time.time())}_{safe_var}"
            
            sql = (
                f'SELECT * FROM "{topic}" '
                f'WHERE payload.value {new_alarm.get("condition")} {new_alarm.get("value")} '
                'AND is_not_null(payload.value)'
            )
            
            rule_payload = {
                "id": rule_id,
                "name": rule_id,
                "sql": sql,
                "actions": [f"http:{bridge_id}"],
                "description": f"Alarm rule for {new_alarm.get('variableFullName', '')}",
                "enable": new_alarm.get("status", True)
            }

            rule_res = await httpx_client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
            if rule_res.status_code not in [200, 201]:
                rule_res = await httpx_client.put(f"{base_url}/rules/{rule_id}", json=rule_payload, headers=headers)

            print(f"⚙️ [EMQX-ALARM-RULE] Status: {rule_res.status_code} Resp: {rule_res.text}")

            if rule_res.status_code in [200, 201]:
                mongo_rule = {
                    "userId": new_alarm.get("userId"),
                    "dId": new_alarm.get("dId"),
                    "emqxRuleId": rule_id,
                    "status": new_alarm.get("status", True),
                    "variable": new_alarm.get("variable"),
                    "variableFullName": new_alarm.get("variableFullName"),
                    "value": new_alarm.get("value"),
                    "condition": new_alarm.get("condition"),
                    "triggerTime": new_alarm.get("triggerTime"),
                    "createTime": int(time.time() * 1000)
                }
                
                await alarm_rules_collection.insert_one(mongo_rule)
                return rule_id
            else:
                return None

    except Exception as error:
        print("[ALARM-CREATE-ERROR]", error)
        return None


async def update_alarm_rule_status_in_emqx(emqx_rule_id: str, status_val: bool) -> bool:
    try:
        base_url, token = await get_emqx_v5_token()
        if not token:
            return False
        
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        update_payload = {"enabled": status_val}

        async with httpx.AsyncClient(verify=False, timeout=15.0) as httpx_client:
            res = await httpx_client.put(f"{base_url}/rules/{emqx_rule_id}", json=update_payload, headers=headers)
            if res.status_code == 200:
                await alarm_rules_collection.update_one(
                    {"emqxRuleId": emqx_rule_id}, 
                    {"$set": {"status": status_val}}
                )
                print("Alarm Rule Status Updated...")
                return True
        return False
    except Exception as error:
        print("[ALARM-UPDATE-ERROR]", error)
        return False


async def delete_alarm_rule_in_emqx(emqx_rule_id: str) -> bool:
    try:
        base_url, token = await get_emqx_v5_token()
        if not token:
            return False
        
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(verify=False, timeout=15.0) as httpx_client:
            await httpx_client.delete(f"{base_url}/rules/{emqx_rule_id}", headers=headers)
            await alarm_rules_collection.delete_one({"emqxRuleId": emqx_rule_id})
            print("Alarm Rule Deleted...")
            return True
    except Exception as error:
        print("[ALARM-DELETE-ERROR]", error)
        return False


# --- ENDPOINTS ---

@router.post("/alarm-rule")
async def create_alarm_rule_endpoint(body: AlarmRuleBody, token: str = Header(None)):
    try:
        user_id = get_user_id_from_token(token)
        new_rule = body.newRule
        new_rule["userId"] = user_id

        r = await create_alarm_rule_in_emqx(new_rule)

        if r:
            return {"status": "success"}
        else:
            raise HTTPException(status_code=500, detail="error creating rule")
    except Exception as error:
        print(error)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.put("/alarm-rule")
async def update_alarm_rule_endpoint(body: AlarmStatusUpdateBody, token: str = Header(None)):
    try:
        get_user_id_from_token(token)
        rule = body.rule

        r = await update_alarm_rule_status_in_emqx(rule.get("emqxRuleId"), rule.get("status"))

        if r:
            return {"status": "success"}
        else:
            return {"status": "error"}
    except Exception as error:
        print(error)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.delete("/alarm-rule")
async def delete_alarm_rule_endpoint(emqxRuleId: str = Query(...), token: str = Header(None)):
    try:
        get_user_id_from_token(token)
        r = await delete_alarm_rule_in_emqx(emqxRuleId)

        if r:
            return {"status": "success"}
        else:
            return {"status": "error"}
    except Exception as error:
        print(error)
        raise HTTPException(status_code=500, detail="Internal server error")