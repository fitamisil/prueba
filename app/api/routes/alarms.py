from fastapi import APIRouter, HTTPException, Header, Query, Request, status
from pydantic import BaseModel
from typing import Optional, Any
from motor.motor_asyncio import AsyncIOMotorClient
import os
import httpx
import time

router = APIRouter(prefix="", tags=["Alarms"])

# Obtener la conexión a MongoDB y la colección desde el cliente global o reconectando
MONGO_URL = os.getenv("MONGO_URI", "mongodb+srv://pigomezschneider_db_user:4WvulXjWAN5QBEBL@cluster0.rcvykvz.mongodb.net/?appName=Cluster0")
client = AsyncIOMotorClient(MONGO_URL)
database = client.iot_db
alarm_rules_collection = database.get_collection("alarmrules")

# Helper para autenticar token (similar al get_user_email_from_token de tu main.py)
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
        # Si usas el email como ID o si guardas el ID real, ajusta según corresponda. 
        # En tu main.py, el email actúa como identificador del usuario.
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")


# --- MODELOS PYDANTIC ---
class AlarmRuleBody(BaseModel):
    newRule: dict

class AlarmStatusUpdateBody(BaseModel):
    rule: dict


# --- FUNCIONES DE EMQX ---
def get_emqx_auth():
    username = "admin"
    password = os.getenv("EMQX_DEFAULT_APPLICATION_SECRET", "virus123")
    return (username, password)

async def create_alarm_rule_in_emqx(new_alarm: dict) -> Optional[str]:
    try:
        host = os.getenv("EMQX_API_HOST", "localhost")
        # Nota: si usas EMQX v4 como estaba en tu código original usa el puerto 8085, 
        # si migraste a EMQX v5 ajusta la URL y la estructura de reglas según corresponda.
        url = f"http://{host}:8085/api/v4/rules"
        auth = get_emqx_auth()

        topic = f"{new_alarm.get('userId')}/{new_alarm.get('dId')}/{new_alarm.get('variable')}/sdata"
        
        rawsql = (
            f'SELECT username, topic, payload FROM "{topic}" '
            f'WHERE payload.value {new_alarm.get("condition")} {new_alarm.get("value")} '
            'AND is_not_null(payload.value)'
        )

        public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

        new_rule_payload = {
            "rawsql": rawsql,
            "actions": [
                {
                    "name": "data_to_webserver",
                    "params": {
                        # Si global.alarmResource no está definido en Python, asegúrate de tenerlo o cambiarlo por el ID del recurso
                        "$resource": getattr(global_vars, "alarm_resource_id", "default_resource_id"),
                        "payload_tmpl": f'{{"userId":"{new_alarm.get("userId")}","payload":${{payload}},"topic":"${{topic}}"}}'
                    }
                }
            ],
            "description": "ALARM-RULE",
            "enabled": new_alarm.get("status", True)
        }

        async with httpx.AsyncClient(verify=False, timeout=15.0) as httpx_client:
            res = await httpx_client.post(url, json=new_rule_payload, auth=auth)
            
            if res.status_code != 200:
                print(f"[EMQX-ALARM-ERROR] Error creando regla: {res.text}")
                return None

            res_data = res.json()
            emqx_rule_id = res_data.get("data", {}).get("id")

            if emqx_rule_id:
                # Grabar regla en MongoDB
                mongo_rule = {
                    "userId": new_alarm.get("userId"),
                    "dId": new_alarm.get("dId"),
                    "emqxRuleId": emqx_rule_id,
                    "status": new_alarm.get("status"),
                    "variable": new_alarm.get("variable"),
                    "variableFullName": new_alarm.get("variableFullName"),
                    "value": new_alarm.get("value"),
                    "condition": new_alarm.get("condition"),
                    "triggerTime": new_alarm.get("triggerTime"),
                    "createTime": int(time.time() * 1000)
                }
                
                insert_result = await alarm_rules_collection.insert_one(mongo_rule)

                # Actualizar la plantilla de payload con más detalles en EMQX
                update_url = f"http://{host}:8085/api/v4/rules/{emqx_rule_id}"
                
                payload_templ = (
                    f'{{"userId":"{new_alarm.get("userId")}","dId":"{new_alarm.get("dId")}",'
                    f'"deviceName":"{new_alarm.get("deviceName")}","payload":${{payload}},'
                    f'"topic":"${{topic}}","emqxRuleId":"{emqx_rule_id}","value":{new_alarm.get("value")},'
                    f'"condition":"{new_alarm.get("condition")}","variable":"{new_alarm.get("variable")}",'
                    f'"variableFullName":"{new_alarm.get("variableFullName")}","triggerTime":{new_alarm.get("triggerTime")}}}'
                )

                new_rule_payload["actions"][0]["params"]["payload_tmpl"] = payload_templ
                await httpx_client.put(update_url, json=new_rule_payload, auth=auth)

                print("New Alarm Rule Created...")
                return emqx_rule_id

    except Exception as error:
        print("[ALARM-CREATE-ERROR]", error)
        return None

async def update_alarm_rule_status_in_emqx(emqx_rule_id: str, status_val: bool) -> bool:
    try:
        host = os.getenv("EMQX_API_HOST", "localhost")
        url = f"http://{host}:8085/api/v4/rules/{emqx_rule_id}"
        auth = get_emqx_auth()

        update_payload = {"enabled": status_val}

        async with httpx.AsyncClient(verify=False, timeout=15.0) as httpx_client:
            res = await httpx_client.put(url, json=update_payload, auth=auth)
            if res.status_code == 200 and res.json().get("data"):
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
        host = os.getenv("EMQX_API_HOST", "localhost")
        url = f"http://{host}:8085/api/v4/rules/{emqx_rule_id}"
        auth = get_emqx_auth()

        async with httpx.AsyncClient(verify=False, timeout=15.0) as httpx_client:
            await httpx_client.delete(url, auth=auth)
            await alarm_rules_collection.delete_one({"emqxRuleId": emqx_rule_id})
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
            raise HTTPException(status_code=500, status="error")
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
