from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException, Header, status, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional, Dict, Any
from bson import ObjectId
import bcrypt
from jose import jwt, JWTError
import time
import random
import os
import asyncio
import httpx
import json

app = FastAPI(title="IoT GL API", version="1.0.0")

SECRET_KEY = "clave-secreta-super-segura-iot-gl"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60
EMQX_API_TOKEN = "tu_token_secreto_emqx"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URL = os.getenv("MONGO_URI", "mongodb+srv://pigomezschneider_db_user:4WvulXjWAN5QBEBL@cluster0.rcvykvz.mongodb.net/?appName=Cluster0")

client = AsyncIOMotorClient(MONGO_URL)
database = client.iot_db

users_collection = database.get_collection("users")
devices_collection = database.get_collection("devices")
templates_collection = database.get_collection("templates")
data_collection = database.get_collection("data")
emqx_auth_rules_collection = database.get_collection("emqxauthrules")
notifications_collection = database.get_collection("notifications")
alarm_rules_collection = database.get_collection("alarmrules")
saver_rules_collection = database.get_collection("saverrules")

string_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

def makeid(length: int = 10) -> str:
    return "".join(random.choice(string_chars) for _ in range(length))

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_user_email_from_token(token: str = Header(None)):
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token no proporcionado")
    try:
        # Limpiar prefijo 'Bearer ' si el frontend lo manda así
        if token.startswith("Bearer "):
            token = token.split(" ")[1]
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise HTTPException(status_code=401, detail="Token inválido")
        return email
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")

async def create_emqx_v5_webhook_rule():
    host = os.getenv("EMQX_API_HOST", "https://egg-des-prospect-damaged.trycloudflare.com")
    
    if host.startswith("http://") or host.startswith("https://"):
        base_url = f"{host}/api/v5"
    else:
        base_url = f"http://{host}/api/v5"
        
    username = os.getenv("EMQX_DASHBOARD_USER", "admin")
    password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

    print(f"[EMQX-INIT] Conectando a la API de EMQX en: {base_url}...")
    try:
        async with httpx.AsyncClient(verify=False, timeout=15.0) as httpx_client:
            try:
                login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
            except Exception as e:
                print(f"[EMQX-WARN] No se pudo contactar a EMQX: {e}")
                return

            if login_res.status_code != 200:
                print(f"[EMQX-ERROR] Login fallido en EMQX ({login_res.status_code}): {login_res.text}")
                return
            
            token = login_res.json().get("token") or login_res.json().get("access_token")
            if not token:
                print(f"[EMQX-ERROR] No se pudo extraer el token de acceso.")
                return

            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

            connector_payload = {
                "name": "saver_webhook_bridge",
                "type": "http",
                "url": f"{public_api_url}/saver-webhook",
                "method": "post",
                "headers": {"token": EMQX_API_TOKEN},
                "body": "${.}",
                "connect_timeout": "15s",
                "pool_size": 8
            }

            conn_res = await httpx_client.post(f"{base_url}/bridges", json=connector_payload, headers=headers)
            if conn_res.status_code not in [200, 201]:
                conn_res = await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=connector_payload, headers=headers)
            
            print(f"[EMQX-CONNECTOR] Status: {conn_res.status_code} Resp: {conn_res.text}")
            await asyncio.sleep(1)

            rule_payload = {
                "id": "saver_webhook_rule", 
                "sql": "SELECT * FROM \"+/+/+/sdata\"", 
                "actions": ["http:saver_webhook_bridge"]
            }

            rule_res = await httpx_client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
            if rule_res.status_code not in [200, 201]:
                rule_res = await httpx_client.put(f"{base_url}/rules/saver_webhook_rule", json=rule_payload, headers=headers)

            print(f"[EMQX-RULE] Status: {rule_res.status_code} Resp: {rule_res.text}")
            
    except Exception as error:
        print("[EMQX-EXCEPTION] Error general crítico al configurar EMQX:", error)

async def check_mqtt_superuser(db):
    try:
        collection = db["emqxauthrules"]
        superuser = await collection.find_one({"type": "superuser"})
        if superuser:
            return
        current_time = int(time.time() * 1000)
        await collection.insert_one({
            "publish": ["#"], "subscribe": ["#"], "userId": "emqxmqttsuperuser",
            "username": os.getenv("EMQX_NODE_SUPERUSER_USER", "admin"),
            "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "public"),
            "type": "superuser", "time": current_time, "updatedTime": current_time
        })
        print("[DB] Superusuario MQTT creado en MongoDB.")
    except Exception as error:
        print("[DB-ERROR] Error creando superusuario MQTT:", error)

async def init_emqx_background_tasks(db):
    try:
        await asyncio.sleep(3)
        print("--------------------------------------------------")
        print("CHECKING MQTT SUPERUSER & EMQX RULES.........")
        await check_mqtt_superuser(db)
        await create_emqx_v5_webhook_rule()
        print("--------------------------------------------------")
    except Exception as e:
        print(f"❌ [CRITICAL BACKGROUND ERROR] Excepción atrapada en segundo plano: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(init_emqx_background_tasks(database))

async def get_device_mqtt_credentials(dId: str, userId: str):
    try:
        rule = await emqx_auth_rules_collection.find_one({"type": "device", "userId": userId, "dId": dId})
        new_username, new_password, current_time = makeid(10), makeid(10), int(time.time() * 1000)

        if not rule:
            await emqx_auth_rules_collection.insert_one({
                "userId": userId, "dId": dId, "username": new_username, "password": new_password,
                "publish": [f"{userId}/{dId}/+/sdata"], "subscribe": [f"{userId}/{dId}/+/actdata"],
                "type": "device", "time": current_time, "updatedTime": current_time
            })
            return {"username": new_username, "password": new_password}

        await emqx_auth_rules_collection.update_one(
            {"type": "device", "dId": dId},
            {"$set": {"username": new_username, "password": new_password, "updatedTime": current_time}}
        )
        return {"username": new_username, "password": new_password}
    except Exception:
        return False

# --- MODELOS ---
class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserCreate(BaseModel):
    email: EmailStr
    password: str

class TemplateBody(BaseModel):
    template: dict

class DeviceCredentialsBody(BaseModel):
    dId: str
    password: str

class NotificationUpdateBody(BaseModel):
    notifId: str

# --- ENDPOINTS ---
@app.get("/")
async def read_root():
    return {"message": "API OK"}

@app.post("/login")
@app.post("/api/login")
async def login_user(user_data: UserLogin):
    user = await users_collection.find_one({"email": user_data.email})
    if not user or not bcrypt.checkpw(user_data.password.encode('utf-8'), user.get("password", "").encode('utf-8')):
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")
    access_token = create_access_token(data={"sub": user["email"]}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    return {"status": "success", "access_token": access_token, "token_type": "bearer", "userData": {"_id": str(user.get("_id", "")), "name": user.get("email").split("@")[0], "email": user["email"]}}

@app.post("/register")
@app.post("/api/register")
async def register_user(user_data: UserCreate):
    if await users_collection.find_one({"email": user_data.email}):
        raise HTTPException(status_code=400, detail="Correo registrado")
    hashed = bcrypt.hashpw(user_data.password.encode('utf-8'), bcrypt.gensalt())
    await users_collection.insert_one({"email": user_data.email, "password": hashed.decode('utf-8')})
    return {"status": "success"}

@app.get("/device")
@app.get("/api/device")
async def get_devices(token: str = Header(None)):
    user_email = get_user_email_from_token(token)
    devices = []
    async for dev in devices_collection.find({"userId": user_email}):
        dev["_id"] = str(dev["_id"])
        devices.append(dev)
    saver_rules = await saver_rules_collection.find({"userId": user_email}).to_list(length=None)
    templates = await templates_collection.find({"userId": user_email}).to_list(length=None)
    alarm_rules = await alarm_rules_collection.find({"userId": user_email}).to_list(length=None)

    for device in devices:
        sr = next((s for s in saver_rules if s.get("dId") == device.get("dId")), None)
        device["saverRule"] = {"_id": str(sr["_id"]), "status": sr.get("status", True)} if sr else {"status": True}
        t = next((tp for tp in templates if str(tp.get("_id")) == str(device.get("templateId"))), None)
        if t:
            t["_id"] = str(t["_id"])
            device["template"] = t
        device["alarmRules"] = [ar for ar in alarm_rules if ar.get("dId") == device.get("dId")]

    return {"status": "success", "data": devices}

@app.post("/device")
@app.post("/api/device")
async def add_device(request_body: dict, token: str = Header(None)):
    user_email = get_user_email_from_token(token)
    new_device = request_body.get("newDevice", {})
    dId = new_device.get("dId")
    await devices_collection.update_many({"userId": user_email}, {"$set": {"selected": False}})
    new_device["userId"] = user_email
    new_device["createdTime"] = int(time.time() * 1000)
    new_device["password"] = makeid(10)
    new_device["selected"] = True
    await saver_rules_collection.insert_one({"userId": user_email, "dId": dId, "status": True})
    await devices_collection.insert_one(new_device)
    return {"status": "success"}

@app.delete("/device")
@app.delete("/api/device")
async def delete_device(dId: str = Query(...), token: str = Header(None)):
    user_email = get_user_email_from_token(token)
    await saver_rules_collection.delete_many({"dId": dId})
    await emqx_auth_rules_collection.delete_many({"dId": dId, "type": "device"})
    result = await devices_collection.delete_one({"userId": user_email, "dId": dId})
    return {"status": "success", "data": result.deleted_count}

@app.put("/device")
@app.put("/api/device")
async def update_device_selector(body: dict, token: str = Header(None)):
    user_email = get_user_email_from_token(token)
    dId = body.get("dId")
    await devices_collection.update_many({"userId": user_email}, {"$set": {"selected": False}})
    await devices_collection.update_one({"userId": user_email, "dId": dId}, {"$set": {"selected": True}})
    return {"status": "success"}

@app.get("/template")
@app.get("/api/template")
async def get_templates(token: str = Header(None)):
    user_email = get_user_email_from_token(token)
    templates = []
    async for template in templates_collection.find({"userId": user_email}):
        template["_id"] = str(template["_id"])
        templates.append(template)
    return {"status": "success", "data": templates}

@app.post("/template")
@app.post("/api/template")
async def create_template(body: TemplateBody, token: str = Header(None)):
    user_email = get_user_email_from_token(token)
    new_template = body.template
    new_template["userId"] = user_email
    result = await templates_collection.insert_one(new_template)
    return {"status": "success", "id": str(result.inserted_id)}

@app.delete("/template")
@app.delete("/api/template")
async def delete_template(templateId: str = Query(...), token: str = Header(None)):
    user_email = get_user_email_from_token(token)
    await templates_collection.delete_one({"userId": user_email, "_id": ObjectId(templateId)})
    return {"status": "success"}

@app.get("/get-small-charts-data")
@app.get("/api/get-small-charts-data")
async def get_small_charts_data(
    chartTimeAgo: int = Query(...),
    dId: str = Query(...),
    variable: str = Query(...),
    token: str = Header(None)
):
    user_email = get_user_email_from_token(token)
    try:
        time_ago_ms = int(time.time() * 1000) - (max(chartTimeAgo, 1440) * 60 * 1000)

        cursor = data_collection.find({
            "dId": dId,
            "variable": variable,
            "time": {"$gt": time_ago_ms}
        }).sort("time", 1)

        data = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            data.append(doc)

        return data
    except Exception as error:
        print("[DB-ERROR] Error consultando gráficos:", error)
        return []


@app.get("/get-small-charts-data")
@app.get("/api/get-small-charts-data")
async def get_small_charts_data(
    chartTimeAgo: int = Query(...),
    dId: str = Query(...),
    variable: str = Query(...),
    token: str = Header(None)
):
    user_email = get_user_email_from_token(token)
    try:
        time_ago_ms = int(time.time() * 1000) - (max(chartTimeAgo, 1440) * 60 * 1000)

        cursor = data_collection.find({
            "dId": dId,
            "variable": variable,
            "time": {"$gt": time_ago_ms}
        }).sort("time", 1)

        data = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            data.append(doc)

        # Devolvemos la lista directamente porque el componente Rtnumberchart.vue espera un array puro
        return data
    except Exception as error:
        print("[DB-ERROR] Error consultando gráficos:", error)
        return []

# @app.post("/getdevicecredentials")
# @app.post("/api/getdevicecredentials")
# async def get_device_credentials(body: DeviceCredentialsBody):
#     device = await devices_collection.find_one({"dId": body.dId})
#     if not device:
#         raise HTTPException(status_code=401)
#     userId = device.get("userId")
#     credentials = await get_device_mqtt_credentials(body.dId, userId)
#     template = await templates_collection.find_one({"_id": ObjectId(device["templateId"])}) if "templateId" in device else None
    
#     variables = []
#     if template and "widgets" in template:
#         for widget in template["widgets"]:
#             variables.append({
#                 "variable": widget.get("variable"),
#                 "variableFullName": widget.get("variableFullName"),
#                 "variableType": widget.get("variableType"),
#                 "variableSendFreq": widget.get("variableSendFreq")
#             })

#     return {
#         "username": credentials["username"],
#         "password": credentials["password"],
#         "topic": f"{userId}/{body.dId}/",
#         "variables": variables
#     }

# SAVER WEBHOOK CON FILTRO DE 0.05% DE VARIACIÓN Y PARSEO ROBUSTO
@app.post("/saver-webhook")
@app.post("/api/saver-webhook")
async def saver_webhook(request: Request, token: Optional[str] = Header(None)):
    if token != EMQX_API_TOKEN:
        raise HTTPException(status_code=404)

    try:
        request_data = await request.json()
    except Exception as e:
        return {"status": "error", "message": "Invalid JSON"}

    topic = request_data.get("topic", "")
    data_user_id = request_data.get("userId")
    splitted_topic = topic.split("/")
    
    if len(splitted_topic) >= 3:
        user_id = splitted_topic[0] if not data_user_id else data_user_id
        dId = splitted_topic[1]
        variable = splitted_topic[2]
        
        device_found = await devices_collection.find_one({"dId": dId})
        
        if device_found:
            real_user_id = device_found.get("userId", user_id)
            payload_val = request_data.get("payload", {})
            
            # Parseo ultra robusto de payload (diccionario, string JSON o número)
            current_value = 0.0
            try:
                if isinstance(payload_val, dict):
                    current_value = float(payload_val.get("value", 0))
                elif isinstance(payload_val, str):
                    parsed_payload = json.loads(payload_val)
                    current_value = float(parsed_payload.get("value", 0))
                else:
                    current_value = float(payload_val)
            except Exception as e:
                print(f"❌ [PAYLOAD ERROR] No se pudo convertir el valor: {payload_val} -> {e}")
                return {"status": "error", "message": "Invalid payload value"}

            # 1. Aplicar filtro de variación del 0.05%
            last_record = await data_collection.find_one(
                {"dId": dId, "variable": variable},
                sort=[("time", -1)]
            )

            if last_record:
                last_value = float(last_record.get("value", 0))
                percent_change = 0.0
                if last_value != 0:
                    percent_change = abs((current_value - last_value) / last_value) * 100
                else:
                    percent_change = 100.0 if current_value != 0 else 0.0

                if percent_change < 0.05:
                    return {"status": "filtered", "message": "Variation below 0.05%"}

            # 2. Guardar en MongoDB si pasa el umbral o es el primer registro
            timestamp = int(time.time() * 1000)
            await data_collection.insert_one({
                "userId": real_user_id,
                "dId": dId,
                "variable": variable,
                "value": current_value,
                "time": timestamp,
                "path": f"/{real_user_id}/{dId}/{timestamp}/"
            })
            print(f"✅ [DB SUCCESS] ¡DATO GUARDADO! -> dId: {dId} | Var: {variable} | Valor: {current_value}")
            
    return {"status": "success"}

@app.post("/alarm-webhook")
@app.post("/api/alarm-webhook")
async def alarm_webhook(incoming_alarm: dict, token: Optional[str] = Header(None)):
    return {"status": "success"}

@app.get("/notifications")
@app.get("/api/notifications")
async def get_notifications(token: str = Header(None)):
    return {"status": "success", "data": []}

@app.put("/notifications")
@app.put("/api/notifications")
async def update_notification(body: NotificationUpdateBody, token: str = Header(None)):
    return {"status": "success"}

@app.post("/getmqttcredentials")
@app.post("/api/getmqttcredentials")
async def get_mqtt_credentials(token: str = Header(None)):
    return {"status": "success", "username": "admin", "password": "public"}

# from datetime import datetime, timedelta, timezone
# from fastapi import FastAPI, HTTPException, Header, status, Query
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel, EmailStr
# from motor.motor_asyncio import AsyncIOMotorClient
# from typing import Optional
# from bson import ObjectId
# import bcrypt
# from jose import jwt, JWTError
# import time
# import random
# import os
# import asyncio
# import httpx

# app = FastAPI(title="IoT GL API", version="1.0.0")

# SECRET_KEY = "clave-secreta-super-segura-iot-gl"
# ALGORITHM = "HS256"
# ACCESS_TOKEN_EXPIRE_MINUTES = 60
# EMQX_API_TOKEN = "tu_token_secreto_emqx"

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# MONGO_URL = os.getenv("MONGO_URI", "mongodb+srv://pigomezschneider_db_user:4WvulXjWAN5QBEBL@cluster0.rcvykvz.mongodb.net/?appName=Cluster0")

# client = AsyncIOMotorClient(MONGO_URL)
# database = client.iot_db

# users_collection = database.get_collection("users")
# devices_collection = database.get_collection("devices")
# templates_collection = database.get_collection("templates")
# data_collection = database.get_collection("data")
# emqx_auth_rules_collection = database.get_collection("emqxauthrules")
# notifications_collection = database.get_collection("notifications")
# alarm_rules_collection = database.get_collection("alarmrules")
# saver_rules_collection = database.get_collection("saverrules")

# string_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

# def makeid(length: int = 10) -> str:
#     return "".join(random.choice(string_chars) for _ in range(length))

# def create_access_token(data: dict, expires_delta: timedelta | None = None):
#     to_encode = data.copy()
#     expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=15))
#     to_encode.update({"exp": expire})
#     return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# def get_user_email_from_token(token: str = Header(None)):
#     if not token:
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token no proporcionado")
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         email: str = payload.get("sub")
#         if email is None:
#             raise HTTPException(status_code=401, detail="Token inválido")
#         return email
#     except JWTError:
#         raise HTTPException(status_code=401, detail="Token inválido o expirado")

# async def create_emqx_v5_webhook_rule():
#     host = os.getenv("EMQX_API_HOST", "https://egg-des-prospect-damaged.trycloudflare.com")
    
#     if host.startswith("http://") or host.startswith("https://"):
#         base_url = f"{host}/api/v5"
#     else:
#         base_url = f"http://{host}/api/v5"
        
#     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
#     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

#     print(f"[EMQX-INIT] Conectando a la API de EMQX en: {base_url}...")
#     try:
#         async with httpx.AsyncClient(verify=False, timeout=15.0) as client:
#             # 1. Login para obtener el token Bearer
#             try:
#                 login_res = await client.post(f"{base_url}/login", json={"username": username, "password": password})
#             except Exception as e:
#                 print(f"[EMQX-WARN] No se pudo contactar a EMQX: {e}")
#                 return

#             if login_res.status_code != 200:
#                 print(f"[EMQX-ERROR] Login fallido en EMQX ({login_res.status_code}): {login_res.text}")
#                 return
            
#             token = login_res.json().get("token") or login_res.json().get("access_token")
#             if not token:
#                 print(f"[EMQX-ERROR] No se pudo extraer el token de acceso.")
#                 return

#             headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#             public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

#             # 2. Conector HTTP exacto apuntando al endpoint /saver-webhook
#             connector_payload = {
#                 "name": "saver_webhook_bridge",
#                 "type": "http",
#                 "url": f"{public_api_url}/saver-webhook",
#                 "method": "post",
#                 "headers": {"token": EMQX_API_TOKEN},
#                 "body": "${.}",
#                 "connect_timeout": "15s",
#                 "pool_size": 8
#             }

#             # Creamos o actualizamos el conector en EMQX v5
#             conn_res = await client.post(f"{base_url}/bridges", json=connector_payload, headers=headers)
#             if conn_res.status_code not in [200, 201]:
#                 conn_res = await client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=connector_payload, headers=headers)
            
#             print(f"[EMQX-CONNECTOR] Status: {conn_res.status_code} Resp: {conn_res.text}")
#             await asyncio.sleep(1)

#             # 3. Creamos o actualizamos la Regla SQL asociada
#             rule_payload = {
#                 "id": "saver_webhook_rule", 
#                 "sql": "SELECT * FROM \"+/+/+/sdata\"", 
#                 "actions": ["http:saver_webhook_bridge"]
#             }

#             rule_res = await client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
#             if rule_res.status_code not in [200, 201]:
#                 rule_res = await client.put(f"{base_url}/rules/saver_webhook_rule", json=rule_payload, headers=headers)

#             print(f"[EMQX-RULE] Status: {rule_res.status_code} Resp: {rule_res.text}")
            
#             if rule_res.status_code in [200, 201] and conn_res.status_code in [200, 201]:
#                 print("[EMQX-SUCCESS] ¡Conector HTTP y regla configurados y sincronizados por código con éxito!")
#             else:
#                 print(f"[EMQX-WARN] Estado - Conector: {conn_res.status_code}, Regla: {rule_res.status_code}")
                
#     except Exception as error:
#         print("[EMQX-EXCEPTION] Error general crítico al configurar EMQX:", error)

# async def check_mqtt_superuser(db):
#     try:
#         collection = db["emqxauthrules"]
#         superuser = await collection.find_one({"type": "superuser"})
#         if superuser:
#             return
#         current_time = int(time.time() * 1000)
#         await collection.insert_one({
#             "publish": ["#"], "subscribe": ["#"], "userId": "emqxmqttsuperuser",
#             "username": os.getenv("EMQX_NODE_SUPERUSER_USER", "admin"),
#             "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "public"),
#             "type": "superuser", "time": current_time, "updatedTime": current_time
#         })
#         print("[DB] Superusuario MQTT creado en MongoDB.")
#     except Exception as error:
#         print("[DB-ERROR] Error creando superusuario MQTT:", error)

# async def init_emqx_background_tasks(db):
#     try:
#         await asyncio.sleep(3)
#         print("--------------------------------------------------")
#         print("CHECKING MQTT SUPERUSER & EMQX RULES.........")
#         await check_mqtt_superuser(db)
#         await create_emqx_v5_webhook_rule()
#         print("--------------------------------------------------")
#     except Exception as e:
#         print(f"❌ [CRITICAL BACKGROUND ERROR] Excepción atrapada en segundo plano: {e}")

# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(init_emqx_background_tasks(database))

# async def get_device_mqtt_credentials(dId: str, userId: str):
#     try:
#         rule = await emqx_auth_rules_collection.find_one({"type": "device", "userId": userId, "dId": dId})
#         new_username, new_password, current_time = makeid(10), makeid(10), int(time.time() * 1000)

#         if not rule:
#             await emqx_auth_rules_collection.insert_one({
#                 "userId": userId, "dId": dId, "username": new_username, "password": new_password,
#                 "publish": [f"{userId}/{dId}/+/sdata"], "subscribe": [f"{userId}/{dId}/+/actdata"],
#                 "type": "device", "time": current_time, "updatedTime": current_time
#             })
#             return {"username": new_username, "password": new_password}

#         await emqx_auth_rules_collection.update_one(
#             {"type": "device", "dId": dId},
#             {"$set": {"username": new_username, "password": new_password, "updatedTime": current_time}}
#         )
#         return {"username": new_username, "password": new_password}
#     except Exception:
#         return False

# # --- MODELOS ---
# class UserLogin(BaseModel):
#     email: EmailStr
#     password: str

# class UserCreate(BaseModel):
#     email: EmailStr
#     password: str

# class TemplateBody(BaseModel):
#     template: dict

# class DeviceCredentialsBody(BaseModel):
#     dId: str
#     password: str

# class NotificationUpdateBody(BaseModel):
#     notifId: str

# # --- ENDPOINTS ---
# @app.get("/")
# async def read_root():
#     return {"message": "API OK"}

# @app.post("/login")
# @app.post("/api/login")
# async def login_user(user_data: UserLogin):
#     user = await users_collection.find_one({"email": user_data.email})
#     if not user or not bcrypt.checkpw(user_data.password.encode('utf-8'), user.get("password", "").encode('utf-8')):
#         raise HTTPException(status_code=401, detail="Credenciales incorrectas")
#     access_token = create_access_token(data={"sub": user["email"]}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
#     return {"status": "success", "access_token": access_token, "token_type": "bearer", "userData": {"_id": str(user.get("_id", "")), "name": user.get("email").split("@")[0], "email": user["email"]}}

# @app.post("/register")
# @app.post("/api/register")
# async def register_user(user_data: UserCreate):
#     if await users_collection.find_one({"email": user_data.email}):
#         raise HTTPException(status_code=400, detail="Correo registrado")
#     hashed = bcrypt.hashpw(user_data.password.encode('utf-8'), bcrypt.gensalt())
#     await users_collection.insert_one({"email": user_data.email, "password": hashed.decode('utf-8')})
#     return {"status": "success"}

# @app.get("/device")
# @app.get("/api/device")
# async def get_devices(token: str = Header(None)):
#     user_email = get_user_email_from_token(token)
#     devices = []
#     async for dev in devices_collection.find({"userId": user_email}):
#         dev["_id"] = str(dev["_id"])
#         devices.append(dev)
#     saver_rules = await saver_rules_collection.find({"userId": user_email}).to_list(length=None)
#     templates = await templates_collection.find({"userId": user_email}).to_list(length=None)
#     alarm_rules = await alarm_rules_collection.find({"userId": user_email}).to_list(length=None)

#     for device in devices:
#         sr = next((s for s in saver_rules if s.get("dId") == device.get("dId")), None)
#         device["saverRule"] = {"_id": str(sr["_id"]), "status": sr.get("status", True)} if sr else {"status": True}
#         t = next((tp for tp in templates if str(tp.get("_id")) == str(device.get("templateId"))), None)
#         if t:
#             t["_id"] = str(t["_id"])
#             device["template"] = t
#         device["alarmRules"] = [ar for ar in alarm_rules if ar.get("dId") == device.get("dId")]

#     return {"status": "success", "data": devices}

# @app.post("/device")
# @app.post("/api/device")
# async def add_device(request_body: dict, token: str = Header(None)):
#     user_email = get_user_email_from_token(token)
#     new_device = request_body.get("newDevice", {})
#     dId = new_device.get("dId")
#     await devices_collection.update_many({"userId": user_email}, {"$set": {"selected": False}})
#     new_device["userId"] = user_email
#     new_device["createdTime"] = int(time.time() * 1000)
#     new_device["password"] = makeid(10)
#     new_device["selected"] = True
#     await saver_rules_collection.insert_one({"userId": user_email, "dId": dId, "status": True})
#     await devices_collection.insert_one(new_device)
#     return {"status": "success"}

# @app.delete("/device")
# @app.delete("/api/device")
# async def delete_device(dId: str = Query(...), token: str = Header(None)):
#     user_email = get_user_email_from_token(token)
#     await saver_rules_collection.delete_many({"dId": dId})
#     await emqx_auth_rules_collection.delete_many({"dId": dId, "type": "device"})
#     result = await devices_collection.delete_one({"userId": user_email, "dId": dId})
#     return {"status": "success", "data": result.deleted_count}

# @app.put("/device")
# @app.put("/api/device")
# async def update_device_selector(body: dict, token: str = Header(None)):
#     user_email = get_user_email_from_token(token)
#     dId = body.get("dId")
#     await devices_collection.update_many({"userId": user_email}, {"$set": {"selected": False}})
#     await devices_collection.update_one({"userId": user_email, "dId": dId}, {"$set": {"selected": True}})
#     return {"status": "success"}

# @app.get("/template")
# @app.get("/api/template")
# async def get_templates(token: str = Header(None)):
#     user_email = get_user_email_from_token(token)
#     templates = []
#     async for template in templates_collection.find({"userId": user_email}):
#         template["_id"] = str(template["_id"])
#         templates.append(template)
#     return {"status": "success", "data": templates}

# @app.post("/template")
# @app.post("/api/template")
# async def create_template(body: TemplateBody, token: str = Header(None)):
#     user_email = get_user_email_from_token(token)
#     new_template = body.template
#     new_template["userId"] = user_email
#     result = await templates_collection.insert_one(new_template)
#     return {"status": "success", "id": str(result.inserted_id)}

# @app.delete("/template")
# @app.delete("/api/template")
# async def delete_template(templateId: str = Query(...), token: str = Header(None)):
#     user_email = get_user_email_from_token(token)
#     await templates_collection.delete_one({"userId": user_email, "_id": ObjectId(templateId)})
#     return {"status": "success"}

# @app.get("/get-small-charts-data")
# @app.get("/api/get-small-charts-data")
# async def get_small_charts_data(
#     chartTimeAgo: int = Query(...),
#     dId: str = Query(...),
#     variable: str = Query(...),
#     token: str = Header(None)
# ):
#     user_email = get_user_email_from_token(token)
#     try:
#         time_ago_ms = int(time.time() * 1000) - (max(chartTimeAgo, 1440) * 60 * 1000)

#         cursor = data_collection.find({
#             "dId": dId,
#             "variable": variable,
#             "time": {"$gt": time_ago_ms}
#         }).sort("time", 1)

#         data = []
#         async for doc in cursor:
#             doc["_id"] = str(doc["_id"])
#             data.append(doc)

#         return data
#     except Exception as error:
#         print("[DB-ERROR] Error consultando gráficos:", error)
#         return []

# @app.post("/getdevicecredentials")
# @app.post("/api/getdevicecredentials")
# async def get_device_credentials(body: DeviceCredentialsBody):
#     device = await devices_collection.find_one({"dId": body.dId})
#     if not device:
#         raise HTTPException(status_code=401)
#     userId = device.get("userId")
#     credentials = await get_device_mqtt_credentials(body.dId, userId)
#     template = await templates_collection.find_one({"_id": ObjectId(device["templateId"])}) if "templateId" in device else None
    
#     variables = []
#     if template and "widgets" in template:
#         for widget in template["widgets"]:
#             variables.append({
#                 "variable": widget.get("variable"),
#                 "variableFullName": widget.get("variableFullName"),
#                 "variableType": widget.get("variableType"),
#                 "variableSendFreq": widget.get("variableSendFreq")
#             })

#     return {
#         "username": credentials["username"],
#         "password": credentials["password"],
#         "topic": f"{userId}/{body.dId}/",
#         "variables": variables
#     }

# # SAVER WEBHOOK CON FILTRO DE 0.05% DE VARIACIÓN
# @app.post("/saver-webhook")
# @app.post("/api/saver-webhook")

# from fastapi import Request, HTTPException, Header
# from typing import Optional
# import time

# @app.post("/saver-webhook")
# @app.post("/api/saver-webhook")

# async def saver_webhook(request: Request, token: Optional[str] = Header(None)):
#     print(f"\n🔥 [WEBHOOK HIT] ¡EMQX impactó el endpoint!")
#     print(f"   -> Token recibido en Header: '{token}' (Esperado: '{EMQX_API_TOKEN}')")
    
#     if token != EMQX_API_TOKEN:
#         print(f"❌ [WEBHOOK RECHAZADO] Token inválido. El webhook fue bloqueado.")
#         raise HTTPException(status_code=404)

#     try:
#         request_data = await request.json()
#     except Exception as e:
#         print(f"❌ [WEBHOOK ERROR] No se pudo parsear el JSON recibido: {e}")
#         return {"status": "error", "message": "Invalid JSON"}

#     topic = request_data.get("topic", "")
#     data_user_id = request_data.get("userId")
#     print(f"   -> Tópico extraído: '{topic}'")
#     splitted_topic = topic.split("/")
    
#     if len(splitted_topic) >= 3:
#         user_id = splitted_topic[0] if not data_user_id else data_user_id
#         dId = splitted_topic[1]
#         variable = splitted_topic[2]
        
#         device_found = await devices_collection.find_one({"dId": dId})
        
#         if device_found:
#             real_user_id = device_found.get("userId", user_id)
#             payload_val = request_data.get("payload", {})
            
#             # Manejo seguro por si el payload viene anidado como dict o como valor directo
#             if isinstance(payload_val, dict):
#                 current_value = float(payload_val.get("value", 0))
#             else:
#                 current_value = float(payload_val)

#             # 1. Aplicar filtro de variación del 0.05%
#             last_record = await data_collection.find_one(
#                 {"dId": dId, "variable": variable},
#                 sort=[("time", -1)]
#             )

#             if last_record:
#                 last_value = float(last_record.get("value", 0))
#                 percent_change = 0.0
#                 if last_value != 0:
#                     percent_change = abs((current_value - last_value) / last_value) * 100
#                 else:
#                     percent_change = 100.0 if current_value != 0 else 0.0

#                 if percent_change < 0.05:
#                     print(f"🛡️ [FILTRO WEBHOOK] Dato descartado. Variación de {percent_change:.4f}% menor al 0.05% (dId: {dId}, var: {variable})")
#                     return {"status": "filtered", "message": "Variation below 0.05%"}

#             # 2. Guardar en MongoDB si pasa el umbral o es el primer registro
#             timestamp = int(time.time() * 1000)
#             await data_collection.insert_one({
#                 "userId": real_user_id,
#                 "dId": dId,
#                 "variable": variable,
#                 "value": current_value,
#                 "time": timestamp,
#                 "path": f"/{real_user_id}/{dId}/{timestamp}/"
#             })
#             print(f"✅ [DB SUCCESS] ¡DATO GUARDADO EN MONGODB! -> dId: {dId} | Var: {variable} | Valor: {current_value}\n")
#         else:
#             print(f"⚠️ [AVISO DB] El dispositivo dId={dId} no existe en MongoDB, se descarta el registro.")
#     else:
#         print(f"❌ [TÓPICO INVÁLIDO] El tópico '{topic}' no cumple con el formato de niveles mínimos.")
            
#     return {"status": "success"}

# @app.post("/alarm-webhook")
# @app.post("/api/alarm-webhook")
# async def alarm_webhook(incoming_alarm: dict, token: Optional[str] = Header(None)):
#     return {"status": "success"}

# @app.get("/notifications")
# @app.get("/api/notifications")
# async def get_notifications(token: str = Header(None)):
#     return {"status": "success", "data": []}

# @app.put("/notifications")
# @app.put("/api/notifications")
# async def update_notification(body: NotificationUpdateBody, token: str = Header(None)):
#     return {"status": "success"}

# @app.post("/getmqttcredentials")
# @app.post("/api/getmqttcredentials")
# async def get_mqtt_credentials(token: str = Header(None)):
#     return {"status": "success", "username": "admin", "password": "public"}