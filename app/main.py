

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

# Importar el módulo de rutas de alarmas separado
from api.routes import alarms

app.include_router(alarms.router)

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
    host = os.getenv("EMQX_API_HOST")
    if not host:
        print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
        return
    
    base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
    username = os.getenv("EMQX_DASHBOARD_USER", "admin")
    password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

    print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
                login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
                print(f"🔑 [EMQX-LOGIN] Status code recibido: {login_res.status_code}")
                
                if login_res.status_code != 200:
                    print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
                    return
                
                token = login_res.json().get("token") or login_res.json().get("access_token")
                if not token:
                    print("❌ [EMQX-ERROR] Login OK pero no se encontró el token.")
                    return

                headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

                # ==========================================
                # 1. SAVER WEBHOOK (Bridge & Rule)
                # ==========================================
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
                
                print(f"🌉 [EMQX-CONNECTOR SAVER] Status: {conn_res.status_code}")
                await asyncio.sleep(1)

                rule_payload = {
                    "id": "saver_webhook_rule", 
                    "sql": "SELECT * FROM \"+/+/+/sdata\"", 
                    "actions": ["http:saver_webhook_bridge"]
                }

                rule_res = await httpx_client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
                if rule_res.status_code not in [200, 201]:
                    rule_res = await httpx_client.put(f"{base_url}/rules/saver_webhook_rule", json=rule_payload, headers=headers)

                print(f"⚙️ [EMQX-RULE SAVER] Status: {rule_res.status_code}")

                # ==========================================
                # 2. ALARM WEBHOOK (Bridge & Rule)
                # ==========================================
                alarm_bridge_payload = {
                    "name": "alarm_webhook_bridge",
                    "type": "http",
                    "url": f"{public_api_url}/alarm-webhook",
                    "method": "post",
                    "headers": {"token": EMQX_API_TOKEN},
                    "body": "${.}",
                    "connect_timeout": "15s",
                    "pool_size": 4
                }

                alarm_conn_res = await httpx_client.post(f"{base_url}/bridges", json=alarm_bridge_payload, headers=headers)
                if alarm_conn_res.status_code not in [200, 201]:
                    alarm_conn_res = await httpx_client.put(f"{base_url}/bridges/http:alarm_webhook_bridge", json=alarm_bridge_payload, headers=headers)
                
                print(f"🌉 [EMQX-CONNECTOR ALARM] Status: {alarm_conn_res.status_code}")
                await asyncio.sleep(1)

                alarm_rule_payload = {
                    "id": "alarm_webhook_rule",
                    "sql": "SELECT * FROM \"+/+/+/sdata\" WHERE payload.alarm = true",
                    "actions": ["http:alarm_webhook_bridge"]
                }

                alarm_rule_res = await httpx_client.post(f"{base_url}/rules", json=alarm_rule_payload, headers=headers)
                if alarm_rule_res.status_code not in [200, 201]:
                    alarm_rule_res = await httpx_client.put(f"{base_url}/rules/alarm_webhook_rule", json=alarm_rule_payload, headers=headers)

                print(f"⚙️ [EMQX-RULE ALARM] Status: {alarm_rule_res.status_code}")
                print("✅ [EMQX-INIT] Puentes y reglas configurados correctamente.")
                break

        except Exception as error:
            print(f"⚠️ [EMQX-WARNING] Intento {attempt}/{max_retries} fallido por {type(error).__name__} {error}")
            if attempt < max_retries:
                print("⏳ Reintentando conexión con EMQX en 5 segundos...")
                await asyncio.sleep(5)
            else:
                print(f"❌ [EMQX-EXCEPTION CRITICAL] Se agotaron los reintentos.")



# async def create_emqx_v5_webhook_rule():
#     host = os.getenv("EMQX_API_HOST")
#     if not host:
#         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
#         return
    
#     base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
#     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
#     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

#     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

#     max_retries = 3
#     for attempt in range(1, max_retries + 1):
#         try:
#             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
#                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
#                 print(f"🔑 [EMQX-LOGIN] Status code recibido: {login_res.status_code}")
                
#                 if login_res.status_code != 200:
#                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
#                     return
                
#                 token = login_res.json().get("token") or login_res.json().get("access_token")
#                 if not token:
#                     print("❌ [EMQX-ERROR] Login OK pero no se encontró el token.")
#                     return

#                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

#                 connector_payload = {
#                     "name": "saver_webhook_bridge",
#                     "type": "http",
#                     "url": f"{public_api_url}/saver-webhook",
#                     "method": "post",
#                     "headers": {"token": EMQX_API_TOKEN},
#                     "body": "${.}",
#                     "connect_timeout": "15s",
#                     "pool_size": 8
#                 }

#                 conn_res = await httpx_client.post(f"{base_url}/bridges", json=connector_payload, headers=headers)
#                 if conn_res.status_code not in [200, 201]:
#                     conn_res = await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=connector_payload, headers=headers)
                
#                 print(f"🌉 [EMQX-CONNECTOR] Status: {conn_res.status_code} Resp: {conn_res.text}")
#                 await asyncio.sleep(1)

#                 rule_payload = {
#                     "id": "saver_webhook_rule", 
#                     "sql": "SELECT * FROM \"+/+/+/sdata\"", 
#                     "actions": ["http:saver_webhook_bridge"]
#                 }

#                 rule_res = await httpx_client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
#                 if rule_res.status_code not in [200, 201]:
#                     rule_res = await httpx_client.put(f"{base_url}/rules/saver_webhook_rule", json=rule_payload, headers=headers)

#                 print(f"⚙️ [EMQX-RULE] Status: {rule_res.status_code} Resp: {rule_res.text}")
                
#                 # 2. Alarm Webhook Bridge & Rule
#                 alarm_bridge_payload = {
#                     "name": "alarm_webhook_bridge",
#                     "type": "http",
#                     "config": {
#                         "url": f"{public_api_url}/alarm-webhook",
#                         "method": "post",
#                         "headers": {"token": EMQX_API_TOKEN},
#                         "body": "${.}",
#                         "connect_timeout": "15s",
#                         "pool_size": 4
#                     }
#                 }
#                 conn_res = await httpx_client.post(f"{base_url}/bridges", json=connector_payload, headers=headers)
#                 if conn_res.status_code not in [200, 201]:
#                     conn_res = await httpx_client.put(f"{base_url}/rules/alarm_webhook_bridge", json=alarm_bridge_payload, headers=headers)
                
#                 print(f"🌉 [EMQX-CONNECTOR] Status: {conn_res.status_code} Resp: {conn_res.text}")
#                 await asyncio.sleep(1)

#                 rule_payload = {
#                     "id": "saver_webhook_rule", 
#                     "sql": "SELECT * FROM \"+/+/+/sdata\"", 
#                     "actions": ["http:saver_webhook_bridge"]
#                 }

#                 rule_res = await httpx_client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
#                 if rule_res.status_code not in [200, 201]:
#                     rule_res = await httpx_client.put(f"{base_url}/rules/alarm_webhook_rule", json=rule_payload, headers=headers)

#                 print(f"⚙️ [EMQX-RULE] Status: {rule_res.status_code} Resp: {rule_res.text}")

#                 print("✅ [EMQX-INIT] Puentes y reglas configurados correctamente.")
#                 break

#         except Exception as error:
#             print(f"⚠️ [EMQX-WARNING] Intento {attempt}/{max_retries} fallido por {type(error).__name__} {error}")
#             if attempt < max_retries:
#                 print("⏳ Reintentando conexión con EMQX en 5 segundos...")
#                 await asyncio.sleep(5)
#             else:
#                 print(f"❌ [EMQX-EXCEPTION CRITICAL] Se agotaron los reintentos.")

# async def create_emqx_v5_webhook_rule():
#     host = os.getenv("EMQX_API_HOST")
#     if not host:
#         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
#         return
    
#     base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
#     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
#     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

#     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

#     max_retries = 3
#     for attempt in range(1, max_retries + 1):
#         try:
#             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
#                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
#                 print(f"🔑 [EMQX-LOGIN] Status code recibido: {login_res.status_code}")
                
#                 if login_res.status_code != 200:
#                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
#                     return
                
#                 token = login_res.json().get("token") or login_res.json().get("access_token")
#                 if not token:
#                     print("❌ [EMQX-ERROR] Login OK pero no se encontró el token.")
#                     return

#                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

#                 connector_payload = {
#                     "name": "saver_webhook_bridge",
#                     "type": "http",
#                     "url": f"{public_api_url}/saver-webhook",
#                     "method": "post",
#                     "headers": {"token": EMQX_API_TOKEN},
#                     "body": "${.}",
#                     "connect_timeout": "15s",
#                     "pool_size": 8
#                 }

#                 conn_res = await httpx_client.post(f"{base_url}/bridges", json=connector_payload, headers=headers)
#                 if conn_res.status_code not in [200, 201]:
#                     conn_res = await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=connector_payload, headers=headers)
                
#                 print(f"🌉 [EMQX-CONNECTOR] Status: {conn_res.status_code} Resp: {conn_res.text}")
#                 await asyncio.sleep(1)

#                 rule_payload = {
#                     "id": "saver_webhook_rule", 
#                     "sql": "SELECT * FROM \"+/+/+/sdata\"", 
#                     "actions": ["http:saver_webhook_bridge"]
#                 }

#                 rule_res = await httpx_client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
#                 if rule_res.status_code not in [200, 201]:
#                     rule_res = await httpx_client.put(f"{base_url}/rules/saver_webhook_rule", json=rule_payload, headers=headers)

#                 print(f"⚙️ [EMQX-RULE] Status: {rule_res.status_code} Resp: {rule_res.text}")
#                 break
                
#                 2. Alarm Webhook Bridge & Rule
#                 alarm_bridge_payload = {
#                     "name": "http:alarm_webhook_bridge",
#                     "type": "http",
#                     "config": {
#                         "url": f"{public_api_url}/alarm-webhook",
#                         "method": "post",
#                         "headers": {"token": EMQX_API_TOKEN},
#                         "body": "${.}",
#                         "connect_timeout": "15s",
#                         "pool_size": 4
#                     }
#                 }

#                 a_res = await httpx_client.post(f"{base_url}/bridges", json=alarm_bridge_payload, headers=headers)
#                 if a_res.status_code not in [200, 201]:
#                     await httpx_client.put(f"{base_url}/bridges/http:alarm_webhook_bridge", json=alarm_bridge_payload, headers=headers)

#                 await asyncio.sleep(1)

#                 rule_alarm_payload = {
#                     "id": "alarm_webhook_rule",
#                     "sql": "SELECT * FROM \"+/+/+/sdata\" WHERE payload.alarm = true",
#                     "actions": ["http:alarm_webhook_bridge"]
#                 }
#                 await httpx_client.post(f"{base_url}/rules", json=rule_alarm_payload, headers=headers)

#                 print("✅ [EMQX-INIT] Puentes y reglas configurados correctamente.")
#                 break





#         except Exception as error:
#             print(f"⚠️ [EMQX-WARNING] Intento {attempt}/{max_retries} fallido por {type(error).__name__} {error}")
#             if attempt < max_retries:
#                 print("⏳ Reintentando conexión con EMQX en 5 segundos...")
#                 await asyncio.sleep(5)
#             else:
#                 print(f"❌ [EMQX-EXCEPTION CRITICAL] Se agotaron los reintentos.")

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
            "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "virus123"),
            "type": "superuser", "time": current_time, "updatedTime": current_time
        })
    except Exception as error:
        print("[DB-ERROR]", error)

async def init_emqx_background_tasks(db):
    try:
        await asyncio.sleep(3)
        await check_mqtt_superuser(db)
        await create_emqx_v5_webhook_rule()
    except Exception as e:
        print(f"❌ [CRITICAL BACKGROUND ERROR] {e}")

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
        
    saver_rules = []
    async for sr in saver_rules_collection.find({"userId": user_email}):
        sr["_id"] = str(sr["_id"])
        saver_rules.append(sr)
        
    templates = []
    async for t in templates_collection.find({"userId": user_email}):
        t["_id"] = str(t["_id"])
        templates.append(t)
        
    alarm_rules = []
    async for ar in alarm_rules_collection.find({"userId": user_email}):
        ar["_id"] = str(ar["_id"])
        alarm_rules.append(ar)

    for device in devices:
        sr = next((s for s in saver_rules if s.get("dId") == device.get("dId")), None)
        device["saverRule"] = {"_id": sr["_id"], "status": sr.get("status", True)} if sr else {"status": True}
        
        t = next((tp for tp in templates if str(tp.get("_id")) == str(device.get("templateId"))), None)
        if t:
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
        print("[DB-ERROR]", error)
        return []

@app.post("/getdevicecredentials")
@app.post("/getdevicecredentials/")
@app.post("/api/getdevicecredentials")
@app.post("/api/getdevicecredentials/")
async def get_device_credentials(body: DeviceCredentialsBody):
    device = await devices_collection.find_one({"dId": body.dId})
    if not device:
        raise HTTPException(status_code=401)
    userId = device.get("userId")
    credentials = await get_device_mqtt_credentials(body.dId, userId)
    template = await templates_collection.find_one({"_id": ObjectId(device["templateId"])}) if "templateId" in device else None
    
    variables = []
    if template and "widgets" in template:
        for widget in template["widgets"]:
            variables.append({
                "variable": widget.get("variable"),
                "variableFullName": widget.get("variableFullName"),
                "variableType": widget.get("variableType"),
                "variableSendFreq": widget.get("variableSendFreq")
            })

    return {
        "username": credentials["username"],
        "password": credentials["password"],
        "topic": f"{userId}/{body.dId}/",
        "variables": variables
    }

@app.post("/getmqttcredentialsforreconnection")
@app.post("/api/getmqttcredentialsforreconnection")
async def get_mqtt_credentials_for_reconnection(token: str = Header(None)):
    return {
        "status": "success", 
        "username": os.getenv("EMQX_NODE_SUPERUSER_USER", "admin"),
        "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "public")
    }

@app.post("/saver-webhook")
@app.post("/api/saver-webhook")
async def saver_webhook(request: Request, token: Optional[str] = Header(None)):
    if token != EMQX_API_TOKEN:
        raise HTTPException(status_code=404)

    try:
        request_data = await request.json()
    except Exception:
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
            
            current_value = 0.0
            try:
                if isinstance(payload_val, dict):
                    current_value = float(payload_val.get("value", 0))
                elif isinstance(payload_val, str):
                    parsed_payload = json.loads(payload_val)
                    current_value = float(parsed_payload.get("value", 0))
                else:
                    current_value = float(payload_val)
            except Exception:
                return {"status": "error", "message": "Invalid payload value"}

            last_record = await data_collection.find_one(
                {"dId": dId, "variable": variable},
                sort=[("time", -1)]
            )

            if last_record:
                last_value = float(last_record.get("value", 0))
                percent_change = abs((current_value - last_value) / last_value) * 100 if last_value != 0 else (100.0 if current_value != 0 else 0.0)
                if percent_change < 0.05:
                    return {"status": "filtered", "message": "Variation below 0.05%"}

            timestamp = int(time.time() * 1000)
            await data_collection.insert_one({
                "userId": real_user_id,
                "dId": dId,
                "variable": variable,
                "value": current_value,
                "time": timestamp,
                "path": f"/{real_user_id}/{dId}/{timestamp}/"
            })
            
    return {"status": "success"}


@app.post("/alarm-webhook")
@app.post("/api/alarm-webhook")
async def alarm_webhook(request: Request, token: Optional[str] = Header(None)):
    if token and token != EMQX_API_TOKEN:
        raise HTTPException(status_code=403, detail="Unauthorized webhook")

    try:
        request_data = await request.json()
    except Exception:
        return {"status": "error", "message": "Invalid JSON"}

    topic = request_data.get("topic", "")
    userId = request_data.get("userId")
    payload = request_data.get("payload", {})
    
    current_value = 0.0
    try:
        if isinstance(payload, dict):
            current_value = float(payload.get("value", 0))
        elif isinstance(payload, str):
            parsed_payload = json.loads(payload)
            current_value = float(parsed_payload.get("value", 0))
        else:
            current_value = float(payload)
    except Exception:
        current_value = 0.0

    splitted_topic = topic.split("/")
    dId = splitted_topic[1] if len(splitted_topic) >= 2 else request_data.get("dId", "unknown")
    variable = splitted_topic[2] if len(splitted_topic) >= 3 else request_data.get("variable", "unknown")

    device_found = await devices_collection.find_one({"dId": dId})
    real_user_id = device_found.get("userId") if device_found else userId
    if not real_user_id:
        real_user_id = "unknown_user"

    alarm_rule = await alarm_rules_collection.find_one({"dId": dId, "variable": variable})
    variable_full_name = alarm_rule.get("variableFullName", variable) if alarm_rule else variable
    condition = alarm_rule.get("condition", "") if alarm_rule else ""
    target_value = alarm_rule.get("value", "") if alarm_rule else ""

    alert_message = f"Alerta en {variable_full_name}: valor actual {current_value} cumple condición {condition} {target_value}"
    timestamp = int(time.time() * 1000)

    notification_doc = {
        "userId": real_user_id,
        "dId": dId,
        "variable": variable,
        "variableFullName": variable_full_name,
        "value": current_value,
        "condition": condition,
        "targetValue": target_value,
        "message": alert_message,
        "leida": False,
        "time": timestamp
    }

    # Guardado seguro en MongoDB evitando duplicados de ID
    try:
        await notifications_collection.insert_one(notification_doc)
    except Exception:
        if "_id" in notification_doc:
            del notification_doc["_id"]
        try:
            await notifications_collection.insert_one(notification_doc)
        except Exception:
            pass

    # 🚀 PUBLICAR EN MQTT HACIA EMQX PARA QUE NUXT LO RECIBA EN TIEMPO REAL
    try:
        host = os.getenv("EMQX_API_HOST")
        if host:
            base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
            username = os.getenv("EMQX_DASHBOARD_USER", "admin")
            password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")
            
            async with httpx.AsyncClient(verify=False, timeout=5.0) as client_http:
                login_res = await client_http.post(f"{base_url}/login", json={"username": username, "password": password})
                if login_res.status_code == 200:
                    emqx_token = login_res.json().get("token") or login_res.json().get("access_token")
                    if emqx_token:
                        notif_topic = f"{real_user_id}/{dId}/{variable}/notif"
                        publish_payload = {
                            "topic": notif_topic,
                            "payload": json.dumps({
                                "msg": alert_message,
                                "variable": variable,
                                "value": current_value,
                                "time": timestamp
                            }),
                            "qos": 0,
                            "retain": False
                        }

                        headers = {"Authorization": f"Bearer {emqx_token}", "Content-Type": "application/json"}
                        await client_http.post(f"{base_url}/mqtt/publish", json=publish_payload, headers=headers)
                      
                        # headers = {"Authorization": f"Bearer {emqx_token}", "Content-Type": "application/json"}
                        # await client_http.post(f"{base_url}/mqtt/publish", json=publish_payload, headers=headers)
    except Exception as pub_error:
        print(f"⚠️ [MQTT-PUBLISH-WARNING] No se pudo publicar la alarma en el broker: {pub_error}")

    print(f"🚨 [ALARM-TRIGGERED] Alerta registrada y publicada para el usuario {real_user_id} en dispositivo {dId}, variable {variable}")

    return {
        "status": "success", 
        "message": "Alarm processed, logged and published successfully",
        "alarm": {
            "active": True,
            "msg": alert_message,
            "variable": variable,
            "value": current_value
        }
    }




# from datetime import datetime, timedelta, timezone
# from fastapi import FastAPI, HTTPException, Header, status, Query, Request
# from fastapi.middleware.cors import CORSMiddleware
# from pydantic import BaseModel, EmailStr
# from motor.motor_asyncio import AsyncIOMotorClient
# from typing import Optional, Dict, Any
# from bson import ObjectId
# import bcrypt
# from jose import jwt, JWTError
# import time
# import random
# import os
# import asyncio
# import httpx
# import json

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

# # Importar el módulo de rutas de alarmas separado
# from api.routes import alarms

# app.include_router(alarms.router)

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
#         if token.startswith("Bearer "):
#             token = token.split(" ")[1]
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         email: str = payload.get("sub")
#         if email is None:
#             raise HTTPException(status_code=401, detail="Token inválido")
#         return email
#     except JWTError:
#         raise HTTPException(status_code=401, detail="Token inválido o expirado")


# async def create_emqx_v5_webhook_rule():
#     host = os.getenv("EMQX_API_HOST")
#     if not host:
#         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
#         return
    
#     base_url = f"{host}/api/v5" if host.startswith(("http://", "https://")) else f"http://{host}/api/v5"
#     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
#     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")
    
#     # Token limpio tal como lo hacías en Node.js con process.env.EMQX_API_TOKEN
#     api_token = os.getenv("EMQX_API_TOKEN", "tu_token_secreto_emqx").strip('"\'')

#     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

#     max_retries = 3
#     for attempt in range(1, max_retries + 1):
#         try:
#             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
#                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
#                 if login_res.status_code != 200:
#                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
#                     return
                
#                 token = login_res.json().get("token") or login_res.json().get("access_token")
#                 if not token:
#                     return

#                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
#                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

#                 # 1. Saver Webhook Bridge & Rule (Estructura limpia para EMQX v5)
#                 saver_bridge_payload = {
#                     "name": "http:saver_webhook_bridge",
#                     "type": "http",
#                     "config": {
#                         "url": f"{public_api_url}/saver-webhook",
#                         "method": "post",
#                         "headers": {"token": api_token},
#                         "body": "${.}",
#                         "connect_timeout": "15s",
#                         "pool_size": 8
#                     }
#                 }

#                 s_res = await httpx_client.post(f"{base_url}/bridges", json=saver_bridge_payload, headers=headers)
#                 if s_res.status_code not in [200, 201]:
#                     await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=saver_bridge_payload, headers=headers)

#                 await asyncio.sleep(1)

#                 rule_saver_payload = {
#                     "id": "saver_webhook_rule",
#                     "sql": "SELECT * FROM \"+/+/+/sdata\"",
#                     "actions": ["http:saver_webhook_bridge"]
#                 }
#                 await httpx_client.post(f"{base_url}/rules", json=rule_saver_payload, headers=headers)

#                 # 2. Alarm Webhook Bridge & Rule
#                 alarm_bridge_payload = {
#                     "name": "http:alarm_webhook_bridge",
#                     "type": "http",
#                     "config": {
#                         "url": f"{public_api_url}/alarm-webhook",
#                         "method": "post",
#                         "headers": {"token": api_token},
#                         "body": "${.}",
#                         "connect_timeout": "15s",
#                         "pool_size": 4
#                     }
#                 }

#                 a_res = await httpx_client.post(f"{base_url}/bridges", json=alarm_bridge_payload, headers=headers)
#                 if a_res.status_code not in [200, 201]:
#                     await httpx_client.put(f"{base_url}/bridges/http:alarm_webhook_bridge", json=alarm_bridge_payload, headers=headers)

#                 await asyncio.sleep(1)

#                 rule_alarm_payload = {
#                     "id": "alarm_webhook_rule",
#                     "sql": "SELECT * FROM \"+/+/+/sdata\" WHERE payload.alarm = true",
#                     "actions": ["http:alarm_webhook_bridge"]
#                 }
#                 await httpx_client.post(f"{base_url}/rules", json=rule_alarm_payload, headers=headers)

#                 print("✅ [EMQX-INIT] Puentes y reglas configurados correctamente.")
#                 break
                
#         except Exception as error:
#             print(f"⚠️ [EMQX-WARNING] Error en intento {attempt}: {error}")
#             await asyncio.sleep(5)


# # async def create_emqx_v5_webhook_rule():
# #     host = os.getenv("EMQX_API_HOST")
# #     if not host:
# #         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
# #         return
    
# #     base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
# #     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
# #     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

# #     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

# #     max_retries = 3
# #     for attempt in range(1, max_retries + 1):
# #         try:
# #             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
# #                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
# #                 print(f"🔑 [EMQX-LOGIN] Status code recibido: {login_res.status_code}")
                
# #                 if login_res.status_code != 200:
# #                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
# #                     return
                
# #                 token = login_res.json().get("token") or login_res.json().get("access_token")
# #                 if not token:
# #                     print("❌ [EMQX-ERROR] Login OK pero no se encontró el token.")
# #                     return

# #                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# #                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

# #                 connector_payload = {
# #                     "name": "saver_webhook_bridge",
# #                     "type": "http",
# #                     "url": f"{public_api_url}/saver-webhook",
# #                     "method": "post",
# #                     "headers": {"token": EMQX_API_TOKEN},
# #                     "body": "${.}",
# #                     "connect_timeout": "15s",
# #                     "pool_size": 8
# #                 }

# #                 conn_res = await httpx_client.post(f"{base_url}/bridges", json=connector_payload, headers=headers)
# #                 if conn_res.status_code not in [200, 201]:
# #                     conn_res = await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=connector_payload, headers=headers)
                
# #                 print(f"🌉 [EMQX-CONNECTOR] Status: {conn_res.status_code} Resp: {conn_res.text}")
# #                 await asyncio.sleep(1)

# #                 rule_payload = {
# #                     "id": "saver_webhook_rule", 
# #                     "sql": "SELECT * FROM \"+/+/+/sdata\"", 
# #                     "actions": ["http:saver_webhook_bridge"]
# #                 }

# #                 rule_res = await httpx_client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
# #                 if rule_res.status_code not in [200, 201]:
# #                     rule_res = await httpx_client.put(f"{base_url}/rules/saver_webhook_rule", json=rule_payload, headers=headers)

# #                 print(f"⚙️ [EMQX-RULE] Status: {rule_res.status_code} Resp: {rule_res.text}")
# #                 break
                
# #         except Exception as error:
# #             print(f"⚠️ [EMQX-WARNING] Intento {attempt}/{max_retries} fallido por {type(error).__name__} {error}")
# #             if attempt < max_retries:
# #                 print("⏳ Reintentando conexión con EMQX en 5 segundos...")
# #                 await asyncio.sleep(5)
# #             else:
# #                 print(f"❌ [EMQX-EXCEPTION CRITICAL] Se agotaron los reintentos.")

# # async def create_emqx_v5_webhook_rule():
# #     host = os.getenv("EMQX_API_HOST")
# #     if not host:
# #         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
# #         return
    
# #     base_url = f"{host}/api/v5" if host.startswith(("http://", "https://")) else f"http://{host}/api/v5"
# #     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
# #     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

# #     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

# #     max_retries = 3
# #     for attempt in range(1, max_retries + 1):
# #         try:
# #             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
# #                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
# #                 if login_res.status_code != 200:
# #                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
# #                     return
                
# #                 token = login_res.json().get("token") or login_res.json().get("access_token")
# #                 if not token:
# #                     return

# #                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# #                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

# #                 # 1. Saver Webhook Bridge & Rule
# #                 saver_bridge_payload = {
# #                     "name": "http:saver_webhook_bridge",
# #                     "type": "http",
# #                     "config": {
# #                         "url": f"{public_api_url}/saver-webhook",
# #                         "method": "post",
# #                         "headers": {"token": EMQX_API_TOKEN},
# #                         "body": "${.}",
# #                         "connect_timeout": "15s",
# #                         "pool_size": 8
# #                     }
# #                 }

# #                 s_res = await httpx_client.post(f"{base_url}/bridges", json=saver_bridge_payload, headers=headers)
# #                 if s_res.status_code not in [200, 201]:
# #                     await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=saver_bridge_payload, headers=headers)

# #                 await asyncio.sleep(1)

# #                 # rule_saver_payload = {
# #                 #     "id": "saver_webhook_rule",
# #                 #     "sql": "SELECT * FROM \"+/+/+/+/sdata\"",
# #                 #     "actions": ["http:saver_webhook_bridge"]
# #                 # }
                
# #                 rule_saver_payload = {
# #                     "id": "saver_webhook_rule",
# #                     "sql": "SELECT * FROM \"+/+/+/sdata\"",
# #                     "actions": ["http:saver_webhook_bridge"]
# #                 }
# #                 await httpx_client.post(f"{base_url}/rules", json=rule_saver_payload, headers=headers)

# #                 # 2. Alarm Webhook Bridge Global (El puente HTTP general para todas las alarmas)
# #                 alarm_bridge_payload = {
# #                     "name": "http:alarm_webhook_bridge",
# #                     "type": "http",
# #                     "config": {
# #                         "url": f"{public_api_url}/alarm-webhook",
# #                         "method": "post",
# #                         "headers": {"token": EMQX_API_TOKEN},
# #                         "body": "${.}",
# #                         "connect_timeout": "15s",
# #                         "pool_size": 4
# #                     }
# #                 }

# #                 a_res = await httpx_client.post(f"{base_url}/bridges", json=alarm_bridge_payload, headers=headers)
# #                 if a_res.status_code not in [200, 201]:
# #                     await httpx_client.put(f"{base_url}/bridges/http:alarm_webhook_bridge", json=alarm_bridge_payload, headers=headers)

# #                 print("✅ [EMQX-INIT] Puentes de datos y alarmas configurados correctamente en el arranque.")
# #                 break
                
# #         except Exception as error:
# #             print(f"⚠️ [EMQX-WARNING] Error en intento {attempt}: {error}")
# #             await asyncio.sleep(5)

# # async def create_emqx_v5_webhook_rule():
# #     host = os.getenv("EMQX_API_HOST")
# #     if not host:
# #         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
# #         return
    
# #     base_url = f"{host}/api/v5" if host.startswith(("http://", "https://")) else f"http://{host}/api/v5"
# #     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
# #     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

# #     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

# #     max_retries = 3
# #     for attempt in range(1, max_retries + 1):
# #         try:
# #             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
# #                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
# #                 if login_res.status_code != 200:
# #                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
# #                     return
                
# #                 token = login_res.json().get("token") or login_res.json().get("access_token")
# #                 if not token:
# #                     return

# #                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# #                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

# #                 # 1. Saver Webhook Bridge & Rule (Este se queda para guardar datos)
# #                 saver_bridge_payload = {
# #                     "name": "http:saver_webhook_bridge",
# #                     "type": "http",
# #                     "config": {
# #                         "url": f"{public_api_url}/saver-webhook",
# #                         "method": "post",
# #                         "headers": {"token": EMQX_API_TOKEN},
# #                         "body": "${.}",
# #                         "connect_timeout": "15s",
# #                         "pool_size": 8
# #                     }
# #                 }

# #                 s_res = await httpx_client.post(f"{base_url}/bridges", json=saver_bridge_payload, headers=headers)
# #                 if s_res.status_code not in [200, 201]:
# #                     await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=saver_bridge_payload, headers=headers)

# #                 await asyncio.sleep(1)

# #                 rule_saver_payload = {
# #                     "id": "saver_webhook_rule",
# #                     "sql": "SELECT * FROM \"+/+/+/sdata\"",
# #                     "actions": ["http:saver_webhook_bridge"]
# #                 }
# #                 await httpx_client.post(f"{base_url}/rules", json=rule_saver_payload, headers=headers)

# #                 print("✅ [EMQX-INIT] Puente y regla de guardado configurados correctamente.")
# #                 break
                
# #         except Exception as error:
# #             print(f"⚠️ [EMQX-WARNING] Error en intento {attempt}: {error}")
# #             await asyncio.sleep(5)


# # esta andaba-----------------------------------------------------------------------------
# # async def create_emqx_v5_webhook_rule():
# #     host = os.getenv("EMQX_API_HOST")
# #     if not host:
# #         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
# #         return
    
# #     base_url = f"{host}/api/v5" if host.startswith(("http://", "https://")) else f"http://{host}/api/v5"
# #     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
# #     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

# #     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

# #     max_retries = 3
# #     for attempt in range(1, max_retries + 1):
# #         try:
# #             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
# #                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
# #                 if login_res.status_code != 200:
# #                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
# #                     return
                
# #                 token = login_res.json().get("token") or login_res.json().get("access_token")
# #                 if not token:
# #                     return

# #                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# #                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

# #                 # 1. Saver Webhook Bridge & Rule
# #                 saver_bridge_payload = {
# #                     "name": "http:saver_webhook_bridge",
# #                     "type": "http",
# #                     "config": {
# #                         "url": f"{public_api_url}/saver-webhook",
# #                         "method": "post",
# #                         "headers": {"token": EMQX_API_TOKEN},
# #                         "body": "${.}",
# #                         "connect_timeout": "15s",
# #                         "pool_size": 8
# #                     }
# #                 }

# #                 s_res = await httpx_client.post(f"{base_url}/bridges", json=saver_bridge_payload, headers=headers)
# #                 if s_res.status_code not in [200, 201]:
# #                     await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=saver_bridge_payload, headers=headers)

# #                 await asyncio.sleep(1)

# #                 rule_saver_payload = {
# #                     "id": "saver_webhook_rule",
# #                     "sql": "SELECT * FROM \"+/+/+/sdata\"",
# #                     "actions": ["http:saver_webhook_bridge"]
# #                 }
# #                 await httpx_client.post(f"{base_url}/rules", json=rule_saver_payload, headers=headers)

# #                 # 2. Alarm Webhook Bridge & Rule
# #                 alarm_bridge_payload = {
# #                     "name": "http:alarm_webhook_bridge",
# #                     "type": "http",
# #                     "config": {
# #                         "url": f"{public_api_url}/alarm-webhook",
# #                         "method": "post",
# #                         "headers": {"token": EMQX_API_TOKEN},
# #                         "body": "${.}",
# #                         "connect_timeout": "15s",
# #                         "pool_size": 4
# #                     }
# #                 }

# #                 a_res = await httpx_client.post(f"{base_url}/bridges", json=alarm_bridge_payload, headers=headers)
# #                 if a_res.status_code not in [200, 201]:
# #                     await httpx_client.put(f"{base_url}/bridges/http:alarm_webhook_bridge", json=alarm_bridge_payload, headers=headers)

# #                 await asyncio.sleep(1)

# #                 rule_alarm_payload = {
# #                     "id": "alarm_webhook_rule",
# #                     "sql": "SELECT * FROM \"+/+/+/sdata\" WHERE payload.alarm = true",
# #                     "actions": ["http:alarm_webhook_bridge"]
# #                 }
# #                 await httpx_client.post(f"{base_url}/rules", json=rule_alarm_payload, headers=headers)
# #                 print("✅ [EMQX-INIT] Puentes y reglas configurados correctamente.")
# #                 break
                
# #         except Exception as error:
# #             print(f"⚠️ [EMQX-WARNING] Error en intento {attempt}: {error}")
# #             await asyncio.sleep(5)


# # async def create_emqx_v5_webhook_rule():
# #     host = os.getenv("EMQX_API_HOST")
# #     if not host:
# #         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
# #         return
    
# #     base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
# #     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
# #     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

# #     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

# #     max_retries = 3
# #     for attempt in range(1, max_retries + 1):
# #         try:
# #             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
# #                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
# #                 if login_res.status_code != 200:
# #                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
# #                     return
                
# #                 token = login_res.json().get("token") or login_res.json().get("access_token")
# #                 if not token:
# #                     return

# #                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# #                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

# #                 # 1. Saver Webhook
# #                 saver_bridge_payload = {
# #                     "name": "saver_webhook_bridge",
# #                     "type": "http",
# #                     "config": {
# #                         "url": f"{public_api_url}/saver-webhook",
# #                         "method": "post",
# #                         "headers": {"token": EMQX_API_TOKEN},
# #                         "body": "${.}",
# #                         "connect_timeout": "15s",
# #                         "pool_size": 8
# #                     }
# #                 }

# #                 s_res = await httpx_client.post(f"{base_url}/bridges", json=saver_bridge_payload, headers=headers)
# #                 if s_res.status_code not in [200, 201]:
# #                     await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=saver_bridge_payload, headers=headers)

# #                 rule_saver_payload = {
# #                     "id": "saver_webhook_rule",
# #                     "sql": "SELECT * FROM \"+/+/+/sdata\"",
# #                     "actions": ["http:saver_webhook_bridge"]
# #                 }
# #                 await httpx_client.post(f"{base_url}/rules", json=rule_saver_payload, headers=headers)
# #                 await asyncio.sleep(1)

# #                 # 2. Alarm Webhook
# #                 alarm_bridge_payload = {
# #                     "name": "alarm_webhook_bridge",
# #                     "type": "http",
# #                     "config": {
# #                         "url": f"{public_api_url}/alarm-webhook",
# #                         "method": "post",
# #                         "headers": {"token": EMQX_API_TOKEN},
# #                         "body": "${.}",
# #                         "connect_timeout": "15s",
# #                         "pool_size": 4
# #                     }
# #                 }

# #                 a_res = await httpx_client.post(f"{base_url}/bridges", json=alarm_bridge_payload, headers=headers)
# #                 if a_res.status_code not in [200, 201]:
# #                     await httpx_client.put(f"{base_url}/bridges/http:alarm_webhook_bridge", json=alarm_bridge_payload, headers=headers)

# #                 rule_alarm_payload = {
# #                     "id": "alarm_webhook_rule",
# #                     "sql": "SELECT * FROM \"+/+/+/sdata\" WHERE payload.alarm = true",
# #                     "actions": ["http:alarm_webhook_bridge"]
# #                 }
# #                 await httpx_client.post(f"{base_url}/rules", json=rule_alarm_payload, headers=headers)

# #                 print("✅ [EMQX-INIT] Ambas reglas y puentes configurados correctamente.")
# #                 break
                
# #         except Exception as error:
# #             print(f"⚠️ [EMQX-WARNING] Error en intento {attempt}: {error}")
# #             await asyncio.sleep(5)

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
#             "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "virus123"),
#             "type": "superuser", "time": current_time, "updatedTime": current_time
#         })
#     except Exception as error:
#         print("[DB-ERROR]", error)

# async def init_emqx_background_tasks(db):
#     try:
#         await asyncio.sleep(3)
#         await check_mqtt_superuser(db)
#         await create_emqx_v5_webhook_rule()
#     except Exception as e:
#         print(f"❌ [CRITICAL BACKGROUND ERROR] {e}")

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
        
#     saver_rules = []
#     async for sr in saver_rules_collection.find({"userId": user_email}):
#         sr["_id"] = str(sr["_id"])
#         saver_rules.append(sr)
        
#     templates = []
#     async for t in templates_collection.find({"userId": user_email}):
#         t["_id"] = str(t["_id"])
#         templates.append(t)
        
#     alarm_rules = []
#     async for ar in alarm_rules_collection.find({"userId": user_email}):
#         ar["_id"] = str(ar["_id"])
#         alarm_rules.append(ar)

#     for device in devices:
#         sr = next((s for s in saver_rules if s.get("dId") == device.get("dId")), None)
#         device["saverRule"] = {"_id": sr["_id"], "status": sr.get("status", True)} if sr else {"status": True}
        
#         t = next((tp for tp in templates if str(tp.get("_id")) == str(device.get("templateId"))), None)
#         if t:
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
#         print("[DB-ERROR]", error)
#         return []


# @app.post("/getdevicecredentials")
# @app.post("/getdevicecredentials/")
# @app.post("/getmqttcredentials")
# @app.post("/getmqttcredentials/")
# @app.post("/api/getdevicecredentials")
# @app.post("/api/getdevicecredentials/")
# @app.post("/api/getmqttcredentials")
# @app.post("/api/getmqttcredentials/")
# async def get_device_credentials(request: Request, dId: Optional[str] = Query(None)):
#     body_data = {}
#     try:
#         body_data = await request.json()
#     except Exception:
#         pass
        
#     device_id = body_data.get("dId") or dId
    
#     # Si aun así no viene el dId, buscamos el primer dispositivo registrado para que no falle
#     if not device_id:
#         first_device = await devices_collection.find_one({})
#         if first_device:
#             device_id = first_device.get("dId")
            
#     if not device_id:
#         raise HTTPException(status_code=400, detail="Falta el campo dId")
        
#     device = await devices_collection.find_one({"dId": device_id})
#     if not device:
#         # Si no lo encuentra, usamos el superusuario de respaldo para evitar cortes en el dashboard
#         return {
#             "username": os.getenv("EMQX_NODE_SUPERUSER_USER", "admin"),
#             "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "public"),
#             "topic": f"unknown/{device_id}/",
#             "variables": []
#         }
        
#     userId = device.get("userId")
#     credentials = await get_device_mqtt_credentials(device_id, userId)
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
#         "topic": f"{userId}/{device_id}/",
#         "variables": variables
#     }

# # @app.post("/getdevicecredentials")
# # @app.post("/getdevicecredentials/")
# # @app.post("/api/getdevicecredentials")
# # @app.post("/api/getdevicecredentials/")
# # async def get_device_credentials(body: DeviceCredentialsBody):
# #     device = await devices_collection.find_one({"dId": body.dId})
# #     if not device:
# #         raise HTTPException(status_code=401)
# #     userId = device.get("userId")
# #     credentials = await get_device_mqtt_credentials(body.dId, userId)
# #     template = await templates_collection.find_one({"_id": ObjectId(device["templateId"])}) if "templateId" in device else None
    
# #     variables = []
# #     if template and "widgets" in template:
# #         for widget in template["widgets"]:
# #             variables.append({
# #                 "variable": widget.get("variable"),
# #                 "variableFullName": widget.get("variableFullName"),
# #                 "variableType": widget.get("variableType"),
# #                 "variableSendFreq": widget.get("variableSendFreq")
# #             })

# #     return {
# #         "username": credentials["username"],
# #         "password": credentials["password"],
# #         "topic": f"{userId}/{body.dId}/",
# #         "variables": variables
# #     }

# @app.post("/getmqttcredentialsforreconnection")
# @app.post("/api/getmqttcredentialsforreconnection")
# async def get_mqtt_credentials_for_reconnection(token: str = Header(None)):
#     return {
#         "status": "success", 
#         "username": os.getenv("EMQX_NODE_SUPERUSER_USER", "admin"),
#         "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "public")
#     }


# @app.post("/saver-webhook")
# async def saver_webhook(request: Request, token: str = Header(None)):
#     if token != EMQX_API_TOKEN:
#         raise HTTPException(status_code=404, detail="Unauthorized")
    
#     try:
#         body = await request.json()
#         topic = body.get("topic", "")
#         splitted_topic = topic.split("/")
        
#         if len(splitted_topic) >= 3:
#             dId = splitted_topic[1]
#             variable = splitted_topic[2]
#             user_id = body.get("userId")
            
#             device_count = await devices_collection.count_documents({"dId": dId, "userId": user_id})
            
#             if device_count == 1:
#                 await data_collection.insert_one({
#                     "userId": user_id,
#                     "dId": dId,
#                     "variable": variable,
#                     "value": body.get("payload", {}).get("value"),
#                     "time": int(time.time() * 1000)
#                 })
#                 print("Data created")
                
#         return {"status": "success"}
#     except Exception as error:
#         print(error)
#         raise HTTPException(status_code=500, detail="Internal Server Error")


# @app.post("/alarm-webhook")
# async def alarm_webhook(request: Request, token: str = Header(None)):
#     if token != EMQX_API_TOKEN:
#         raise HTTPException(status_code=404, detail="Unauthorized")
    
#     try:
#         incoming_alarm = await request.json()
#         emqx_rule_id = incoming_alarm.get("emqxRuleId")
        
#         await alarm_rules_collection.update_one(
#             {"emqxRuleId": emqx_rule_id},
#             {"$inc": {"counter": 1}}
#         )
        
#         last_notif = await notifications_collection.find(
#             {"dId": incoming_alarm.get("dId"), "emqxRuleId": emqx_rule_id}
#         ).sort("time", -1).limit(1).to_list(length=1)
        
#         if not last_notif:
#             print("FIRST TIME ALARM")
#             new_notif = incoming_alarm.copy()
#             new_notif["time"] = int(time.time() * 1000)
#             new_notif["readed"] = False
#             await notifications_collection.insert_one(new_notif)
#         else:
#             last_time = last_notif[0].get("time", 0)
#             last_notif_to_now_mins = (int(time.time() * 1000) - last_time) / 1000 / 60
#             trigger_time = incoming_alarm.get("triggerTime", 0)
            
#             if last_notif_to_now_mins > trigger_time:
#                 print("TRIGGERED")
#                 new_notif = incoming_alarm.copy()
#                 new_notif["time"] = int(time.time() * 1000)
#                 new_notif["readed"] = False
#                 await notifications_collection.insert_one(new_notif)
                
#         return {"status": "success"}
#     except Exception as error:
#         print(error)
#         raise HTTPException(status_code=500, detail="Internal Server Error")

# @app.on_event("startup")
# async def startup_event():
#     asyncio.create_task(create_emqx_v5_webhook_rule())


# # @app.post("/saver-webhook")
# # @app.post("/api/saver-webhook")
# # async def saver_webhook(request: Request, token: Optional[str] = Header(None)):
# #     if token != EMQX_API_TOKEN:
# #         raise HTTPException(status_code=404)

# #     try:
# #         request_data = await request.json()
# #     except Exception:
# #         return {"status": "error", "message": "Invalid JSON"}

# #     topic = request_data.get("topic", "")
# #     data_user_id = request_data.get("userId")
# #     splitted_topic = topic.split("/")
    
# #     if len(splitted_topic) >= 3:
# #         user_id = splitted_topic[0] if not data_user_id else data_user_id
# #         dId = splitted_topic[1]
# #         variable = splitted_topic[2]
        
# #         device_found = await devices_collection.find_one({"dId": dId})
        
# #         if device_found:
# #             real_user_id = device_found.get("userId", user_id)
# #             payload_val = request_data.get("payload", {})
            
# #             current_value = 0.0
# #             try:
# #                 if isinstance(payload_val, dict):
# #                     current_value = float(payload_val.get("value", 0))
# #                 elif isinstance(payload_val, str):
# #                     parsed_payload = json.loads(payload_val)
# #                     current_value = float(parsed_payload.get("value", 0))
# #                 else:
# #                     current_value = float(payload_val)
# #             except Exception:
# #                 return {"status": "error", "message": "Invalid payload value"}

# #             last_record = await data_collection.find_one(
# #                 {"dId": dId, "variable": variable},
# #                 sort=[("time", -1)]
# #             )

# #             if last_record:
# #                 last_value = float(last_record.get("value", 0))
# #                 percent_change = abs((current_value - last_value) / last_value) * 100 if last_value != 0 else (100.0 if current_value != 0 else 0.0)
# #                 if percent_change < 0.05:
# #                     return {"status": "filtered", "message": "Variation below 0.05%"}

# #             timestamp = int(time.time() * 1000)
# #             await data_collection.insert_one({
# #                 "userId": real_user_id,
# #                 "dId": dId,
# #                 "variable": variable,
# #                 "value": current_value,
# #                 "time": timestamp,
# #                 "path": f"/{real_user_id}/{dId}/{timestamp}/"
# #             })
            
# #     return {"status": "success"}


# # @app.post("/alarm-webhook")
# # @app.post("/api/alarm-webhook")
# # async def alarm_webhook(request: Request, token: Optional[str] = Header(None)):
# #     if token and token != EMQX_API_TOKEN:
# #         raise HTTPException(status_code=403, detail="Unauthorized webhook")

# #     try:
# #         request_data = await request.json()
# #     except Exception:
# #         return {"status": "error", "message": "Invalid JSON"}

# #     topic = request_data.get("topic", "")
# #     userId = request_data.get("userId")
# #     payload = request_data.get("payload", {})
    
# #     current_value = 0.0
# #     try:
# #         if isinstance(payload, dict):
# #             current_value = float(payload.get("value", 0))
# #         elif isinstance(payload, str):
# #             parsed_payload = json.loads(payload)
# #             current_value = float(parsed_payload.get("value", 0))
# #         else:
# #             current_value = float(payload)
# #     except Exception:
# #         current_value = 0.0

# #     splitted_topic = topic.split("/")
# #     dId = splitted_topic[1] if len(splitted_topic) >= 2 else request_data.get("dId", "unknown")
# #     variable = splitted_topic[2] if len(splitted_topic) >= 3 else request_data.get("variable", "unknown")

# #     device_found = await devices_collection.find_one({"dId": dId})
# #     real_user_id = device_found.get("userId") if device_found else userId
# #     if not real_user_id:
# #         real_user_id = "unknown_user"

# #     alarm_rule = await alarm_rules_collection.find_one({"dId": dId, "variable": variable})
# #     variable_full_name = alarm_rule.get("variableFullName", variable) if alarm_rule else variable
# #     condition = alarm_rule.get("condition", "") if alarm_rule else ""
# #     target_value = alarm_rule.get("value", "") if alarm_rule else ""

# #     alert_message = f"Alerta en {variable_full_name}: valor actual {current_value} cumple condición {condition} {target_value}"
# #     timestamp = int(time.time() * 1000)

# #     notification_doc = {
# #         "userId": real_user_id,
# #         "dId": dId,
# #         "variable": variable,
# #         "variableFullName": variable_full_name,
# #         "value": current_value,
# #         "condition": condition,
# #         "targetValue": target_value,
# #         "message": alert_message,
# #         "leida": False,
# #         "time": timestamp
# #     }

# #     # Guardado seguro en MongoDB evitando duplicados de ID
# #     try:
# #         await notifications_collection.insert_one(notification_doc)
# #     except Exception:
# #         if "_id" in notification_doc:
# #             del notification_doc["_id"]
# #         try:
# #             await notifications_collection.insert_one(notification_doc)
# #         except Exception:
# #             pass

# #     # 🚀 PUBLICAR EN MQTT HACIA EMQX PARA QUE NUXT LO RECIBA EN TIEMPO REAL
# #     try:
# #         host = os.getenv("EMQX_API_HOST")
# #         if host:
# #             base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
# #             username = os.getenv("EMQX_DASHBOARD_USER", "admin")
# #             password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")
            
# #             async with httpx.AsyncClient(verify=False, timeout=5.0) as client_http:
# #                 login_res = await client_http.post(f"{base_url}/login", json={"username": username, "password": password})
# #                 if login_res.status_code == 200:
# #                     emqx_token = login_res.json().get("token") or login_res.json().get("access_token")
# #                     if emqx_token:
# #                         notif_topic = f"{real_user_id}/{dId}/{variable}/notif"
# #                         publish_payload = {
# #                             "topic": notif_topic,
# #                             "payload": json.dumps({
# #                                 "msg": alert_message,
# #                                 "variable": variable,
# #                                 "value": current_value,
# #                                 "time": timestamp
# #                             }),
# #                             "qos": 0,
# #                             "retain": False
# #                         }

# #                         headers = {"Authorization": f"Bearer {emqx_token}", "Content-Type": "application/json"}
# #                         await client_http.post(f"{base_url}/mqtt/publish", json=publish_payload, headers=headers)
# #     except Exception as pub_error:
# #         print(f"⚠️ [MQTT-PUBLISH-WARNING] No se pudo publicar la alarma en el broker: {pub_error}")

# #     print(f"🚨 [ALARM-TRIGGERED] Alerta registrada y publicada para el usuario {real_user_id} en dispositivo {dId}, variable {variable}")

# #     return {
# #         "status": "success", 
# #         "message": "Alarm processed, logged and published successfully",
# #         "alarm": {
# #             "active": True,
# #             "msg": alert_message,
# #             "variable": variable,
# #             "value": current_value
# #         }
# #     }



# # from datetime import datetime, timedelta, timezone
# # from fastapi import FastAPI, HTTPException, Header, status, Query, Request
# # from fastapi.middleware.cors import CORSMiddleware
# # from pydantic import BaseModel, EmailStr
# # from motor.motor_asyncio import AsyncIOMotorClient
# # from typing import Optional, Dict, Any
# # from bson import ObjectId
# # import bcrypt
# # from jose import jwt, JWTError
# # import time
# # import random
# # import os
# # import asyncio
# # import httpx
# # import json

# # app = FastAPI(title="IoT GL API", version="1.0.0")

# # SECRET_KEY = "clave-secreta-super-segura-iot-gl"
# # ALGORITHM = "HS256"
# # ACCESS_TOKEN_EXPIRE_MINUTES = 60
# # EMQX_API_TOKEN = "tu_token_secreto_emqx"

# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=["*"],
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# # MONGO_URL = os.getenv("MONGO_URI", "mongodb+srv://pigomezschneider_db_user:4WvulXjWAN5QBEBL@cluster0.rcvykvz.mongodb.net/?appName=Cluster0")

# # client = AsyncIOMotorClient(MONGO_URL)
# # database = client.iot_db

# # users_collection = database.get_collection("users")
# # devices_collection = database.get_collection("devices")
# # templates_collection = database.get_collection("templates")
# # data_collection = database.get_collection("data")
# # emqx_auth_rules_collection = database.get_collection("emqxauthrules")
# # notifications_collection = database.get_collection("notifications")
# # alarm_rules_collection = database.get_collection("alarmrules")
# # saver_rules_collection = database.get_collection("saverrules")

# # # Importar el módulo de rutas de alarmas separado
# # from api.routes import alarms

# # app.include_router(alarms.router)

# # string_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

# # def makeid(length: int = 10) -> str:
# #     return "".join(random.choice(string_chars) for _ in range(length))

# # def create_access_token(data: dict, expires_delta: timedelta | None = None):
# #     to_encode = data.copy()
# #     expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=15))
# #     to_encode.update({"exp": expire})
# #     return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# # def get_user_email_from_token(token: str = Header(None)):
# #     if not token:
# #         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token no proporcionado")
# #     try:
# #         if token.startswith("Bearer "):
# #             token = token.split(" ")[1]
# #         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
# #         email: str = payload.get("sub")
# #         if email is None:
# #             raise HTTPException(status_code=401, detail="Token inválido")
# #         return email
# #     except JWTError:
# #         raise HTTPException(status_code=401, detail="Token inválido o expirado")

# # async def create_emqx_v5_webhook_rule():
# #     host = os.getenv("EMQX_API_HOST")
# #     if not host:
# #         print("[EMQX-ERROR] La variable EMQX_API_HOST no está definida.")
# #         return
    
# #     base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
# #     username = os.getenv("EMQX_DASHBOARD_USER", "admin")
# #     password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")

# #     print(f"🔌 [EMQX-INIT] Intentando conectar a la API de EMQX en: {base_url}/login")

# #     max_retries = 3
# #     for attempt in range(1, max_retries + 1):
# #         try:
# #             async with httpx.AsyncClient(verify=False, timeout=30.0) as httpx_client:
# #                 login_res = await httpx_client.post(f"{base_url}/login", json={"username": username, "password": password})
# #                 print(f"🔑 [EMQX-LOGIN] Status code recibido: {login_res.status_code}")
                
# #                 if login_res.status_code != 200:
# #                     print(f"❌ [EMQX-ERROR] Login rechazado: {login_res.text}")
# #                     return
                
# #                 token = login_res.json().get("token") or login_res.json().get("access_token")
# #                 if not token:
# #                     print("❌ [EMQX-ERROR] Login OK pero no se encontró el token.")
# #                     return

# #                 headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# #                 public_api_url = os.getenv("PUBLIC_API_URL", "https://prueba-tzir.onrender.com")

# #                 # 1. Puente y regla para el guardado general de datos (/saver-webhook)
# #                 connector_payload = {
# #                     "name": "saver_webhook_bridge",
# #                     "type": "http",
# #                     "url": f"{public_api_url}/saver-webhook",
# #                     "method": "post",
# #                     "headers": {"token": EMQX_API_TOKEN},
# #                     "body": "${.}",
# #                     "connect_timeout": "15s",
# #                     "pool_size": 8
# #                 }

# #                 conn_res = await httpx_client.post(f"{base_url}/bridges", json=connector_payload, headers=headers)
# #                 if conn_res.status_code not in [200, 201]:
# #                     conn_res = await httpx_client.put(f"{base_url}/bridges/http:saver_webhook_bridge", json=connector_payload, headers=headers)
                
# #                 print(f"🌉 [EMQX-CONNECTOR-SAVER] Status: {conn_res.status_code} Resp: {conn_res.text}")
# #                 await asyncio.sleep(1)

# #                 rule_payload = {
# #                     "id": "saver_webhook_rule", 
# #                     "sql": "SELECT * FROM \"+/+/+/sdata\"", 
# #                     "actions": ["http:saver_webhook_bridge"]
# #                 }

# #                 rule_res = await httpx_client.post(f"{base_url}/rules", json=rule_payload, headers=headers)
# #                 if rule_res.status_code not in [200, 201]:
# #                     rule_res = await httpx_client.put(f"{base_url}/rules/saver_webhook_rule", json=rule_payload, headers=headers)

# #                 print(f"⚙️ [EMQX-RULE-SAVER] Status: {rule_res.status_code} Resp: {rule_res.text}")
# #                 await asyncio.sleep(1)

# #                 # 2. Puente dedicado para las alarmas (/alarm-webhook)
# #                 alarm_connector_payload = {
# #                     "name": "alarm_webhook_bridge",
# #                     "type": "http",
# #                     "url": f"{public_api_url}/alarm-webhook",
# #                     "method": "post",
# #                     "headers": {"token": EMQX_API_TOKEN},
# #                     "body": "${.}",
# #                     "connect_timeout": "15s",
# #                     "pool_size": 4
# #                 }

# #                 alarm_conn_res = await httpx_client.post(f"{base_url}/bridges", json=alarm_connector_payload, headers=headers)
# #                 if alarm_conn_res.status_code not in [200, 201]:
# #                     alarm_conn_res = await httpx_client.put(f"{base_url}/bridges/http:alarm_webhook_bridge", json=alarm_connector_payload, headers=headers)
                
# #                 print(f"🌉 [EMQX-CONNECTOR-ALARM] Status: {alarm_conn_res.status_code} Resp: {alarm_conn_res.text}")
# #                 print("✅ [EMQX-INIT] Todos los puentes y reglas base configurados con éxito.")
# #                 break
                
# #         except Exception as error:
# #             print(f"⚠️ [EMQX-WARNING] Intento {attempt}/{max_retries} fallido por {type(error).__name__} {error}")
# #             if attempt < max_retries:
# #                 print("⏳ Reintentando conexión con EMQX en 5 segundos...")
# #                 await asyncio.sleep(5)
# #             else:
# #                 print(f"❌ [EMQX-EXCEPTION CRITICAL] Se agotaron los reintentos.")

# # async def check_mqtt_superuser(db):
# #     try:
# #         collection = db["emqxauthrules"]
# #         superuser = await collection.find_one({"type": "superuser"})
# #         if superuser:
# #             return
# #         current_time = int(time.time() * 1000)
# #         await collection.insert_one({
# #             "publish": ["#"], "subscribe": ["#"], "userId": "emqxmqttsuperuser",
# #             "username": os.getenv("EMQX_NODE_SUPERUSER_USER", "admin"),
# #             "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "virus123"),
# #             "type": "superuser", "time": current_time, "updatedTime": current_time
# #         })
# #     except Exception as error:
# #         print("[DB-ERROR]", error)

# # async def init_emqx_background_tasks(db):
# #     try:
# #         await asyncio.sleep(3)
# #         await check_mqtt_superuser(db)
# #         await create_emqx_v5_webhook_rule()
# #     except Exception as e:
# #         print(f"❌ [CRITICAL BACKGROUND ERROR] {e}")

# # @app.on_event("startup")
# # async def startup_event():
# #     asyncio.create_task(init_emqx_background_tasks(database))

# # async def get_device_mqtt_credentials(dId: str, userId: str):
# #     try:
# #         rule = await emqx_auth_rules_collection.find_one({"type": "device", "userId": userId, "dId": dId})
# #         new_username, new_password, current_time = makeid(10), makeid(10), int(time.time() * 1000)

# #         if not rule:
# #             await emqx_auth_rules_collection.insert_one({
# #                 "userId": userId, "dId": dId, "username": new_username, "password": new_password,
# #                 "publish": [f"{userId}/{dId}/+/sdata"], "subscribe": [f"{userId}/{dId}/+/actdata"],
# #                 "type": "device", "time": current_time, "updatedTime": current_time
# #             })
# #             return {"username": new_username, "password": new_password}

# #         await emqx_auth_rules_collection.update_one(
# #             {"type": "device", "dId": dId},
# #             {"$set": {"username": new_username, "password": new_password, "updatedTime": current_time}}
# #         )
# #         return {"username": new_username, "password": new_password}
# #     except Exception:
# #         return False

# # class UserLogin(BaseModel):
# #     email: EmailStr
# #     password: str

# # class UserCreate(BaseModel):
# #     email: EmailStr
# #     password: str

# # class TemplateBody(BaseModel):
# #     template: dict

# # class DeviceCredentialsBody(BaseModel):
# #     dId: str
# #     password: str

# # class NotificationUpdateBody(BaseModel):
# #     notifId: str

# # @app.get("/")
# # async def read_root():
# #     return {"message": "API OK"}

# # @app.post("/login")
# # @app.post("/api/login")
# # async def login_user(user_data: UserLogin):
# #     user = await users_collection.find_one({"email": user_data.email})
# #     if not user or not bcrypt.checkpw(user_data.password.encode('utf-8'), user.get("password", "").encode('utf-8')):
# #         raise HTTPException(status_code=401, detail="Credenciales incorrectas")
# #     access_token = create_access_token(data={"sub": user["email"]}, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
# #     return {"status": "success", "access_token": access_token, "token_type": "bearer", "userData": {"_id": str(user.get("_id", "")), "name": user.get("email").split("@")[0], "email": user["email"]}}

# # @app.post("/register")
# # @app.post("/api/register")
# # async def register_user(user_data: UserCreate):
# #     if await users_collection.find_one({"email": user_data.email}):
# #         raise HTTPException(status_code=400, detail="Correo registrado")
# #     hashed = bcrypt.hashpw(user_data.password.encode('utf-8'), bcrypt.gensalt())
# #     await users_collection.insert_one({"email": user_data.email, "password": hashed.decode('utf-8')})
# #     return {"status": "success"}

# # @app.get("/device")
# # @app.get("/api/device")
# # async def get_devices(token: str = Header(None)):
# #     user_email = get_user_email_from_token(token)
# #     devices = []
# #     async for dev in devices_collection.find({"userId": user_email}):
# #         dev["_id"] = str(dev["_id"])
# #         devices.append(dev)
        
# #     saver_rules = []
# #     async for sr in saver_rules_collection.find({"userId": user_email}):
# #         sr["_id"] = str(sr["_id"])
# #         saver_rules.append(sr)
        
# #     templates = []
# #     async for t in templates_collection.find({"userId": user_email}):
# #         t["_id"] = str(t["_id"])
# #         templates.append(t)
        
# #     alarm_rules = []
# #     async for ar in alarm_rules_collection.find({"userId": user_email}):
# #         ar["_id"] = str(ar["_id"])
# #         alarm_rules.append(ar)

# #     for device in devices:
# #         sr = next((s for s in saver_rules if s.get("dId") == device.get("dId")), None)
# #         device["saverRule"] = {"_id": sr["_id"], "status": sr.get("status", True)} if sr else {"status": True}
        
# #         t = next((tp for tp in templates if str(tp.get("_id")) == str(device.get("templateId"))), None)
# #         if t:
# #             device["template"] = t
            
# #         device["alarmRules"] = [ar for ar in alarm_rules if ar.get("dId") == device.get("dId")]

# #     return {"status": "success", "data": devices}

# # @app.post("/device")
# # @app.post("/api/device")
# # async def add_device(request_body: dict, token: str = Header(None)):
# #     user_email = get_user_email_from_token(token)
# #     new_device = request_body.get("newDevice", {})
# #     dId = new_device.get("dId")
# #     await devices_collection.update_many({"userId": user_email}, {"$set": {"selected": False}})
# #     new_device["userId"] = user_email
# #     new_device["createdTime"] = int(time.time() * 1000)
# #     new_device["password"] = makeid(10)
# #     new_device["selected"] = True
# #     await saver_rules_collection.insert_one({"userId": user_email, "dId": dId, "status": True})
# #     await devices_collection.insert_one(new_device)
# #     return {"status": "success"}

# # @app.delete("/device")
# # @app.delete("/api/device")
# # async def delete_device(dId: str = Query(...), token: str = Header(None)):
# #     user_email = get_user_email_from_token(token)
# #     await saver_rules_collection.delete_many({"dId": dId})
# #     await emqx_auth_rules_collection.delete_many({"dId": dId, "type": "device"})
# #     result = await devices_collection.delete_one({"userId": user_email, "dId": dId})
# #     return {"status": "success", "data": result.deleted_count}

# # @app.put("/device")
# # @app.put("/api/device")
# # async def update_device_selector(body: dict, token: str = Header(None)):
# #     user_email = get_user_email_from_token(token)
# #     dId = body.get("dId")
# #     await devices_collection.update_many({"userId": user_email}, {"$set": {"selected": False}})
# #     await devices_collection.update_one({"userId": user_email, "dId": dId}, {"$set": {"selected": True}})
# #     return {"status": "success"}

# # @app.get("/template")
# # @app.get("/api/template")
# # async def get_templates(token: str = Header(None)):
# #     user_email = get_user_email_from_token(token)
# #     templates = []
# #     async for template in templates_collection.find({"userId": user_email}):
# #         template["_id"] = str(template["_id"])
# #         templates.append(template)
# #     return {"status": "success", "data": templates}

# # @app.post("/template")
# # @app.post("/api/template")
# # async def create_template(body: TemplateBody, token: str = Header(None)):
# #     user_email = get_user_email_from_token(token)
# #     new_template = body.template
# #     new_template["userId"] = user_email
# #     result = await templates_collection.insert_one(new_template)
# #     return {"status": "success", "id": str(result.inserted_id)}

# # @app.delete("/template")
# # @app.delete("/api/template")
# # async def delete_template(templateId: str = Query(...), token: str = Header(None)):
# #     user_email = get_user_email_from_token(token)
# #     await templates_collection.delete_one({"userId": user_email, "_id": ObjectId(templateId)})
# #     return {"status": "success"}

# # @app.get("/get-small-charts-data")
# # @app.get("/api/get-small-charts-data")
# # async def get_small_charts_data(
# #     chartTimeAgo: int = Query(...),
# #     dId: str = Query(...),
# #     variable: str = Query(...),
# #     token: str = Header(None)
# # ):
# #     user_email = get_user_email_from_token(token)
# #     try:
# #         time_ago_ms = int(time.time() * 1000) - (max(chartTimeAgo, 1440) * 60 * 1000)
# #         cursor = data_collection.find({
# #             "dId": dId,
# #             "variable": variable,
# #             "time": {"$gt": time_ago_ms}
# #         }).sort("time", 1)

# #         data = []
# #         async for doc in cursor:
# #             doc["_id"] = str(doc["_id"])
# #             data.append(doc)
# #         return data
# #     except Exception as error:
# #         print("[DB-ERROR]", error)
# #         return []

# # @app.post("/getdevicecredentials")
# # @app.post("/getdevicecredentials/")
# # @app.post("/api/getdevicecredentials")
# # @app.post("/api/getdevicecredentials/")
# # async def get_device_credentials(body: DeviceCredentialsBody):
# #     device = await devices_collection.find_one({"dId": body.dId})
# #     if not device:
# #         raise HTTPException(status_code=401)
# #     userId = device.get("userId")
# #     credentials = await get_device_mqtt_credentials(body.dId, userId)
# #     template = await templates_collection.find_one({"_id": ObjectId(device["templateId"])}) if "templateId" in device else None
    
# #     variables = []
# #     if template and "widgets" in template:
# #         for widget in template["widgets"]:
# #             variables.append({
# #                 "variable": widget.get("variable"),
# #                 "variableFullName": widget.get("variableFullName"),
# #                 "variableType": widget.get("variableType"),
# #                 "variableSendFreq": widget.get("variableSendFreq")
# #             })

# #     return {
# #         "username": credentials["username"],
# #         "password": credentials["password"],
# #         "topic": f"{userId}/{body.dId}/",
# #         "variables": variables
# #     }

# # @app.post("/getmqttcredentialsforreconnection")
# # @app.post("/api/getmqttcredentialsforreconnection")
# # async def get_mqtt_credentials_for_reconnection(token: str = Header(None)):
# #     return {
# #         "status": "success", 
# #         "username": os.getenv("EMQX_NODE_SUPERUSER_USER", "admin"),
# #         "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "public")
# #     }

# # @app.post("/saver-webhook")
# # @app.post("/api/saver-webhook")
# # async def saver_webhook(request: Request, token: Optional[str] = Header(None)):
# #     if token != EMQX_API_TOKEN:
# #         raise HTTPException(status_code=404)

# #     try:
# #         request_data = await request.json()
# #     except Exception:
# #         return {"status": "error", "message": "Invalid JSON"}

# #     topic = request_data.get("topic", "")
# #     data_user_id = request_data.get("userId")
# #     splitted_topic = topic.split("/")
    
# #     if len(splitted_topic) >= 3:
# #         user_id = splitted_topic[0] if not data_user_id else data_user_id
# #         dId = splitted_topic[1]
# #         variable = splitted_topic[2]
        
# #         device_found = await devices_collection.find_one({"dId": dId})
        
# #         if device_found:
# #             real_user_id = device_found.get("userId", user_id)
# #             payload_val = request_data.get("payload", {})
            
# #             current_value = 0.0
# #             try:
# #                 if isinstance(payload_val, dict):
# #                     current_value = float(payload_val.get("value", 0))
# #                 elif isinstance(payload_val, str):
# #                     parsed_payload = json.loads(payload_val)
# #                     current_value = float(parsed_payload.get("value", 0))
# #                 else:
# #                     current_value = float(payload_val)
# #             except Exception:
# #                 return {"status": "error", "message": "Invalid payload value"}

# #             last_record = await data_collection.find_one(
# #                 {"dId": dId, "variable": variable},
# #                 sort=[("time", -1)]
# #             )

# #             if last_record:
# #                 last_value = float(last_record.get("value", 0))
# #                 percent_change = abs((current_value - last_value) / last_value) * 100 if last_value != 0 else (100.0 if current_value != 0 else 0.0)
# #                 if percent_change < 0.05:
# #                     return {"status": "filtered", "message": "Variation below 0.05%"}

# #             timestamp = int(time.time() * 1000)
# #             await data_collection.insert_one({
# #                 "userId": real_user_id,
# #                 "dId": dId,
# #                 "variable": variable,
# #                 "value": current_value,
# #                 "time": timestamp,
# #                 "path": f"/{real_user_id}/{dId}/{timestamp}/"
# #             })
            
# #     return {"status": "success"}


# # @app.post("/alarm-webhook")
# # @app.post("/api/alarm-webhook")
# # async def alarm_webhook(request: Request, token: Optional[str] = Header(None)):
# #     if token and token != EMQX_API_TOKEN:
# #         raise HTTPException(status_code=403, detail="Unauthorized webhook")

# #     try:
# #         request_data = await request.json()
# #     except Exception:
# #         return {"status": "error", "message": "Invalid JSON"}

# #     topic = request_data.get("topic", "")
# #     userId = request_data.get("userId")
# #     payload = request_data.get("payload", {})
    
# #     current_value = 0.0
# #     try:
# #         if isinstance(payload, dict):
# #             current_value = float(payload.get("value", 0))
# #         elif isinstance(payload, str):
# #             parsed_payload = json.loads(payload)
# #             current_value = float(parsed_payload.get("value", 0))
# #         else:
# #             current_value = float(payload)
# #     except Exception:
# #         current_value = 0.0

# #     splitted_topic = topic.split("/")
# #     dId = splitted_topic[1] if len(splitted_topic) >= 2 else request_data.get("dId", "unknown")
# #     variable = splitted_topic[2] if len(splitted_topic) >= 3 else request_data.get("variable", "unknown")

# #     device_found = await devices_collection.find_one({"dId": dId})
# #     real_user_id = device_found.get("userId") if device_found else userId
# #     if not real_user_id:
# #         real_user_id = "unknown_user"

# #     alarm_rule = await alarm_rules_collection.find_one({"dId": dId, "variable": variable})
# #     variable_full_name = alarm_rule.get("variableFullName", variable) if alarm_rule else variable
# #     condition = alarm_rule.get("condition", "") if alarm_rule else ""
# #     target_value = alarm_rule.get("value", "") if alarm_rule else ""

# #     alert_message = f"Alerta en {variable_full_name}: valor actual {current_value} cumple condición {condition} {target_value}"
# #     timestamp = int(time.time() * 1000)

# #     notification_doc = {
# #         "userId": real_user_id,
# #         "dId": dId,
# #         "variable": variable,
# #         "variableFullName": variable_full_name,
# #         "value": current_value,
# #         "condition": condition,
# #         "targetValue": target_value,
# #         "message": alert_message,
# #         "leida": False,
# #         "time": timestamp
# #     }

# #     # Guardado seguro en MongoDB evitando duplicados de ID
# #     try:
# #         await notifications_collection.insert_one(notification_doc)
# #     except Exception:
# #         if "_id" in notification_doc:
# #             del notification_doc["_id"]
# #         try:
# #             await notifications_collection.insert_one(notification_doc)
# #         except Exception:
# #             pass

# #     # 🚀 PUBLICAR EN MQTT HACIA EMQX PARA QUE NUXT LO RECIBA EN TIEMPO REAL
# #     try:
# #         host = os.getenv("EMQX_API_HOST")
# #         if host:
# #             base_url = f"{host}/api/v5" if host.startswith("http://") or host.startswith("https://") else f"http://{host}/api/v5"
# #             username = os.getenv("EMQX_DASHBOARD_USER", "admin")
# #             password = os.getenv("EMQX_DASHBOARD_PASSWORD", "virus123")
            
# #             async with httpx.AsyncClient(verify=False, timeout=5.0) as client_http:
# #                 login_res = await client_http.post(f"{base_url}/login", json={"username": username, "password": password})
# #                 if login_res.status_code == 200:
# #                     emqx_token = login_res.json().get("token") or login_res.json().get("access_token")
# #                     if emqx_token:
# #                         notif_topic = f"{real_user_id}/{dId}/{variable}/notif"
# #                         publish_payload = {
# #                             "topic": notif_topic,
# #                             "payload": json.dumps({
# #                                 "msg": alert_message,
# #                                 "variable": variable,
# #                                 "value": current_value,
# #                                 "time": timestamp
# #                             }),
# #                             "qos": 0,
# #                             "retain": False
# #                         }

# #                         headers = {"Authorization": f"Bearer {emqx_token}", "Content-Type": "application/json"}
# #                         await client_http.post(f"{base_url}/publish", json=publish_payload, headers=headers)
# #                         # headers = {"Authorization": f"Bearer {emqx_token}", "Content-Type": "application/json"}
# #                         # await client_http.post(f"{base_url}/mqtt/publish", json=publish_payload, headers=headers)
# #     except Exception as pub_error:
# #         print(f"⚠️ [MQTT-PUBLISH-WARNING] No se pudo publicar la alarma en el broker: {pub_error}")

# #     print(f"🚨 [ALARM-TRIGGERED] Alerta registrada y publicada para el usuario {real_user_id} en dispositivo {dId}, variable {variable}")

# #     return {
# #         "status": "success", 
# #         "message": "Alarm processed, logged and published successfully",
# #         "alarm": {
# #             "active": True,
# #             "msg": alert_message,
# #             "variable": variable,
# #             "value": current_value
# #         }
# #     }



# # # @app.post("/alarm-webhook")
# # # @app.post("/api/alarm-webhook")
# # # async def alarm_webhook(request: Request, token: Optional[str] = Header(None)):
# # #     if token and token != EMQX_API_TOKEN:
# # #         raise HTTPException(status_code=403, detail="Unauthorized webhook")

# # #     try:
# # #         request_data = await request.json()
# # #     except Exception:
# # #         return {"status": "error", "message": "Invalid JSON"}

# # #     topic = request_data.get("topic", "")
# # #     userId = request_data.get("userId")
# # #     payload = request_data.get("payload", {})
    
# # #     current_value = 0.0
# # #     try:
# # #         if isinstance(payload, dict):
# # #             current_value = float(payload.get("value", 0))
# # #         elif isinstance(payload, str):
# # #             parsed_payload = json.loads(payload)
# # #             current_value = float(parsed_payload.get("value", 0))
# # #         else:
# # #             current_value = float(payload)
# # #     except Exception:
# # #         current_value = 0.0

# # #     splitted_topic = topic.split("/")
# # #     dId = splitted_topic[1] if len(splitted_topic) >= 2 else request_data.get("dId", "unknown")
# # #     variable = splitted_topic[2] if len(splitted_topic) >= 3 else request_data.get("variable", "unknown")

# # #     device_found = await devices_collection.find_one({"dId": dId})
# # #     real_user_id = device_found.get("userId") if device_found else userId
# # #     if not real_user_id:
# # #         real_user_id = "unknown_user"

# # #     alarm_rule = await alarm_rules_collection.find_one({"dId": dId, "variable": variable})
# # #     variable_full_name = alarm_rule.get("variableFullName", variable) if alarm_rule else variable
# # #     condition = alarm_rule.get("condition", "") if alarm_rule else ""
# # #     target_value = alarm_rule.get("value", "") if alarm_rule else ""

# # #     alert_message = f"Alerta en {variable_full_name}: valor actual {current_value} cumple condición {condition} {target_value}"
# # #     timestamp = int(time.time() * 1000)

# # #     notification_doc = {
# # #         "userId": real_user_id,
# # #         "dId": dId,
# # #         "variable": variable,
# # #         "variableFullName": variable_full_name,
# # #         "value": current_value,
# # #         "condition": condition,
# # #         "targetValue": target_value,
# # #         "message": alert_message,
# # #         "leida": False,
# # #         "time": timestamp
# # #     }

# # #     # Guardado seguro en MongoDB evitando duplicados de ID
# # #     try:
# # #         await notifications_collection.insert_one(notification_doc)
# # #     except Exception:
# # #         if "_id" in notification_doc:
# # #             del notification_doc["_id"]
# # #         try:
# # #             await notifications_collection.insert_one(notification_doc)
# # #         except Exception:
# # #             pass

# # #     print(f"🚨 [ALARM-TRIGGERED] Alerta registrada para el usuario {real_user_id} en dispositivo {dId}, variable {variable}")

# #     return {
# #         "status": "success", 
# #         "message": "Alarm processed and logged successfully",
# #         "alarm": {
# #             "active": True,
# #             "msg": alert_message,
# #             "variable": variable,
# #             "value": current_value
# #         }
# #     }

# # # @app.post("/alarm-webhook")
# # # @app.post("/api/alarm-webhook")
# # # async def alarm_webhook(request: Request, token: Optional[str] = Header(None)):
# # #     if token and token != EMQX_API_TOKEN:
# # #         raise HTTPException(status_code=403, detail="Unauthorized webhook")

# # #     try:
# # #         request_data = await request.json()
# # #     except Exception:
# # #         return {"status": "error", "message": "Invalid JSON"}

# # #     topic = request_data.get("topic", "")
# # #     userId = request_data.get("userId")
# # #     payload = request_data.get("payload", {})
    
# # #     current_value = 0.0
# # #     try:
# # #         if isinstance(payload, dict):
# # #             current_value = float(payload.get("value", 0))
# # #         elif isinstance(payload, str):
# # #             parsed_payload = json.loads(payload)
# # #             current_value = float(parsed_payload.get("value", 0))
# # #         else:
# # #             current_value = float(payload)
# # #     except Exception:
# # #         current_value = 0.0

# # #     splitted_topic = topic.split("/")
# # #     dId = splitted_topic[1] if len(splitted_topic) >= 2 else request_data.get("dId", "unknown")
# # #     variable = splitted_topic[2] if len(splitted_topic) >= 3 else request_data.get("variable", "unknown")

# # #     # 1. Buscar el dispositivo para heredar su userId real si viene vacío en el webhook
# # #     device_found = await devices_collection.find_one({"dId": dId})
# # #     real_user_id = device_found.get("userId") if device_found else userId

# # #     # Si por alguna razón sigue sin encontrarse el usuario, evitamos fallar
# # #     if not real_user_id:
# # #         real_user_id = "unknown_user"

# # #     # 2. Buscar la regla de alarma asociada para traer los detalles de condición y umbral
# # #     alarm_rule = await alarm_rules_collection.find_one({"dId": dId, "variable": variable})
    
# # #     variable_full_name = alarm_rule.get("variableFullName", variable) if alarm_rule else variable
# # #     condition = alarm_rule.get("condition", "") if alarm_rule else ""
# # #     target_value = alarm_rule.get("value", "") if alarm_rule else ""

# # #     alert_message = f"Alerta en {variable_full_name}: valor actual {current_value} cumple condición {condition} {target_value}"

# # #     timestamp = int(time.time() * 1000)
# # #     notification_doc = {
# # #         "userId": real_user_id,
# # #         "dId": dId,
# # #         "variable": variable,
# # #         "variableFullName": variable_full_name,
# # #         "value": current_value,
# # #         "condition": condition,
# # #         "targetValue": target_value,
# # #         "message": alert_message,
# # #         "leida": False,
# # #         "time": timestamp
# # #     }

# # #     # Guardar en MongoDB (Historial)
# # #     await notifications_collection.insert_one(notification_doc)
# # #     print(f"🚨 [ALARM-TRIGGERED] Alerta registrada para el usuario {real_user_id} en dispositivo {dId}, variable {variable}")

# # #     # 3. PUBLICAR EN TIEMPO REAL A EMQX PARA QUE NUXT MUESTRE EL CARTEL AL INSTANTE
# # #     # Tópico dinámico universal: userId/dId/variable/notif (coincide exactamente con el comodín del frontend)
# # #     notif_topic = f"{real_user_id}/{dId}/{variable}/notif"
    
# # #     mqtt_payload = {
# # #         "active": True,
# # #         "msg": alert_message,
# # #         "type": "danger",
# # #         "value": current_value,
# # #         "timestamp": timestamp
# # #     }

# # #     try:
# # #         # Usamos el superusuario o credenciales internas de EMQX para inyectar el mensaje al broker
# # #         mqtt_host = os.getenv("EMQX_BROKER_HOST", "localhost")
# # #         publish.single(
# # #             topic=notif_topic,
# # #             payload=json.dumps(mqtt_payload),
# # #             hostname=mqtt_host,
# # #             port=1883,
# # #             auth={
# # #                 "username": os.getenv("EMQX_NODE_SUPERUSER_USER", "admin"),
# # #                 "password": os.getenv("EMQX_NODE_SUPERUSER_PASSWORD", "virus123")
# # #             }
# # #         )
# # #         print(f"📡 [MQTT-PUBLISHED] Notificación en tiempo real enviada al tópico: {notif_topic}")
# # #     except Exception as e:
# # #         print(f"❌ [MQTT-PUBLISH-ERROR] No se pudo publicar la alerta en MQTT: {e}")

# # #     return {"status": "success", "message": "Alarm processed successfully"}

# # # @app.post("/alarm-webhook")
# # # @app.post("/api/alarm-webhook")
# # # async def alarm_webhook(request: Request, token: Optional[str] = Header(None)):
# # #     if token and token != EMQX_API_TOKEN:
# # #         raise HTTPException(status_code=403, detail="Unauthorized webhook")

# # #     try:
# # #         request_data = await request.json()
# # #     except Exception:
# # #         return {"status": "error", "message": "Invalid JSON"}

# # #     topic = request_data.get("topic", "")
# # #     userId = request_data.get("userId")
# # #     payload = request_data.get("payload", {})
    
# # #     current_value = 0.0
# # #     try:
# # #         if isinstance(payload, dict):
# # #             current_value = float(payload.get("value", 0))
# # #         elif isinstance(payload, str):
# # #             parsed_payload = json.loads(payload)
# # #             current_value = float(parsed_payload.get("value", 0))
# # #         else:
# # #             current_value = float(payload)
# # #     except Exception:
# # #         current_value = 0.0

# # #     splitted_topic = topic.split("/")
# # #     dId = splitted_topic[1] if len(splitted_topic) >= 2 else request_data.get("dId", "unknown")
# # #     variable = splitted_topic[2] if len(splitted_topic) >= 3 else request_data.get("variable", "unknown")

# # #     # 1. Buscar el dispositivo para heredar su userId real si viene vacío en el webhook
# # #     device_found = await devices_collection.find_one({"dId": dId})
# # #     real_user_id = device_found.get("userId") if device_found else userId

# # #     # 2. Buscar la regla de alarma asociada para traer los detalles de condición y umbral
# # #     alarm_rule = await alarm_rules_collection.find_one({"dId": dId, "variable": variable})
    
# # #     variable_full_name = alarm_rule.get("variableFullName", variable) if alarm_rule else variable
# # #     condition = alarm_rule.get("condition", "") if alarm_rule else ""
# # #     target_value = alarm_rule.get("value", "") if alarm_rule else ""

# # #     timestamp = int(time.time() * 1000)
# # #     notification_doc = {
# # #         "userId": real_user_id,
# # #         "dId": dId,
# # #         "variable": variable,
# # #         "variableFullName": variable_full_name,
# # #         "value": current_value,
# # #         "condition": condition,
# # #         "targetValue": target_value,
# # #         "message": f"Alerta en {variable_full_name}: valor actual {current_value} cumple condición {condition} {target_value}",
# # #         "leida": False,
# # #         "time": timestamp
# # #     }

# # #     await notifications_collection.insert_one(notification_doc)
# # #     print(f"🚨 [ALARM-TRIGGERED] Alerta registrada para el usuario {real_user_id} en dispositivo {dId}, variable {variable}")

# # #     return {"status": "success", "message": "Alarm processed successfully"}


# # # @app.post("/alarm-webhook")
# # # @app.post("/api/alarm-webhook")
# # # async def alarm_webhook(request: Request, token: Optional[str] = Header(None)):
# # #     if token and token != EMQX_API_TOKEN:
# # #         raise HTTPException(status_code=403, detail="Unauthorized webhook")

# # #     try:
# # #         request_data = await request.json()
# # #     except Exception:
# # #         return {"status": "error", "message": "Invalid JSON"}

# # #     topic = request_data.get("topic", "")
# # #     userId = request_data.get("userId")
# # #     payload = request_data.get("payload", {})
    
# # #     current_value = 0.0
# # #     try:
# # #         if isinstance(payload, dict):
# # #             current_value = float(payload.get("value", 0))
# # #         elif isinstance(payload, str):
# # #             parsed_payload = json.loads(payload)
# # #             current_value = float(parsed_payload.get("value", 0))
# # #         else:
# # #             current_value = float(payload)
# # #     except Exception:
# # #         current_value = 0.0

# # #     splitted_topic = topic.split("/")
# # #     dId = splitted_topic[1] if len(splitted_topic) >= 2 else request_data.get("dId", "unknown")
# # #     variable = splitted_topic[2] if len(splitted_topic) >= 3 else request_data.get("variable", "unknown")

# # #     alarm_rule = await alarm_rules_collection.find_one({"dId": dId, "variable": variable, "userId": userId})
    
# # #     variable_full_name = alarm_rule.get("variableFullName", variable) if alarm_rule else variable
# # #     condition = alarm_rule.get("condition", "") if alarm_rule else ""
# # #     target_value = alarm_rule.get("value", "") if alarm_rule else ""

# # #     timestamp = int(time.time() * 1000)
# # #     notification_doc = {
# # #         "userId": userId,
# # #         "dId": dId,
# # #         "variable": variable,
# # #         "variableFullName": variable_full_name,
# # #         "value": current_value,
# # #         "condition": condition,
# # #         "targetValue": target_value,
# # #         "message": f"Alerta en {variable_full_name}: valor actual {current_value} cumple condición {condition} {target_value}",
# # #         "leida": False,
# # #         "time": timestamp
# # #     }

# # #     await notifications_collection.insert_one(notification_doc)
# # #     print(f"🚨 [ALARM-TRIGGERED] Alerta registrada para el dispositivo {dId} en la variable {variable}")

# # #     return {"status": "success", "message": "Alarm processed successfully"}

# # @app.get("/notifications")
# # @app.get("/api/notifications")
# # async def get_notifications(token: str = Header(None)):
# #     user_email = get_user_email_from_token(token)
# #     notifications = []
# #     async for notif in notifications_collection.find({"userId": user_email}).sort("time", -1).limit(50):
# #         notif["_id"] = str(notif["_id"])
# #         notifications.append(notif)
# #     return {"status": "success", "data": notifications}

# # @app.put("/notifications")
# # @app.put("/api/notifications")
# # async def update_notification(body: NotificationUpdateBody, token: str = Header(None)):
# #     return {"status": "success"}

# # @app.post("/getmqttcredentials")
# # @app.post("/api/getmqttcredentials")
# # async def get_mqtt_credentials(token: str = Header(None)):
# #     return {"status": "success", "username": "admin", "password": "public"}
