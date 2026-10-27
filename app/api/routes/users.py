import random
import string
import time
from fastapi import APIRouter, Depends, HTTPException, status
import bcrypt
import jwt
from pydantic import BaseModel, EmailStr

# Importar la conexión a las colecciones de MongoDB
from backend.app.database import users_collection, emqx_collection
from backend.app.api.middlewares.authentication import check_auth

router = APIRouter()
SECRET_KEY = "securePasswordHere"


class LoginSchema(BaseModel):
    email: EmailStr
    password: str


class RegisterSchema(BaseModel):
    name: str
    email: EmailStr
    password: str


# --- LOGIN REAL CON MONGODB ---
@router.post("/login")
async def login(credentials: LoginSchema):
    try:
        # Buscar usuario en MongoDB
        user = await users_collection.find_one({"email": credentials.email})

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"status": "error", "error": "Invalid Credentials"},
            )

        # Verificar contraseña con bcrypt
        if bcrypt.checkpw(
            credentials.password.encode("utf-8"), user["password"].encode("utf-8")
        ):
            # Preparar datos del usuario para el token (convertir ObjectId a string)
            user_dict = dict(user)
            user_dict["_id"] = str(user_dict["_id"])
            user_dict.pop("password", None)  # Ocultar contraseña

            # Generar token JWT (expira en 30 días)
            token_payload = {"userData": user_dict}
            token = jwt.encode(token_payload, SECRET_KEY, algorithm="HS256")

            return {
                "status": "success",
                "token": token,
                "userData": user_dict,
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={"status": "error", "error": "Invalid Credentials"},
            )

    except HTTPException as he:
        raise he
    except Exception as error:
        print(error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error", "error": str(error)},
        )


# --- REGISTER REAL CON MONGODB ---
@router.post("/register")
async def register(credentials: RegisterSchema):
    try:
        # Verificar si el email ya existe
        existing_user = await users_collection.find_one({"email": credentials.email})
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"status": "error", "error": "Error, email already exists."},
            )

        # Hashear contraseña
        hashed_password = bcrypt.hashpw(
            credentials.password.encode("utf-8"), bcrypt.gensalt(rounds=10)
        ).decode("utf-8")

        new_user = {
            "name": credentials.name,
            "email": credentials.email,
            "password": hashed_password,
        }

        await users_collection.insert_one(new_user)

        return {"status": "success"}

    except HTTPException as he:
        raise he
    except Exception as error:
        print("ERROR - REGISTER ENDPOINT")
        print(error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error", "error": str(error)},
        )


# --- MQTT CREDENTIALS ---
@router.post("/getmqttcredentials")
async def get_mqtt_credentials(user_data=Depends(check_auth)):
    try:
        user_id = user_data.get("_id") or user_data.get("id")
        credentials = await get_web_user_mqtt_credentials(user_id)

        return {
            "status": "success",
            "username": credentials["username"],
            "password": credentials["password"],
        }
    except Exception as error:
        print(error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error"},
        )


@router.post("/getmqttcredentialsforreconnection")
async def get_mqtt_credentials_for_reconnection(user_data=Depends(check_auth)):
    try:
        user_id = user_data.get("_id") or user_data.get("id")
        credentials = await get_web_user_mqtt_credentials_for_reconnection(user_id)

        return {
            "status": "success",
            "username": credentials["username"],
            "password": credentials["password"],
        }
    except Exception as error:
        print(error)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"status": "error"},
        )


# --- FUNCIONES AUXILIARES DE MQTT ---
async def get_web_user_mqtt_credentials(user_id):
    rule = await emqx_collection.find_one({"type": "user", "userId": user_id})

    if not rule:
        new_rule = {
            "userId": user_id,
            "username": makeid(10),
            "password": makeid(10),
            "publish": [f"{user_id}/#"],
            "subscribe": [f"{user_id}/#"],
            "type": "user",
            "time": int(time.time() * 1000),
            "updatedTime": int(time.time() * 1000),
        }
        await emqx_collection.insert_one(new_rule)
        return {"username": new_rule["username"], "password": new_rule["password"]}

    new_username = makeid(10)
    new_password = makeid(10)

    await emqx_collection.update_one(
        {"type": "user", "userId": user_id},
        {
            "$set": {
                "username": new_username,
                "password": new_password,
                "updatedTime": int(time.time() * 1000),
            }
        },
    )

    return {"username": new_username, "password": new_password}


async def get_web_user_mqtt_credentials_for_reconnection(user_id):
    rule = await emqx_collection.find_one({"type": "user", "userId": user_id})
    if rule:
        return {"username": rule["username"], "password": rule["password"]}
    return {"username": "", "password": ""}


def makeid(length: int) -> str:
    characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choice(characters) for _ in range(length))