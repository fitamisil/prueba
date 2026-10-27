from fastapi import FastAPI, HTTPException, Header, status, Query
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import time

# --- MODELOS DE ENTRADA ---
class TemplateBody(BaseModel):
    template: dict

# --- ENDPOINTS DE PLANTILLAS ---

@app.get("/template", status_code=status.HTTP_200_OK)
@app.get("/api/template", status_code=status.HTTP_200_OK)
async def get_templates(token: str = Header(None)):
    """Obtiene todas las plantillas del usuario autenticado."""
    try:
        # Aquí extraes el ID del usuario según tu validación de token JWT actual
        userId = "ID_DEL_USUARIO_EXTRAIDO_DEL_TOKEN" 

        templates = []
        async for template in templates_collection.find({"userId": userId}):
            template["_id"] = str(template["_id"])  # Convertir ObjectId a string
            templates.append(template)

        return {
            "status": "success",
            "data": templates
        }

    except Exception as error:
        print(error)
        return {
            "status": "error",
            "error": str(error)
        }


@app.post("/template", status_code=status.HTTP_200_OK)
@app.post("/api/template", status_code=status.HTTP_200_OK)
async def create_template(body: TemplateBody, token: str = Header(None)):
    """Crea una nueva plantilla para el usuario."""
    try:
        userId = "ID_DEL_USUARIO_EXTRAIDO_DEL_TOKEN"

        new_template = body.template
        new_template["userId"] = userId
        new_template["createdTime"] = int(time.time() * 1000)  # Equivalente a Date.now() en ms

        await templates_collection.insert_one(new_template)

        return {
            "status": "success"
        }

    except Exception as error:
        print(error)
        return {
            "status": "error",
            "error": str(error)
        }


@app.delete("/template", status_code=status.HTTP_200_OK)
@app.delete("/api/template", status_code=status.HTTP_200_OK)
async def delete_template(templateId: str = Query(...), token: str = Header(None)):
    """Elimina una plantilla si no está siendo utilizada por ningún dispositivo."""
    try:
        userId = "ID_DEL_USUARIO_EXTRAIDO_DEL_TOKEN"

        # Verificar si hay dispositivos usando esta plantilla
        devices_count = await devices_collection.count_documents({
            "userId": userId,
            "templateId": templateId
        })

        if devices_count > 0:
            return {
                "status": "fail",
                "error": "template in use"
            }

        # Eliminar la plantilla
        result = await templates_collection.delete_one({
            "userId": userId,
            "_id": ObjectId(templateId)
        })

        return {
            "status": "success"
        }

    except Exception as error:
        print(error)
        return {
            "status": "error",
            "error": str(error)
        }