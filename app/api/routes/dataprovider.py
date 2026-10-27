from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
import time
from datetime import datetime

# Asegúrate de importar tu dependencia de autenticación y el modelo de base de datos de FastAPI
# from ..middlewares.authentication import check_auth
# from ..models.data import Data

router = APIRouter()

@router.get("/get-last-data")
async def get_last_data(
    chartTimeAgo: int = Query(...),
    dId: str = Query(...),
    variable: str = Query(...),
    # user = Depends(check_auth)  # Descomenta cuando tengas tu dependencia de token
):
    try:
        # Simulando el ID de usuario obtenido del token (ajustar según tu auth de FastAPI)
        user_id = "p.i.gomezschneider@gmail.com" # user.get("_id")
        
        time_ago_ms = int(time.time() * 1000) - (chartTimeAgo * 60 * 1000)

        # Consulta a MongoDB con Motor / ODM correspondiente
        # data = await Data.find({"userId": user_id, "dId": dId, "variable": variable}).sort("time", -1).limit(1).to_list(1)
        data = [] # Reemplazar con el resultado de tu BD

        return {
            "status": "success",
            "data": data
        }

    except Exception as error:
        print(error)
        return {
            "status": "error",
            "error": str(error)
        }


@router.get("/get-small-charts-data")
async def get_small_charts_data(
    chartTimeAgo: int = Query(...),
    dId: str = Query(...),
    variable: str = Query(...),
    # user = Depends(check_auth)  # Descomenta cuando tengas tu dependencia de token
):
    try:
        # Simulando el ID de usuario obtenido del token
        user_id = "p.i.gomezschneider@gmail.com" # user.get("_id")
        
        time_ago_ms = int(time.time() * 1000) - (chartTimeAgo * 60 * 1000)

        # Consulta a MongoDB filtrando por rango de tiempo
        # data = await Data.find({"userId": user_id, "dId": dId, "variable": variable, "time": {"$gt": time_ago_ms}}).sort("time", 1).to_list(None)
        data = [] # Reemplazar con el resultado de tu BD

        return {
            "status": "success",
            "data": data
        }

    except Exception as error:
        print(error)
        return {
            "status": "error",
            "error": str(error)
        }