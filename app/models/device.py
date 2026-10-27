from pydantic import BaseModel
from typing import Optional

# --- MODELO PYDANTIC PARA DEVICE ---
class DeviceSchema(BaseModel):
    userId: str
    dId: str
    name: str
    password: str
    selected: bool = False
    templateId: str
    templateName: str
    createdTime: Optional[int] = None

# Si el frontend envía los datos anidados bajo la propiedad 'newDevice' (como en Nuxt):
class DeviceBody(BaseModel):
    newDevice: DeviceSchema