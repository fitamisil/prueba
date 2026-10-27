from motor.motor_asyncio import AsyncIOMotorClient

# Conexión al MongoDB local (sin necesidad de internet ni contraseñas complejas)
MONGO_DETAILS = "mongodb://localhost:27017"

client = AsyncIOMotorClient(MONGO_DETAILS)

# Base de datos exclusiva para este proyecto
database = client["iot_db"]

# Colecciones exclusivas
users_collection = database.get_collection("usuarios")
emqx_collection = database.get_collection("emqxauthrules")