from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def get_users():
    return [
        {"id": 1, "name": "Paulo Gomez", "role": "Admin"},
        {"id": 2, "name": "Test User", "role": "Developer"}
    ]