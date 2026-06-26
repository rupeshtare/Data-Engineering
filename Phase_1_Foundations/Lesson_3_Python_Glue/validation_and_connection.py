# validation_and_connection.py
# Purpose: Advanced Python patterns for Data Validation (Pydantic) and 
# Resource Management (Context Managers).

from pydantic import BaseModel, EmailStr, Field, validator
from typing import List, Optional
from contextlib import contextmanager

# 1. DATA VALIDATION (The "Data Contract")
class UserRecord(BaseModel):
    user_id: int
    username: str = Field(..., min_length=3)
    email: EmailStr
    age: Optional[int] = None
    tags: List[str] = []

    @validator('age')
    def age_must_be_positive(cls, v):
        if v is not None and v <= 0:
            raise ValueError('Age must be a positive integer')
        return v

# 2. CONTEXT MANAGER (Safe Resource Handling)
@contextmanager
def mock_db_connection(db_name: str):
    print(f"--- [CONNECTING] to {db_name} ---")
    try:
        # Yielding a dummy "connection" object
        yield {"status": "connected", "db": db_name}
    finally:
        print(f"--- [DISCONNECTING] from {db_name} ---")

# 🏛️ Architect's Tip:
# "Always validate data at the gate. Pydantic ensures that bad data 
# never enters your expensive Spark or Snowflake pipelines."

if __name__ == "__main__":
    # Example 1: Validating a record
    raw_input = {
        "user_id": 101,
        "username": "alex_de",
        "email": "alex@example.com",
        "age": 28,
        "tags": ["engineer", "architect"]
    }
    
    user = UserRecord(**raw_input)
    print(f"Validated User: {user.username} (Age: {user.age})")

    # Example 2: Using the context manager
    with mock_db_connection("Production_Lakehouse") as conn:
        print(f"Performing operations on {conn['db']}...")
