import os
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import http.client


# Đọc biến môi trường với giá trị mặc định
SERVICE_NAME = os.getenv("SERVICE_NAME", "iot-ingestion")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.5.0")
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "local-dev-token")

# Cấu hình Database
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "iotdb"),
        user=os.getenv("POSTGRES_USER", "lab05"),
        password=os.getenv("POSTGRES_PASSWORD", "lab05pass"),
    )

def init_db():
    for i in range(15):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS readings (
                    reading_id VARCHAR(50) PRIMARY KEY,
                    device_id VARCHAR(100) NOT NULL,
                    metric VARCHAR(50) NOT NULL,
                    value DOUBLE PRECISION NOT NULL,
                    unit VARCHAR(50),
                    timestamp VARCHAR(100) NOT NULL,
                    created_at VARCHAR(100) NOT NULL
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("Database initialized successfully.")
            break
        except Exception as e:
            print(f"Database initialization failed, retrying in 2 seconds... Error: {e}")
            time.sleep(2)



app = FastAPI(
    title="FIT4110 Lab 05 - IoT Ingestion Service",
    version=SERVICE_VERSION,
    description=(
        "IoT Ingestion API chạy trong ngữ cảnh Docker Compose cho Lab 05. "
        "Luồng logic được kế thừa từ Lab 04 và tiếp tục được dùng để kiểm thử end‑to‑end."
    ),
)


class SensorMetric(str, Enum):
    temperature = "temperature"
    humidity = "humidity"
    motion = "motion"
    smoke = "smoke"


class SensorUnit(str, Enum):
    celsius = "celsius"
    percent = "percent"
    boolean = "boolean"
    ppm = "ppm"


class ProblemDetails(BaseModel):
    type: str = "about:blank"
    title: str
    status: int = Field(..., ge=400, le=599)
    detail: str
    instance: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


class SensorReadingCreate(BaseModel):
    device_id: str = Field(..., min_length=3, examples=["ESP32-LAB-A01"])
    metric: SensorMetric = Field(..., examples=["temperature"])
    value: float = Field(
        ...,
        ge=-40,
        le=80,
        description="Boundary range used in Lab 03 và Lab 04: -40 đến 80.",
        examples=[31.5],
    )
    unit: Optional[SensorUnit] = Field(default=None, examples=["celsius"])
    timestamp: str = Field(..., examples=["2026-05-13T08:30:00+07:00"])


class SensorReading(BaseModel):
    reading_id: str
    device_id: str
    metric: SensorMetric
    value: float
    unit: Optional[SensorUnit] = None
    timestamp: str
    created_at: str


class SensorReadingCreated(BaseModel):
    reading_id: str
    device_id: str
    metric: SensorMetric
    accepted: bool
    created_at: str


READINGS: List[Dict] = []


def build_problem(
    *,
    status_code: int,
    title: str,
    detail: str,
    instance: Optional[str] = None,
    problem_type: str = "about:blank",
) -> Dict:
    problem = {
        "type": problem_type,
        "title": title,
        "status": status_code,
        "detail": detail,
    }
    if instance:
        problem["instance"] = instance
    return problem


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    if isinstance(exc.detail, dict):
        problem = exc.detail
    else:
        problem = build_problem(
            status_code=exc.status_code,
            title=http.client.responses.get(exc.status_code, "HTTP Error"),
            detail=str(exc.detail),
            instance=str(request.url.path),
        )

    problem.setdefault("status", exc.status_code)
    problem.setdefault("title", http.client.responses.get(exc.status_code, "HTTP Error"))
    problem.setdefault("type", "about:blank")
    problem.setdefault("detail", "Request failed")
    problem.setdefault("instance", str(request.url.path))

    return JSONResponse(
        status_code=exc.status_code,
        content=problem,
        media_type="application/problem+json",
        headers=getattr(exc, "headers", None),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    first_error = exc.errors()[0] if exc.errors() else {}
    location = ".".join(str(item) for item in first_error.get("loc", []))
    message = first_error.get("msg", "Request validation error")
    detail = f"{location}: {message}" if location else message

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=build_problem(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            title="Validation error",
            detail=detail,
            instance=str(request.url.path),
            problem_type="https://smart-campus.local/problems/validation-error",
        ),
        media_type="application/problem+json",
    )


def verify_bearer_token(authorization: Optional[str] = Header(default=None)) -> None:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=build_problem(
                status_code=status.HTTP_401_UNAUTHORIZED,
                title="Unauthorized",
                detail="Missing Authorization header",
                problem_type="https://smart-campus.local/problems/unauthorized",
            ),
        )

    expected = f"Bearer {AUTH_TOKEN}"
    if authorization != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=build_problem(
                status_code=status.HTTP_401_UNAUTHORIZED,
                title="Unauthorized",
                detail="Invalid bearer token",
                problem_type="https://smart-campus.local/problems/unauthorized",
            ),
        )


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@app.on_event("startup")
def startup_event():
    init_db()


def next_reading_id() -> str:
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM readings WHERE reading_id LIKE %s", (f"R-{today}-%",))
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return f"R-{today}-{count + 1:04d}"
    except Exception as e:
        print(f"Error generating reading ID, using uuid fallback: {e}")
        import uuid
        return f"R-{today}-{uuid.uuid4().hex[:4]}"


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    # 1. Check DB connection
    db_ok = False
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        db_ok = True
    except Exception as e:
        print(f"Healthcheck: DB connection failed: {e}")

    # 2. Check AI service connection
    ai_ok = False
    try:
        ai_url = os.getenv("AI_SERVICE_URL", "http://ai-service:9000")
        ai_res = requests.get(f"{ai_url}/health", timeout=3)
        if ai_res.status_code == 200:
            ai_ok = True
    except Exception as e:
        print(f"Healthcheck: AI service connection failed: {e}")

    if not db_ok or not ai_ok:
        errors = []
        if not db_ok:
            errors.append("database is unreachable")
        if not ai_ok:
            errors.append("ai-service is unreachable")
        raise HTTPException(
            status_code=503,
            detail=build_problem(
                status_code=503,
                title="Service Unavailable",
                detail=f"System degradation: {', '.join(errors)}",
            )
        )

    return HealthResponse(
        status="ok",
        service=SERVICE_NAME,
        version=SERVICE_VERSION,
    )


@app.post(
    "/readings",
    response_model=SensorReadingCreated,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_bearer_token)],
    responses={
        401: {"model": ProblemDetails},
        422: {"model": ProblemDetails},
        429: {"model": ProblemDetails},
    },
)
def create_reading(payload: SensorReadingCreate, response: Response) -> SensorReadingCreated:
    # Ví dụ logic cảnh báo: nếu nhiệt độ >= 70 thì thêm header cảnh báo
    if payload.metric == SensorMetric.temperature and payload.value >= 70:
        response.headers["X-Warning"] = "high-temperature"

    # Call AI service predict
    try:
        ai_url = os.getenv("AI_SERVICE_URL", "http://ai-service:9000")
        ai_res = requests.post(f"{ai_url}/predict", json={}, timeout=5)
        if ai_res.status_code == 200:
            print(f"AI Service response: {ai_res.json()}")
    except Exception as e:
        print(f"Failed to contact AI service: {e}")

    reading_id = next_reading_id()
    created_at = now_iso()

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO readings (reading_id, device_id, metric, value, unit, timestamp, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                reading_id,
                payload.device_id,
                payload.metric.value,
                payload.value,
                payload.unit.value if payload.unit else None,
                payload.timestamp,
                created_at
            )
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=build_problem(
                status_code=500,
                title="Database Error",
                detail=f"Failed to save reading to database: {e}",
            )
        )

    return SensorReadingCreated(
        reading_id=reading_id,
        device_id=payload.device_id,
        metric=payload.metric,
        accepted=True,
        created_at=created_at,
    )


@app.get("/readings/latest", dependencies=[Depends(verify_bearer_token)])
def latest_readings(
    device_id: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
) -> Dict[str, List[Dict]]:
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if device_id:
            cur.execute(
                "SELECT * FROM readings WHERE device_id = %s ORDER BY created_at DESC, reading_id DESC LIMIT %s",
                (device_id, limit)
            )
        else:
            cur.execute(
                "SELECT * FROM readings ORDER BY created_at DESC, reading_id DESC LIMIT %s",
                (limit,)
            )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        items = []
        for row in rows:
            items.append({
                "reading_id": row["reading_id"],
                "device_id": row["device_id"],
                "metric": row["metric"],
                "value": row["value"],
                "unit": row["unit"],
                "timestamp": row["timestamp"],
                "created_at": row["created_at"]
            })
        
        items.reverse()
        return {"items": items}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=build_problem(
                status_code=500,
                title="Database Error",
                detail=f"Failed to query database: {e}",
            )
        )


@app.get("/readings/{reading_id}", dependencies=[Depends(verify_bearer_token)])
def get_reading(reading_id: str) -> Dict:
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM readings WHERE reading_id = %s", (reading_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=build_problem(
                status_code=500,
                title="Database Error",
                detail=f"Failed to query database: {e}",
            )
        )

    if row:
        return {
            "reading_id": row["reading_id"],
            "device_id": row["device_id"],
            "metric": row["metric"],
            "value": row["value"],
            "unit": row["unit"],
            "timestamp": row["timestamp"],
            "created_at": row["created_at"]
        }

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=build_problem(
            status_code=status.HTTP_404_NOT_FOUND,
            title="Not Found",
            detail=f"Reading {reading_id} does not exist",
            instance=f"/readings/{reading_id}",
            problem_type="https://smart-campus.local/problems/not-found",
        ),
    )