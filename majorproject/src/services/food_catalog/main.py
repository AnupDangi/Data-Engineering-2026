"""Food Catalog Service - FastAPI application."""
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .db import close_connection_pool, get_db_cursor, init_connection_pool
from .routes.foods import router as foods_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    print("🚀 Starting Food Catalog Service...")
    init_connection_pool(minconn=2, maxconn=10)
    
    # Load food items into database if empty
    await load_initial_data()
    
    print("✅ Food Catalog Service ready on port 8001")
    
    yield
    
    # Shutdown
    print("🔴 Shutting down Food Catalog Service...")
    close_connection_pool()


async def load_initial_data():
    """Load food items from JSON if database is empty."""
    try:
        with get_db_cursor() as cursor:
            # Check if data already exists
            cursor.execute("SELECT COUNT(*) as count FROM foods")
            result = cursor.fetchone()
            
            if result["count"] > 0:
                print(f"✅ Database already has {result['count']} food items")
                return
            
            # Load from JSON file
            json_path = os.path.join(
                os.path.dirname(__file__),
                "..", "..", "..",
                "data", "food_items.json"
            )
            
            if not os.path.exists(json_path):
                print(f"⚠️  Food items JSON not found at {json_path}")
                return
            
            with open(json_path, "r") as f:
                food_items = json.load(f)
            
            # Insert into database
            insert_query = """
                INSERT INTO foods (food_id, name, category, price, description, image_url, is_available)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            for item in food_items:
                cursor.execute(insert_query, (
                    item["food_id"],
                    item["name"],
                    item["category"],
                    item["price"],
                    item.get("description"),
                    item["image_url"],
                    item.get("is_available", True)
                ))
            
            print(f"✅ Loaded {len(food_items)} food items into database")
    
    except Exception as e:
        print(f"❌ Error loading initial data: {e}")
        raise


# Create FastAPI app
app = FastAPI(
    title="FlowGuard - Food Catalog Service",
    description="Reference data service for food items catalog",
    version="1.0.0",
    lifespan=lifespan
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Frontend on different port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(foods_router)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT 1")
            db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "service": "food_catalog",
        "database": db_status
    }


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "FlowGuard Food Catalog Service",
        "version": "1.0.0",
        "docs": "/docs"
    }
