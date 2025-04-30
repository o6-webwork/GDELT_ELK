from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import routers
from routers import reports_backend, query_gen, sem_search_backend

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
 
# Register routers
app.include_router(reports_backend.router, prefix="/reports_backend")
app.include_router(query_gen.router, prefix="/query_gen")
app.include_router(sem_search_backend.router, prefix="/sem_search_backend")
