"""
Configuration principale de l'application FastAPI.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
import logging
import os
from app.api import etablissements, urgences, recommandations, geo  # Ajout du nouveau module geo
from app.db.database import engine
from app.models.models import Base

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/api.log")
    ]
)
logger = logging.getLogger(__name__)

# Création de l'application FastAPI
app = FastAPI(
    title="Santé Express API",
    description="API RESTful pour l'application Santé Express permettant de trouver les établissements de santé les plus proches et de connaître l'état des urgences en temps réel.",
    version="1.0.0",
    docs_url=None,           # Désactiver l'URL par défaut de Swagger
    redoc_url="/api/docs",   # ReDoc disponible ici
    openapi_url="/openapi.json",  # URL du schéma OpenAPI
)

# 💡 Forcer la version OpenAPI 3.0.0 pour Swagger UI
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    openapi_schema["openapi"] = "3.0.0"  # 👈 Fix ici
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Autoriser toutes les origines en développement
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Route personnalisée pour Swagger UI
@app.get("/api/swagger", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Santé Express API - Documentation",
        swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4/swagger-ui-bundle.js",
        swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@4/swagger-ui.css",
    )

# Inclure les routeurs
app.include_router(etablissements.router)
app.include_router(urgences.router)
app.include_router(recommandations.router)
app.include_router(geo.router)  # Ajout du nouveau routeur pour les données géographiques

# Route racine
@app.get("/")
async def root():
    return {
        "message": "Bienvenue sur l'API Santé Express",
        "documentation": "/api/docs ou /api/swagger",
        "version": "1.0.0"
    }

# Démarrage de l'application
@app.on_event("startup")
async def startup_event():
    logger.info("Démarrage de l'API Santé Express")
    
    # Créer les répertoires nécessaires s'ils n'existent pas
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data", exist_ok=True)
    
    # Initialiser les tables de la base de données
    logger.info("Initialisation des tables de la base de données")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Tables de la base de données créées avec succès")
        
        # Vérifier et initialiser les régions
        logger.info("Vérification et initialisation des régions")
        from scripts.fix_tables import init_regions
        init_regions()
        logger.info("Vérification des régions terminée")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation de la base de données: {str(e)}")

# Arrêt de l'application
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Arrêt de l'API Santé Express")