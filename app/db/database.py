"""
Configuration de la base de données et des sessions SQLAlchemy.
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os
from typing import Generator

# Récupérer les variables d'environnement pour la configuration de la base de données
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres:postgres@db:5432/sante_express"
)

# Créer le moteur SQLAlchemy avec des options de pool optimisées
engine = create_engine(
    DATABASE_URL,
    pool_size=20,  # Nombre de connexions permanentes
    max_overflow=40,  # Nombre de connexions supplémentaires autorisées
    pool_timeout=30,  # Temps d'attente pour une connexion (secondes)
    pool_recycle=1800,  # Recycler les connexions après 30 minutes
    pool_pre_ping=True,  # Vérifier la validité des connexions avant utilisation
    poolclass=QueuePool  # Utiliser QueuePool pour gérer les connexions
)

# Créer une session locale
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base pour les modèles déclaratifs
Base = declarative_base()

def get_db() -> Generator:
    """
    Fournit une session de base de données pour les opérations.
    
    Yields:
        Session: Session SQLAlchemy
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
