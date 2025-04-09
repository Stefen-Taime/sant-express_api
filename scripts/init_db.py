#!/usr/bin/env python3
"""
Script d'initialisation de la base de données avec vérification.
Ce script s'assure que toutes les tables nécessaires sont créées
et que les données de base sont insérées avant le démarrage de l'application.
"""
import sys
import os
import logging
import time
from sqlalchemy import text, inspect, create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import sessionmaker
import pandas as pd

# Ajouter le répertoire parent au path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.insert(0, parent_dir)

# Importer après avoir ajusté le path
from app.db.database import Base, engine, SessionLocal
from app.models.models import Region, Etablissement, UrgencesEtatActuel, UrgencesHistorique
from app.utils.encoding_utils import normalize_text

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/init_db.log")
    ]
)
logger = logging.getLogger(__name__)

# Créer le répertoire des logs s'il n'existe pas
os.makedirs("logs", exist_ok=True)

# Définition des régions du Québec
REGIONS_QUEBEC = [
    {"rss": "01", "nom": "Bas-Saint-Laurent"},
    {"rss": "02", "nom": "Saguenay-Lac-Saint-Jean"},
    {"rss": "03", "nom": "Capitale-Nationale"},
    {"rss": "04", "nom": "Mauricie et Centre-du-Québec"},
    {"rss": "05", "nom": "Estrie"},
    {"rss": "06", "nom": "Montréal"},
    {"rss": "07", "nom": "Outaouais"},
    {"rss": "08", "nom": "Abitibi-Témiscamingue"},
    {"rss": "09", "nom": "Côte-Nord"},
    {"rss": "10", "nom": "Nord-du-Québec"},
    {"rss": "11", "nom": "Gaspésie-Îles-de-la-Madeleine"},
    {"rss": "12", "nom": "Chaudière-Appalaches"},
    {"rss": "13", "nom": "Laval"},
    {"rss": "14", "nom": "Lanaudière"},
    {"rss": "15", "nom": "Laurentides"},
    {"rss": "16", "nom": "Montérégie"},
    {"rss": "17", "nom": "Nunavik"},
    {"rss": "18", "nom": "Terres-Cries-de-la-Baie-James"}
]

def wait_for_database(max_retries=30, delay=5):
    """
    Attend que la base de données soit disponible.
    
    Args:
        max_retries: Nombre maximum de tentatives
        delay: Délai entre les tentatives en secondes
    
    Returns:
        bool: True si la base de données est disponible, False sinon
    """
    logger.info("Attente de la base de données...")
    
    for attempt in range(max_retries):
        try:
            # Tenter de se connecter à la base de données
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("Connexion à la base de données établie.")
            return True
        except OperationalError as e:
            logger.warning(f"Tentative {attempt+1}/{max_retries}: Base de données non disponible. Attente de {delay} secondes...")
            time.sleep(delay)
    
    logger.error(f"Impossible de se connecter à la base de données après {max_retries} tentatives.")
    return False

def check_postgis_extension():
    """
    Vérifie si l'extension PostGIS est installée, sinon l'installe.
    """
    logger.info("Vérification de l'extension PostGIS...")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT extname FROM pg_extension WHERE extname='postgis'"))
            if not result.fetchone():
                logger.info("Installation de l'extension PostGIS...")
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                conn.commit()
                logger.info("Extension PostGIS installée avec succès.")
            else:
                logger.info("L'extension PostGIS est déjà installée.")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la vérification/installation de PostGIS: {str(e)}")
        return False

def create_tables():
    """
    Crée toutes les tables définies dans les modèles.
    
    Returns:
        bool: True si les tables ont été créées avec succès, False sinon
    """
    logger.info("Création des tables...")
    
    try:
        # Créer toutes les tables
        Base.metadata.create_all(bind=engine)
        
        # Vérifier si la colonne geom existe dans la table regions
        inspector = inspect(engine)
        
        # Vérifier d'abord si la table regions existe
        if 'regions' in inspector.get_table_names():
            # Ensuite, vérifier les colonnes
            columns = [c['name'] for c in inspector.get_columns('regions')]
            
            # Si la colonne geom n'existe pas, l'ajouter
            if 'geom' not in columns:
                logger.info("Ajout de la colonne geom à la table regions")
                with engine.connect() as connection:
                    connection.execute(text("ALTER TABLE regions ADD COLUMN IF NOT EXISTS geom geometry(MULTIPOLYGON, 4326);"))
                    connection.commit()
                logger.info("Colonne geom ajoutée avec succès")
        
        # Vérifier que les tables ont été créées
        inspector = inspect(engine)
        expected_tables = ['regions', 'etablissements', 'urgences_etat_actuel', 'urgences_historique']
        existing_tables = inspector.get_table_names()
        
        all_tables_exist = all(table in existing_tables for table in expected_tables)
        
        if all_tables_exist:
            logger.info("Toutes les tables ont été créées avec succès.")
            return True
        else:
            missing_tables = [table for table in expected_tables if table not in existing_tables]
            logger.warning(f"Certaines tables n'ont pas été créées: {missing_tables}")
            return False
            
    except Exception as e:
        logger.error(f"Erreur lors de la création des tables: {str(e)}")
        return False

def initialize_regions():
    """
    Initialise les régions du Québec dans la base de données.
    
    Returns:
        bool: True si les régions ont été initialisées avec succès, False sinon
    """
    logger.info("Initialisation des régions...")
    
    db = SessionLocal()
    try:
        # Vérifier si des régions existent déjà
        existing_regions = db.query(Region).count()
        
        if existing_regions > 0:
            logger.info(f"{existing_regions} régions existent déjà dans la base de données.")
            return True
        
        # Ajouter les régions
        for region_data in REGIONS_QUEBEC:
            region = Region(
                rss=region_data["rss"],
                nom=region_data["nom"],
                nom_normalise=normalize_text(region_data["nom"], remove_accents=True)
            )
            db.add(region)
        
        db.commit()
        logger.info(f"{len(REGIONS_QUEBEC)} régions ont été ajoutées avec succès.")
        return True
        
    except Exception as e:
        db.rollback()
        logger.error(f"Erreur lors de l'initialisation des régions: {str(e)}")
        return False
    finally:
        db.close()

def verify_initialization():
    """
    Vérifie que l'initialisation a bien été effectuée.
    
    Returns:
        bool: True si l'initialisation est complète, False sinon
    """
    logger.info("Vérification de l'initialisation...")
    
    db = SessionLocal()
    try:
        # Vérifier que les tables existent
        inspector = inspect(engine)
        expected_tables = ['regions', 'etablissements', 'urgences_etat_actuel', 'urgences_historique']
        existing_tables = inspector.get_table_names()
        
        all_tables_exist = all(table in existing_tables for table in expected_tables)
        
        if not all_tables_exist:
            missing_tables = [table for table in expected_tables if table not in existing_tables]
            logger.error(f"Vérification échouée: tables manquantes {missing_tables}")
            return False
        
        # Vérifier que les régions existent
        regions_count = db.query(Region).count()
        if regions_count != len(REGIONS_QUEBEC):
            logger.error(f"Vérification échouée: nombre de régions incorrect ({regions_count} au lieu de {len(REGIONS_QUEBEC)})")
            return False
        
        logger.info("Vérification réussie: la base de données est correctement initialisée.")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de la vérification: {str(e)}")
        return False
    finally:
        db.close()

def fix_null_values_in_database():
    """
    Corrige les valeurs NULL dans les tables de la base de données
    en appelant le script fix_tables.py.
    """
    logger.info("Correction des valeurs NULL dans les tables...")
    
    try:
        # Importer la fonction depuis fix_tables.py
        from scripts.fix_tables import fix_null_values_in_tables
        
        # Exécuter la correction
        fix_null_values_in_tables()
        
        logger.info("Correction des valeurs NULL terminée avec succès.")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la correction des valeurs NULL: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    """
    Fonction principale d'initialisation de la base de données.
    """
    logger.info("Début de l'initialisation de la base de données")
    
    # Attendre que la base de données soit disponible
    if not wait_for_database():
        logger.error("Impossible de se connecter à la base de données. Arrêt du script.")
        sys.exit(1)
    
    # Vérifier et installer PostGIS si nécessaire
    if not check_postgis_extension():
        logger.warning("Problème avec l'extension PostGIS. Continuation avec précaution.")
    
    # Créer les tables
    if not create_tables():
        logger.error("Échec de la création des tables. Arrêt du script.")
        sys.exit(1)
    
    # Initialiser les régions
    if not initialize_regions():
        logger.error("Échec de l'initialisation des régions. Arrêt du script.")
        sys.exit(1)
    
    # Vérifier l'initialisation
    if not verify_initialization():
        logger.error("La vérification de l'initialisation a échoué. La base de données pourrait ne pas être correctement configurée.")
        sys.exit(1)
    
    # Corriger les valeurs NULL dans les tables
    fix_null_values_in_database()
    
    logger.info("Initialisation de la base de données terminée avec succès.")

if __name__ == "__main__":
    main()