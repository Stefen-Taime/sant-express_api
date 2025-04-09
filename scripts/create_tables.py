#!/usr/bin/env python3
"""
Script pour créer explicitement les tables dans la base de données PostgreSQL/PostGIS.
Ce script utilise des instructions SQL directes plutôt que SQLAlchemy pour éviter
les problèmes de conflit lors de la création des tables et des index.
"""
import os
import sys
import logging
import time
from sqlalchemy import text, create_engine, inspect
from sqlalchemy.exc import OperationalError, ProgrammingError

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# URL de la base de données
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@db:5432/sante_express')

# Création du moteur de base de données
engine = create_engine(DATABASE_URL)

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
    Vérifie si l'extension PostGIS est installée et l'installe si nécessaire.
    
    Returns:
        bool: True si l'extension est installée, False sinon
    """
    logger.info("Vérification de l'extension PostGIS...")
    
    try:
        with engine.connect() as connection:
            # Vérifier si l'extension est déjà installée
            result = connection.execute(text("SELECT * FROM pg_extension WHERE extname = 'postgis'"))
            if result.rowcount == 0:
                logger.info("Installation de l'extension PostGIS...")
                # Installez l'extension PostGIS
                connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                connection.commit()
                logger.info("Extension PostGIS installée avec succès.")
            else:
                logger.info("L'extension PostGIS est déjà installée.")
            return True
    except Exception as e:
        logger.error(f"Erreur lors de la vérification/installation de PostGIS: {str(e)}")
        return False

def create_tables_if_not_exist():
    """
    Crée les tables si elles n'existent pas déjà.
    
    Returns:
        bool: True si les tables ont été créées avec succès, False sinon
    """
    logger.info("Création des tables si elles n'existent pas...")
    
    # Vérifier les tables existantes
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    logger.info(f"Tables existantes: {existing_tables}")
    
    try:
        with engine.connect() as connection:
            # Création des tables avec des instructions SQL directes
            # Table regions
            if "regions" not in existing_tables:
                logger.info("Création de la table 'regions'...")
                connection.execute(text("""
                    CREATE TABLE regions (
                        id SERIAL PRIMARY KEY,
                        rss VARCHAR(2) NOT NULL,
                        nom VARCHAR(100) NOT NULL,
                        nom_normalise VARCHAR(100) NOT NULL,
                        geom GEOMETRY(MULTIPOLYGON, 4326)
                    )
                """))
                # Création des index
                connection.execute(text("CREATE INDEX idx_regions_rss ON regions (rss)"))
                connection.execute(text("CREATE INDEX idx_regions_nom_normalise ON regions (nom_normalise)"))
                connection.execute(text("CREATE INDEX idx_regions_geom ON regions USING gist (geom)"))
                connection.commit()
                logger.info("Table 'regions' créée avec succès.")
            
            # Table etablissements
            if "etablissements" not in existing_tables:
                logger.info("Création de la table 'etablissements'...")
                connection.execute(text("""
                    CREATE TABLE etablissements (
                        id SERIAL PRIMARY KEY,
                        source_id VARCHAR(100),
                        no_permis_installation VARCHAR(100),
                        nom_etablissement VARCHAR(255),
                        nom_etablissement_normalise VARCHAR(255),
                        nom_installation VARCHAR(255),
                        nom_installation_normalise VARCHAR(255),
                        type VARCHAR(100),
                        adresse VARCHAR(255),
                        code_postal VARCHAR(10),
                        ville VARCHAR(100),
                        province VARCHAR(100),
                        region_id INTEGER REFERENCES regions(id),
                        point_geo GEOMETRY(POINT, 4326),
                        date_maj TIMESTAMP WITHOUT TIME ZONE
                    )
                """))
                # Création des index
                connection.execute(text("CREATE INDEX idx_etablissements_nom_etablissement_normalise ON etablissements (nom_etablissement_normalise)"))
                connection.execute(text("CREATE INDEX idx_etablissements_nom_installation_normalise ON etablissements (nom_installation_normalise)"))
                connection.execute(text("CREATE INDEX idx_etablissements_region_id ON etablissements (region_id)"))
                try:
                    connection.execute(text("CREATE INDEX idx_etablissements_point_geo ON etablissements USING gist (point_geo)"))
                except Exception as e:
                    logger.warning(f"L'index spatial existe peut-être déjà: {str(e)}")
                connection.commit()
                logger.info("Table 'etablissements' créée avec succès.")
            
            # Table urgences_etat_actuel
            if "urgences_etat_actuel" not in existing_tables:
                logger.info("Création de la table 'urgences_etat_actuel'...")
                connection.execute(text("""
                    CREATE TABLE urgences_etat_actuel (
                        id SERIAL PRIMARY KEY,
                        etablissement_id INTEGER NOT NULL REFERENCES etablissements(id),
                        civieres_fonctionnelles INTEGER,
                        civieres_occupees INTEGER,
                        patients_24h INTEGER,
                        patients_48h INTEGER,
                        total_patients INTEGER,
                        patients_en_attente INTEGER,
                        dms_civiere FLOAT,
                        dms_ambulatoire FLOAT,
                        taux_occupation FLOAT,
                        date_extraction TIMESTAMP WITHOUT TIME ZONE,
                        date_maj TIMESTAMP WITHOUT TIME ZONE,
                        statut_validation VARCHAR(50)
                    )
                """))
                # Création des index
                connection.execute(text("CREATE INDEX idx_urgences_etat_actuel_etablissement_id ON urgences_etat_actuel (etablissement_id)"))
                connection.execute(text("CREATE INDEX idx_urgences_etat_actuel_date_extraction ON urgences_etat_actuel (date_extraction)"))
                connection.commit()
                logger.info("Table 'urgences_etat_actuel' créée avec succès.")
            
            # Table urgences_historique
            if "urgences_historique" not in existing_tables:
                logger.info("Création de la table 'urgences_historique'...")
                connection.execute(text("""
                    CREATE TABLE urgences_historique (
                        id SERIAL PRIMARY KEY,
                        etablissement_id INTEGER NOT NULL REFERENCES etablissements(id),
                        civieres_fonctionnelles INTEGER,
                        civieres_occupees INTEGER,
                        patients_24h INTEGER,
                        patients_48h INTEGER,
                        total_patients INTEGER,
                        patients_en_attente INTEGER,
                        dms_civiere FLOAT,
                        dms_ambulatoire FLOAT,
                        taux_occupation FLOAT,
                        date_extraction TIMESTAMP WITHOUT TIME ZONE,
                        date_maj TIMESTAMP WITHOUT TIME ZONE,
                        statut_validation VARCHAR(50)
                    )
                """))
                # Création des index
                connection.execute(text("CREATE INDEX idx_urgences_historique_etablissement_id ON urgences_historique (etablissement_id)"))
                connection.execute(text("CREATE INDEX idx_urgences_historique_date_extraction ON urgences_historique (date_extraction)"))
                connection.commit()
                logger.info("Table 'urgences_historique' créée avec succès.")
            
        # Vérifier que les tables ont été créées
        inspector = inspect(engine)
        updated_tables = inspector.get_table_names()
        expected_tables = ['regions', 'etablissements', 'urgences_etat_actuel', 'urgences_historique']
        
        all_tables_exist = all(table in updated_tables for table in expected_tables)
        
        if all_tables_exist:
            logger.info("Toutes les tables ont été créées avec succès.")
            return True
        else:
            missing_tables = [table for table in expected_tables if table not in updated_tables]
            logger.warning(f"Certaines tables n'ont pas été créées: {missing_tables}")
            return False
            
    except Exception as e:
        logger.error(f"Erreur lors de la création des tables: {str(e)}")
        return False

def initialize_regions():
    """
    Initialise les régions du Québec si elles n'existent pas déjà.
    
    Returns:
        bool: True si les régions ont été initialisées avec succès, False sinon
    """
    logger.info("Initialisation des régions...")
    
    try:
        with engine.connect() as connection:
            # Vérifier si des régions existent déjà
            result = connection.execute(text("SELECT COUNT(*) FROM regions"))
            count = result.scalar()
            
            if count > 0:
                logger.info(f"{count} régions existent déjà dans la base de données.")
                return True
            
            # Ajouter les régions
            for region in REGIONS_QUEBEC:
                # Normaliser le nom pour la recherche
                nom_normalise = region["nom"].lower().replace("-", " ").replace("é", "e").replace("è", "e").replace("ê", "e").replace("à", "a").replace("ô", "o").replace("î", "i").replace("ç", "c")
                
                connection.execute(text("""
                    INSERT INTO regions (rss, nom, nom_normalise)
                    VALUES (:rss, :nom, :nom_normalise)
                """), {"rss": region["rss"], "nom": region["nom"], "nom_normalise": nom_normalise})
            
            connection.commit()
            logger.info(f"{len(REGIONS_QUEBEC)} régions ont été ajoutées avec succès.")
            return True
            
    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation des régions: {str(e)}")
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
    
    # Vérifier l'extension PostGIS
    if not check_postgis_extension():
        logger.error("Problème avec l'extension PostGIS. Arrêt du script.")
        sys.exit(1)
    
    # Créer les tables
    if not create_tables_if_not_exist():
        logger.error("Échec de la création des tables. Arrêt du script.")
        sys.exit(1)
    
    # Initialiser les régions
    if not initialize_regions():
        logger.error("Échec de l'initialisation des régions. Arrêt du script.")
        sys.exit(1)
    
    logger.info("Initialisation de la base de données terminée avec succès.")

if __name__ == "__main__":
    main()