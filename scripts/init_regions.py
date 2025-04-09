import sys
import os
import logging
from sqlalchemy import text, inspect

# Ajouter le répertoire parent au chemin
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db.database import SessionLocal, engine, Base
from app.models.models import Region
from app.utils.encoding_utils import normalize_text

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_regions():
    """
    Initialise les régions dans la base de données.
    """
    # Créer les tables si elles n'existent pas
    Base.metadata.create_all(bind=engine)
    logger.info("Tables créées avec succès")
    
    # Vérifier si la colonne geom existe dans la table regions
    inspector = inspect(engine)
    
    # Vérifier d'abord si la table regions existe
    if 'regions' in inspector.get_table_names():
        # Ensuite, vérifier les colonnes
        columns = [c['name'] for c in inspector.get_columns('regions')]
        
        # Si la colonne geom n'existe pas, l'ajouter
        if 'geom' not in columns:
            logger.info("Ajout de la colonne geom à la table regions")
            try:
                with engine.connect() as connection:
                    connection.execute(text("ALTER TABLE regions ADD COLUMN IF NOT EXISTS geom geometry(MULTIPOLYGON, 4326);"))
                    connection.commit()
                logger.info("Colonne geom ajoutée avec succès")
            except Exception as e:
                logger.error(f"Erreur lors de l'ajout de la colonne geom: {str(e)}")
    
    # Liste des régions du Québec
    regions = [
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
    
    # Ouvrir une session
    db = SessionLocal()
    try:
        # Créer les régions si elles n'existent pas
        for region_data in regions:
            # Vérifier si la région existe déjà
            exists = db.query(Region).filter(Region.rss == region_data["rss"]).first()
            if not exists:
                region = Region(
                    rss=region_data["rss"],
                    nom=region_data["nom"],
                    nom_normalise=normalize_text(region_data["nom"], remove_accents=True)
                )
                db.add(region)
                logger.info(f"Région ajoutée: {region_data['nom']}")
        
        db.commit()
        logger.info("Régions initialisées avec succès")
    except Exception as e:
        db.rollback()
        logger.error(f"Erreur lors de l'initialisation des régions: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    init_regions()