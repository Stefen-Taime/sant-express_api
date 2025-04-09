#!/usr/bin/env python3
"""
Script pour corriger les valeurs NULL dans les tables de la base de données.
"""
import sys
import os
import logging
import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy.orm import Session
from geoalchemy2 import WKTElement
from datetime import datetime

# Ajouter le répertoire parent au path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.insert(0, parent_dir)

# Importer après avoir ajusté le path
from app.db.database import SessionLocal, engine
from app.models.models import Etablissement, Region, UrgencesEtatActuel, UrgencesHistorique
from app.utils.encoding_utils import read_csv_with_encoding_detection, clean_dataframe, standardize_establishment_names, normalize_text

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(parent_dir, "logs/fix_tables.log"))
    ]
)
logger = logging.getLogger(__name__)

# Créer les répertoires nécessaires
os.makedirs(os.path.join(parent_dir, "logs"), exist_ok=True)
os.makedirs(os.path.join(parent_dir, "data"), exist_ok=True)

def extraire_info_etablissements(url_odhf: str = None):
    """
    Extrait les informations d'établissements depuis le fichier ODHF pour compléter les entrées manquantes.
    """
    if url_odhf is None:
        url_odhf = os.path.join(parent_dir, "data/odhf_v1.1.csv")
        
    try:
        # Vérifier si le fichier existe
        if not os.path.exists(url_odhf):
            logger.warning(f"Le fichier {url_odhf} n'existe pas. Utilisation des valeurs par défaut.")
            return {}
            
        # Lire le fichier CSV ODHF
        df_odhf = read_csv_with_encoding_detection(url_odhf)
        
        # Nettoyer les données
        text_cols = [col for col in df_odhf.columns if df_odhf[col].dtype == 'object']
        df_odhf_clean = clean_dataframe(df_odhf, text_cols)
        
        # Normaliser les noms pour faciliter le matching
        df_odhf_clean = standardize_establishment_names(df_odhf_clean, "facility_name")
        
        # Créer un dictionnaire des établissements
        etablissements_info = {}
        for _, row in df_odhf_clean.iterrows():
            nom = row.get('facility_name', '').strip()
            if nom:
                # Construire une adresse complète
                adresse = ""
                if not pd.isna(row.get('street_no')) and str(row.get('street_no')).strip():
                    adresse += str(row.get('street_no')).strip() + " "
                if not pd.isna(row.get('street_name')) and str(row.get('street_name')).strip():
                    adresse += str(row.get('street_name')).strip()
                
                # Standardiser
                nom_normalise = row.get('facility_name_normalise', '').lower().strip()
                
                etablissements_info[nom_normalise] = {
                    'type': row.get('odhf_facility_type', 'Hôpital'),
                    'adresse': adresse.strip() if adresse.strip() else "À compléter",
                    'code_postal': str(row.get('postal_code', '')).strip(),
                    'ville': str(row.get('city', '')).strip(),
                    'province': str(row.get('province', 'Québec')).strip().upper(),
                    'latitude': row.get('latitude'),
                    'longitude': row.get('longitude')
                }
                
        logger.info(f"{len(etablissements_info)} établissements extraits du fichier ODHF")
        return etablissements_info
    
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des informations d'établissements: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {}

def get_etablissement_info(etablissements_info, nom_installation_norm, nom_etablissement_norm):
    """
    Récupère les informations complémentaires sur un établissement depuis ODHF.
    """
    # Vérifier si les noms sont valides
    if not nom_installation_norm and not nom_etablissement_norm:
        return {
            'type': 'Hôpital',
            'adresse': 'À compléter',
            'code_postal': '',
            'ville': 'À compléter',
            'province': 'Québec'
        }
    
    # Recherche par mots-clés dans les noms normalisés
    for nom_odhf, info in etablissements_info.items():
        # Recherche de correspondance approximative
        if (nom_installation_norm and nom_installation_norm in nom_odhf) or \
           (nom_etablissement_norm and nom_etablissement_norm in nom_odhf):
            return info
            
    # Valeurs par défaut si aucune correspondance trouvée
    return {
        'type': 'Hôpital',
        'adresse': 'À compléter',
        'code_postal': '',
        'ville': 'À compléter',
        'province': 'Québec'
    }

def fix_null_values_in_tables():
    """
    Corrige les valeurs NULL dans les tables de la base de données.
    """
    logger.info("Correction des valeurs NULL dans les tables")
    db = SessionLocal()
    
    try:
        # Extraire les informations d'établissements
        etablissements_info = extraire_info_etablissements()
        
        # 1. Correction des établissements
        query_etablissements = """
        SELECT id, nom_etablissement, nom_etablissement_normalise, nom_installation, nom_installation_normalise 
        FROM etablissements 
        WHERE adresse IS NULL OR ville IS NULL OR type IS NULL OR province IS NULL OR
              adresse = '' OR ville = '' OR province = ''
        """
        
        result = db.execute(text(query_etablissements))
        etablissements_a_corriger = result.fetchall()
        
        logger.info(f"Correction de {len(etablissements_a_corriger)} établissements avec des valeurs NULL ou vides")
        
        for etab in etablissements_a_corriger:
            # Extraction des valeurs
            id_etab = etab[0]
            nom_etablissement = etab[1] or ""
            nom_etablissement_norm = etab[2] or ""
            nom_installation = etab[3] or ""
            nom_installation_norm = etab[4] or ""
            
            # Rechercher des informations complémentaires dans ODHF
            info = get_etablissement_info(etablissements_info, nom_installation_norm, nom_etablissement_norm)
            
            # Mise à jour des champs NULL ou vides
            update_query = """
            UPDATE etablissements 
            SET 
                adresse = CASE WHEN adresse IS NULL OR adresse = '' THEN :adresse ELSE adresse END,
                ville = CASE WHEN ville IS NULL OR ville = '' THEN :ville ELSE ville END,
                type = CASE WHEN type IS NULL OR type = '' THEN :type ELSE type END,
                code_postal = CASE WHEN code_postal IS NULL THEN :code_postal ELSE code_postal END,
                province = CASE WHEN province IS NULL OR province = '' THEN :province ELSE province END,
                date_maj = :date_maj
            WHERE id = :id_etab
            """
            
            db.execute(text(update_query), {
                'adresse': info.get('adresse'),
                'ville': info.get('ville'),
                'type': info.get('type'),
                'code_postal': info.get('code_postal', ''),
                'province': info.get('province', 'Québec'),
                'date_maj': datetime.now(),
                'id_etab': id_etab
            })
            
            # Mise à jour des coordonnées géographiques si disponibles
            lat = info.get('latitude')
            lon = info.get('longitude')
            if lat and lon and not pd.isna(lat) and not pd.isna(lon):
                # Vérifier si point_geo est NULL
                check_query = "SELECT point_geo FROM etablissements WHERE id = :id_etab"
                result = db.execute(text(check_query), {'id_etab': id_etab})
                point_geo = result.scalar()
                
                if point_geo is None:
                    try:
                        point_wkt = f'POINT({lon} {lat})'
                        update_geo_query = """
                        UPDATE etablissements 
                        SET point_geo = ST_GeomFromText(:point_wkt, 4326)
                        WHERE id = :id_etab
                        """
                        db.execute(text(update_geo_query), {
                            'point_wkt': point_wkt,
                            'id_etab': id_etab
                        })
                    except Exception as e:
                        logger.warning(f"Erreur lors de la mise à jour des coordonnées géographiques: {str(e)}")
        
        # 2. Correction des données d'urgence
        query_urgences = """
        SELECT id, etablissement_id, civieres_fonctionnelles, civieres_occupees 
        FROM urgences_etat_actuel 
        WHERE dms_civiere IS NULL OR dms_ambulatoire IS NULL OR 
              total_patients IS NULL OR patients_en_attente IS NULL OR taux_occupation IS NULL
        """
        
        result = db.execute(text(query_urgences))
        urgences_a_corriger = result.fetchall()
        
        logger.info(f"Correction de {len(urgences_a_corriger)} données d'urgence avec des valeurs NULL")
        
        for urg in urgences_a_corriger:
            # Extraction des valeurs
            id_urg = urg[0]
            civieres_fonctionnelles = urg[2] or 0
            civieres_occupees = urg[3] or 0
            
            # Calculer le taux d'occupation
            taux_occupation = 0
            if civieres_fonctionnelles and civieres_fonctionnelles > 0 and civieres_occupees is not None:
                taux_occupation = (civieres_occupees / civieres_fonctionnelles) * 100
            
            # Mise à jour des champs NULL
            update_query = """
            UPDATE urgences_etat_actuel 
            SET 
                dms_civiere = COALESCE(dms_civiere, 0),
                dms_ambulatoire = COALESCE(dms_ambulatoire, 0),
                total_patients = COALESCE(total_patients, 0),
                patients_en_attente = COALESCE(patients_en_attente, 0),
                taux_occupation = COALESCE(taux_occupation, :taux_occupation),
                date_maj = :date_maj,
                statut_validation = 'validé'
            WHERE id = :id_urg
            """
            
            db.execute(text(update_query), {
                'taux_occupation': taux_occupation,
                'date_maj': datetime.now(),
                'id_urg': id_urg
            })
        
        # Valider les changements
        db.commit()
        logger.info("Correction des valeurs NULL terminée avec succès")
        
        # Vérifier les résultats
        verify_query_etab = """
        SELECT COUNT(*) FROM etablissements 
        WHERE adresse IS NULL OR ville IS NULL OR type IS NULL OR province IS NULL
        """
        result = db.execute(text(verify_query_etab))
        count_null_etab = result.scalar()
        
        verify_query_urg = """
        SELECT COUNT(*) FROM urgences_etat_actuel 
        WHERE dms_civiere IS NULL OR dms_ambulatoire IS NULL OR 
              total_patients IS NULL OR patients_en_attente IS NULL OR taux_occupation IS NULL
        """
        result = db.execute(text(verify_query_urg))
        count_null_urg = result.scalar()
        
        logger.info(f"Résultats après correction: {count_null_etab} établissements et {count_null_urg} données d'urgence ont encore des valeurs NULL")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Erreur lors de la correction des valeurs NULL: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        db.close()

def create_missing_tables():
    """
    Crée uniquement les tables manquantes dans la base de données.
    """
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
        
    logger.info(f"Tables existantes: {existing_tables}")
        
    # Obtenir toutes les tables définies dans les modèles
    from app.models.models import Base
    metadata = Base.metadata
    defined_tables = metadata.tables.keys()
        
    logger.info(f"Tables définies dans les modèles: {defined_tables}")
        
    # Identifier les tables manquantes
    missing_tables = set(defined_tables) - set(existing_tables)
    logger.info(f"Tables manquantes: {missing_tables}")
        
    if missing_tables:
        # Créer uniquement les tables manquantes
        for table_name in missing_tables:
            table = metadata.tables[table_name]
            table.create(engine)
            logger.info(f"Table créée: {table_name}")
    else:
        logger.info("Aucune table manquante à créer")

def init_regions():
    """
    Initialise les régions dans la base de données.
    """
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
                # Normaliser le nom sans accents
                from app.utils.encoding_utils import normalize_text
                nom_normalise = normalize_text(region_data["nom"], remove_accents=True)
                        
                region = Region(
                    rss=region_data["rss"],
                    nom=region_data["nom"],
                    nom_normalise=nom_normalise
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
    create_missing_tables()
    init_regions()
    fix_null_values_in_tables()