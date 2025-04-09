"""
Module de planification des tâches pour les mises à jour périodiques des données.
"""
import logging
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from typing import Optional, Dict, List
from sqlalchemy.orm import Session
from sqlalchemy import inspect, func, text
from geoalchemy2 import WKTElement

# IMPORTS DE TES MODULES PERSO
from app.db.database import SessionLocal, engine
from app.models.models import Base, Etablissement, Region, UrgencesEtatActuel, UrgencesHistorique
from app.utils.encoding_utils import read_csv_with_encoding_detection, clean_dataframe, standardize_establishment_names, normalize_text, fix_text_encoding
from app.utils.data_validation import DataValidator, DataProcessor

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/scheduler.log")
    ]
)
logger = logging.getLogger(__name__)

# Créer les répertoires nécessaires
os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# URLs (à adapter si besoin)
URGENCES_URL = "https://www.msss.gouv.qc.ca/professionnels/statistiques/documents/urgences/Releve_horaire_urgences_7jours.csv"
URGENCES_DETAILS_URL = "https://www.msss.gouv.qc.ca/professionnels/statistiques/documents/urgences/Releve_horaire_urgences_7jours_nbpers.csv"

# Définition des régions du Québec (ou ce que tu utilises comme base)
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

# Mappings pour normaliser les provinces
PROVINCE_MAPPING = {
    "qc": "Québec",
    "QC": "Québec",
    "Qc": "Québec",
    "quebec": "Québec",
    "QUEBEC": "Québec",
    "on": "Ontario",
    "ON": "Ontario",
    "bc": "Colombie-Britannique",
    "BC": "Colombie-Britannique",
    "ab": "Alberta",
    "AB": "Alberta",
    "mb": "Manitoba",
    "MB": "Manitoba",
    "sk": "Saskatchewan",
    "SK": "Saskatchewan",
    "ns": "Nouvelle-Écosse",
    "NS": "Nouvelle-Écosse",
    "nb": "Nouveau-Brunswick",
    "NB": "Nouveau-Brunswick",
    "nl": "Terre-Neuve-et-Labrador",
    "NL": "Terre-Neuve-et-Labrador",
    "pe": "Île-du-Prince-Édouard",
    "PE": "Île-du-Prince-Édouard",
    "yt": "Yukon",
    "YT": "Yukon",
    "nt": "Territoires du Nord-Ouest",
    "NT": "Territoires du Nord-Ouest",
    "nu": "Nunavut",
    "NU": "Nunavut"
}


def initialize_database():
    """
    Initialise la base de données en créant toutes les tables nécessaires
    et en ajoutant les données de base comme les régions.
    """
    logger.info("Initialisation de la base de données")

    try:
        # Vérifier les tables existantes
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        logger.info(f"Tables existantes: {existing_tables}")

        # Créer toutes les tables définies dans les modèles si elles n'existent pas
        Base.metadata.create_all(bind=engine)
        logger.info("Structure de la base de données vérifiée et mise à jour")

        # Initialiser les régions si nécessaire
        db = SessionLocal()
        try:
            # Vérifier si des régions existent déjà
            region_count = db.query(Region).count()

            if region_count == 0:
                logger.info("Aucune région trouvée. Création des régions du Québec...")

                # Créer les régions
                for region_data in REGIONS_QUEBEC:
                    nom_normalise = normalize_text(region_data["nom"], remove_accents=True)
                    region = Region(
                        rss=region_data["rss"],
                        nom=region_data["nom"],
                        nom_normalise=nom_normalise
                    )
                    db.add(region)

                db.commit()
                logger.info(f"{len(REGIONS_QUEBEC)} régions créées avec succès")
            else:
                logger.info(f"{region_count} régions existent déjà dans la base de données")

        except Exception as e:
            db.rollback()
            logger.error(f"Erreur lors de l'initialisation des régions: {str(e)}")
        finally:
            db.close()

        logger.info("Initialisation de la base de données terminée avec succès")
        return True

    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation de la base de données: {str(e)}")
        return False

def extraire_info_etablissements(url_odhf: str = "data/odhf_v1.1.csv") -> Dict[str, Dict]:
    """
    Extrait les informations d'établissements depuis le fichier ODHF pour compléter les entrées manquantes.
    
    Args:
        url_odhf: Chemin vers le fichier odhf_v1.1.csv
        
    Returns:
        Dict: Dictionnaire des informations d'établissements
    """
    try:
        # Vérifier si le fichier existe
        if not os.path.exists(url_odhf):
            logger.warning(f"Le fichier {url_odhf} n'existe pas. Impossible d'extraire les infos sur les établissements.")
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
        for i, row in df_odhf_clean.iterrows():
            nom = row.get('facility_name', '').strip()
            if nom:
                # Construire une adresse complète
                adresse = ""
                if not pd.isna(row.get('street_no')) and str(row.get('street_no')).strip():
                    adresse += str(row.get('street_no')).strip() + " "
                if not pd.isna(row.get('street_name')) and str(row.get('street_name')).strip():
                    adresse += str(row.get('street_name')).strip()
                
                # Standardiser la province
                province = str(row.get('province', 'Québec')).strip()
                province = PROVINCE_MAPPING.get(province.lower(), province)
                
                # Standardiser
                nom_normalise = row.get('facility_name_normalise', '').lower().strip()
                
                # Générer un source_id unique
                source_id = f"ODHF-{i+1:06d}"
                
                etablissements_info[nom_normalise] = {
                    'source_id': source_id,
                    'type': row.get('odhf_facility_type', 'Hôpital'),
                    'adresse': adresse.strip() if adresse.strip() else "À compléter",
                    'code_postal': str(row.get('postal_code', '')).strip(),
                    'ville': str(row.get('city', '')).strip(),
                    'province': province,
                    'latitude': row.get('latitude'),
                    'longitude': row.get('longitude')
                }
                
        logger.info(f"{len(etablissements_info)} établissements extraits du fichier ODHF")
        return etablissements_info
    
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des informations d'établissements: {str(e)}")
        return {}


class DataUpdater:
    """
    Classe pour gérer les mises à jour périodiques des données.
    """

    def __init__(self):
        """
        Initialise le gestionnaire de mises à jour.
        """
        self.validator = DataValidator(log_dir="logs")
        self.processor = DataProcessor(self.validator)
        
        # Charger les informations ODHF sur les établissements
        self.etablissements_info = extraire_info_etablissements()

    def download_file(self, url: str, destination: str) -> bool:
        """
        Télécharge un fichier depuis une URL.

        Args:
            url: URL du fichier à télécharger
            destination: Chemin de destination pour le fichier

        Returns:
            bool: True si le téléchargement a réussi, False sinon
        """
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(destination, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Fichier téléchargé avec succès: {destination}")
            return True

        except Exception as e:
            logger.error(f"Erreur lors du téléchargement du fichier {url}: {str(e)}")
            return False

    def update_urgences_data(self) -> None:
        """
        Met à jour les données d'urgence.
        """
        logger.info("Début de la mise à jour des données d'urgence")

        # Télécharger les fichiers
        urgences_file = "data/Releve_horaire_urgences_7jours.csv"
        urgences_details_file = "data/Releve_horaire_urgences_7jours_nbpers.csv"

        if not self.download_file(URGENCES_URL, urgences_file):
            logger.error("Échec du téléchargement des données d'urgence, abandon de la mise à jour")
            return

        # (Facultatif) Télécharge aussi le fichier détaillé si nécessaire
        if not self.download_file(URGENCES_DETAILS_URL, urgences_details_file):
            logger.warning("Échec du téléchargement des données détaillées d'urgence, on continue quand même...")

        # Traiter les données
        try:
            # Lire le fichier avec détection d'encodage
            df_urgences = read_csv_with_encoding_detection(urgences_file)

            # Nettoyer et valider les données
            df_urgences = self.processor.process_urgences_data(df_urgences)

            # Mettre à jour la base de données
            self._update_database(df_urgences)

            logger.info("Mise à jour des données d'urgence terminée avec succès")

        except Exception as e:
            logger.error(f"Erreur lors du traitement des données d'urgence: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _update_database(self, df_urgences: pd.DataFrame) -> None:
        """
        Met à jour la base de données avec les nouvelles données d'urgence.

        Args:
            df_urgences: DataFrame contenant les données d'urgence
        """
        db = SessionLocal()

        try:
            # Date d'extraction actuelle
            date_extraction = datetime.now()

            # Parcourir les données d'urgence
            for _, row in df_urgences.iterrows():
                # Trouver l'établissement correspondant
                etablissement = self._find_etablissement(db, row)

                if etablissement:
                    # Archiver les données actuelles dans l'historique
                    self._archive_current_data(db, etablissement.id)

                    # Mettre à jour ou créer les données actuelles
                    self._update_current_data(db, etablissement.id, row, date_extraction)

            # Valider les changements
            db.commit()
            logger.info(f"Base de données mise à jour avec succès à {date_extraction}")

        except Exception as e:
            db.rollback()
            logger.error(f"Erreur lors de la mise à jour de la base de données: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            db.close()

    def _find_etablissement(self, db: Session, row: pd.Series) -> Optional[Etablissement]:
        """
        Trouve l'établissement correspondant aux données d'urgence.
        Si l'établissement n'est pas trouvé, il est créé automatiquement.

        Args:
            db: Session de base de données
            row: Ligne de données d'urgence

        Returns:
            Optional[Etablissement]: Établissement trouvé ou créé
        """
        # Extraire le nom de l'établissement et de l'installation
        nom_etablissement = row.get('nom_etablissement', '')
        nom_installation = row.get('nom_installation', '')
        no_permis = str(row.get('no_permis_installation', ''))  # Convertir explicitement en string

        # Corriger l'encodage
        nom_etablissement = fix_text_encoding(nom_etablissement)
        nom_installation = fix_text_encoding(nom_installation)

        # Normaliser (minuscules, etc.)
        nom_etablissement_std = normalize_text(nom_etablissement, remove_accents=True)
        nom_installation_std = normalize_text(nom_installation, remove_accents=True)

        etablissement = None
        try:
            # 1) Cherche par numéro de permis si disponible
            if no_permis and no_permis.strip():
                etablissement = db.query(Etablissement).filter(
                    Etablissement.no_permis_installation == no_permis
                ).first()
                
            # 2) Cherche par installation
            if not etablissement and nom_installation_std:
                etablissement = db.query(Etablissement).filter(
                    Etablissement.nom_installation_normalise.ilike(f"%{nom_installation_std}%")
                ).first()

            # 3) Sinon, cherche par nom d'établissement
            if not etablissement and nom_etablissement_std:
                etablissement = db.query(Etablissement).filter(
                    Etablissement.nom_etablissement_normalise.ilike(f"%{nom_etablissement_std}%")
                ).first()

        except Exception as e:
            db.rollback()  # Important: faire un rollback en cas d'erreur
            logger.error(f"Erreur lors de la recherche d'établissement: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

        # Si pas trouvé, on le crée
        if not etablissement:
            logger.info(f"Création automatique de l'établissement: {nom_etablissement} / {nom_installation}")

            # Chercher une région correspondante ou par défaut
            region_id = self._find_region_id(db, row.get('rss', ''), row.get('region', ''))

            # Rechercher des informations complémentaires dans ODHF
            etablissement_info = self._get_etablissement_info(nom_installation_std, nom_etablissement_std)

            try:
                # Créer un point géographique si latitude/longitude disponibles
                point_geo = None
                if etablissement_info and 'latitude' in etablissement_info and 'longitude' in etablissement_info:
                    lat = etablissement_info.get('latitude')
                    lon = etablissement_info.get('longitude')
                    if lat and lon and not pd.isna(lat) and not pd.isna(lon):
                        try:
                            point_geo = WKTElement(f'POINT({lon} {lat})', srid=4326)
                        except Exception as e:
                            logger.warning(f"Erreur lors de la création du point géographique: {str(e)}")

                # Standardiser la province
                province = etablissement_info.get('province', 'Québec')
                if province.lower() in PROVINCE_MAPPING:
                    province = PROVINCE_MAPPING[province.lower()]

                # Créer le nouvel établissement
                etablissement = Etablissement(
                    source_id=etablissement_info.get('source_id', f"SRC-{datetime.now().strftime('%Y%m%d')}-{no_permis}"),
                    nom_etablissement=nom_etablissement,
                    nom_etablissement_normalise=nom_etablissement_std,
                    nom_installation=nom_installation,
                    nom_installation_normalise=nom_installation_std,
                    no_permis_installation=no_permis,  # Déjà converti en string
                    region_id=region_id,
                    type=etablissement_info.get('type', 'Hôpital'),
                    adresse=etablissement_info.get('adresse', 'À compléter'),
                    code_postal=etablissement_info.get('code_postal', ''),
                    ville=etablissement_info.get('ville', 'À compléter'),
                    province=province,
                    point_geo=point_geo,
                    date_maj=datetime.now()
                )
                db.add(etablissement)
                db.flush()  # Pour obtenir l'ID sans commit
                logger.info(f"Établissement créé avec l'ID: {etablissement.id}")
            except Exception as e:
                db.rollback()
                logger.error(f"Erreur lors de la création de l'établissement: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return None

        return etablissement

    def _find_region_id(self, db: Session, rss: str, region_name: str) -> int:
        """
        Trouve l'ID de la région correspondante.
        
        Args:
            db: Session de base de données
            rss: Code RSS de la région
            region_name: Nom de la région
            
        Returns:
            int: ID de la région trouvée ou région par défaut (Montréal)
        """
        region = None
        
        try:
            # 1) Chercher par RSS
            rss = str(rss).strip()
            if rss:
                region = db.query(Region).filter(Region.rss == rss).first()
                
            # 2) Chercher par nom
            if not region and region_name:
                region_name_norm = normalize_text(region_name, remove_accents=True)
                region = db.query(Region).filter(Region.nom_normalise.ilike(f"%{region_name_norm}%")).first()
            
            # 3) Par défaut: Montréal
            if not region:
                region = db.query(Region).filter(Region.rss == "06").first()  # Montréal
                
            # 4) Dernière chance: prendre la première région disponible
            if not region:
                region = db.query(Region).first()
                
            return region.id if region else None
            
        except Exception as e:
            logger.warning(f"Erreur lors de la recherche de région: {str(e)}")
            db.rollback()
            return None

    def _get_etablissement_info(self, nom_installation_norm: str, nom_etablissement_norm: str) -> Dict:
        """
        Récupère les informations complémentaires sur un établissement depuis ODHF.
        
        Args:
            nom_installation_norm: Nom normalisé de l'installation
            nom_etablissement_norm: Nom normalisé de l'établissement
            
        Returns:
            Dict: Informations sur l'établissement ou valeurs par défaut
        """
        # Recherche par mots-clés dans les noms normalisés
        for nom_odhf, info in self.etablissements_info.items():
            # Recherche de correspondance approximative
            if (nom_installation_norm and nom_installation_norm in nom_odhf) or \
               (nom_etablissement_norm and nom_etablissement_norm in nom_odhf):
                return info
                
        # Valeurs par défaut si aucune correspondance trouvée
        return {
            'source_id': f"GEN-{datetime.now().strftime('%Y%m%d')}-{len(self.etablissements_info) + 1:04d}",
            'type': 'Hôpital',
            'adresse': 'À compléter',
            'code_postal': '',
            'ville': 'À compléter',
            'province': 'Québec'
        }

    def _archive_current_data(self, db: Session, etablissement_id: int) -> None:
        """
        Archive les données actuelles d'urgence dans l'historique.

        Args:
            db: Session de base de données
            etablissement_id: ID de l'établissement
        """
        try:
            current_data = db.query(UrgencesEtatActuel).filter(
                UrgencesEtatActuel.etablissement_id == etablissement_id
            ).first()

            if current_data:
                historique = UrgencesHistorique(
                    etablissement_id=current_data.etablissement_id,
                    civieres_fonctionnelles=current_data.civieres_fonctionnelles,
                    civieres_occupees=current_data.civieres_occupees,
                    patients_24h=current_data.patients_24h,
                    patients_48h=current_data.patients_48h,
                    total_patients=current_data.total_patients,
                    patients_en_attente=current_data.patients_en_attente,
                    dms_civiere=current_data.dms_civiere,
                    dms_ambulatoire=current_data.dms_ambulatoire,
                    taux_occupation=current_data.taux_occupation,
                    date_extraction=current_data.date_extraction,
                    date_maj=current_data.date_maj,
                    statut_validation=current_data.statut_validation
                )
                db.add(historique)
                logger.debug(f"Données archivées pour l'établissement {etablissement_id}")

        except Exception as e:
            logger.error(f"Erreur lors de l'archivage des données pour l'établissement {etablissement_id}: {str(e)}")

    def _update_current_data(self, db: Session, etablissement_id: int, row: pd.Series, date_extraction: datetime) -> None:
        """
        Met à jour ou crée les données actuelles d'urgence.

        Args:
            db: Session de base de données
            etablissement_id: ID de l'établissement
            row: Ligne de données d'urgence
            date_extraction: Date d'extraction des données
        """
        try:
            current_data = db.query(UrgencesEtatActuel).filter(
                UrgencesEtatActuel.etablissement_id == etablissement_id
            ).first()

            if not current_data:
                current_data = UrgencesEtatActuel(etablissement_id=etablissement_id)
                db.add(current_data)

            # Traiter les valeurs numériques avec des valeurs par défaut
            current_data.civieres_fonctionnelles = float(row.get('civieres_fonctionnelles', 0))
            current_data.civieres_occupees = float(row.get('civieres_occupees', 0))
            current_data.patients_24h = float(row.get('patients_24h', 0))
            current_data.patients_48h = float(row.get('patients_48h', 0))
            current_data.total_patients = float(row.get('total_patients', 0))
            current_data.patients_en_attente = float(row.get('patients_en_attente', 0))
            
            # Traiter les durées moyennes de séjour (DMS)
            try:
                dms_civiere = row.get('dms_civiere')
                if isinstance(dms_civiere, str):
                    # Convertir format "hh:mm" en nombre décimal d'heures
                    if ":" in dms_civiere:
                        h, m = dms_civiere.split(":")
                        dms_civiere = float(h) + float(m)/60
                    else:
                        # Convertir nombre avec virgule
                        dms_civiere = float(dms_civiere.replace(',', '.'))
                current_data.dms_civiere = float(dms_civiere) if dms_civiere is not None else 0
            except Exception as e:
                logger.warning(f"Erreur lors de la conversion de DMS civière: {str(e)}")
                current_data.dms_civiere = 0
                
            try:
                dms_ambulatoire = row.get('dms_ambulatoire')
                if isinstance(dms_ambulatoire, str):
                    # Convertir format "hh:mm" en nombre décimal d'heures
                    if ":" in dms_ambulatoire:
                        h, m = dms_ambulatoire.split(":")
                        dms_ambulatoire = float(h) + float(m)/60
                    else:
                        # Convertir nombre avec virgule
                        dms_ambulatoire = float(dms_ambulatoire.replace(',', '.'))
                current_data.dms_ambulatoire = float(dms_ambulatoire) if dms_ambulatoire is not None else 0
            except Exception as e:
                logger.warning(f"Erreur lors de la conversion de DMS ambulatoire: {str(e)}")
                current_data.dms_ambulatoire = 0

            # Calculer le taux d'occupation
            if (current_data.civieres_fonctionnelles
                and current_data.civieres_fonctionnelles > 0
                and current_data.civieres_occupees is not None):
                current_data.taux_occupation = (current_data.civieres_occupees / current_data.civieres_fonctionnelles) * 100
            else:
                current_data.taux_occupation = 0  # Valeur par défaut

            current_data.date_extraction = date_extraction
            current_data.date_maj = datetime.now()
            current_data.statut_validation = "validé"

            logger.debug(f"Données mises à jour pour l'établissement {etablissement_id}")

        except Exception as e:
            logger.error(f"Erreur lors de la mise à jour des données pour l'établissement {etablissement_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def fix_null_values(self):
        """
        Corrige les valeurs NULL dans les tables de la base de données.
        """
        logger.info("Correction des valeurs NULL dans les tables")
        db = SessionLocal()
        
        try:
            # 1. Correction des établissements
            etablissements_avec_null = db.query(Etablissement).filter(
                (Etablissement.adresse.is_(None)) |
                (Etablissement.ville.is_(None)) |
                (Etablissement.type.is_(None)) |
                (Etablissement.source_id.is_(None))
            ).all()
            
            logger.info(f"Correction de {len(etablissements_avec_null)} établissements avec des valeurs NULL")
            
            for etab in etablissements_avec_null:
                # Rechercher des informations complémentaires dans ODHF
                info = self._get_etablissement_info(
                    etab.nom_installation_normalise or "", 
                    etab.nom_etablissement_normalise or ""
                )
                
                # Mise à jour des champs NULL
                if etab.adresse is None:
                    etab.adresse = info.get('adresse', 'À compléter')
                if etab.ville is None:
                    etab.ville = info.get('ville', 'À compléter')
                if etab.type is None:
                    etab.type = info.get('type', 'Hôpital')
                if etab.code_postal is None:
                    etab.code_postal = info.get('code_postal', '')
                if etab.province is None:
                    etab.province = 'Québec'
                if etab.source_id is None:
                    etab.source_id = info.get('source_id', f"GEN-{datetime.now().strftime('%Y%m%d')}-{etab.id:04d}")
                
                # Standardiser la province
                if etab.province in PROVINCE_MAPPING:
                    etab.province = PROVINCE_MAPPING[etab.province]
                
                # Mise à jour des coordonnées géographiques si disponibles
                if etab.point_geo is None and 'latitude' in info and 'longitude' in info:
                    lat = info.get('latitude')
                    lon = info.get('longitude')
                    if lat and lon and not pd.isna(lat) and not pd.isna(lon):
                        try:
                            etab.point_geo = WKTElement(f'POINT({lon} {lat})', srid=4326)
                        except Exception as e:
                            logger.warning(f"Erreur lors de la création du point géographique: {str(e)}")
                
                etab.date_maj = datetime.now()
            
            # 2. Correction des données d'urgence
            urgences_avec_null = db.query(UrgencesEtatActuel).filter(
                (UrgencesEtatActuel.dms_civiere.is_(None)) |
                (UrgencesEtatActuel.dms_ambulatoire.is_(None)) |
                (UrgencesEtatActuel.total_patients.is_(None)) |
                (UrgencesEtatActuel.patients_en_attente.is_(None)) |
                (UrgencesEtatActuel.taux_occupation.is_(None))
            ).all()
            
            logger.info(f"Correction de {len(urgences_avec_null)} données d'urgence avec des valeurs NULL")
            
            for urg in urgences_avec_null:
                # Mise à jour des champs NULL avec valeurs par défaut
                if urg.dms_civiere is None:
                    urg.dms_civiere = 0
                if urg.dms_ambulatoire is None:
                    urg.dms_ambulatoire = 0
                if urg.total_patients is None:
                    urg.total_patients = 0
                if urg.patients_en_attente is None:
                    urg.patients_en_attente = 0
                    
                # Recalculer le taux d'occupation
                if urg.taux_occupation is None:
                    if urg.civieres_fonctionnelles and urg.civieres_fonctionnelles > 0 and urg.civieres_occupees is not None:
                        urg.taux_occupation = (urg.civieres_occupees / urg.civieres_fonctionnelles) * 100
                    else:
                        urg.taux_occupation = 0
                
                urg.date_maj = datetime.now()
            
            # Valider les changements
            db.commit()
            logger.info("Correction des valeurs NULL terminée avec succès")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur lors de la correction des valeurs NULL: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            db.close()


def start_scheduler():
    """
    Démarre le planificateur de tâches.
    """
    logger.info("Démarrage du planificateur de tâches")

    # Initialiser la base de données
    initialize_database()

    # Créer le planificateur
    scheduler = BackgroundScheduler()

    # Créer le gestionnaire de mises à jour
    updater = DataUpdater()
    
    # Exécuter la correction des valeurs NULL au démarrage
    updater.fix_null_values()

    # Ajouter la tâche de mise à jour horaire des données d'urgence (ex: toutes les heures pile)
    scheduler.add_job(
        updater.update_urgences_data,
        trigger=CronTrigger(minute=0),
        id="update_urgences",
        name="Mise à jour des données d'urgence",
        replace_existing=True
    )
    
    # Ajouter la tâche de correction des valeurs NULL (tous les jours à minuit)
    scheduler.add_job(
        updater.fix_null_values,
        trigger=CronTrigger(hour=0, minute=0),
        id="fix_null_values",
        name="Correction des valeurs NULL",
        replace_existing=True
    )

    # Démarrer le planificateur
    scheduler.start()
    logger.info("Planificateur de tâches démarré")

    return scheduler


if __name__ == "__main__":
    # Démarrer le planificateur
    scheduler = start_scheduler()

    try:
        # Exécuter une mise à jour initiale
        updater = DataUpdater()
        updater.update_urgences_data()

        # Boucle pour garder le scheduler actif
        import time
        while True:
            time.sleep(60)

    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Planificateur de tâches arrêté")