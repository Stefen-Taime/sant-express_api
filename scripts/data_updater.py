"""
Module amélioré pour la mise à jour des données avec vérification et création des tables nécessaires.
"""
import logging
import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import text, inspect
from sqlalchemy.exc import OperationalError, ProgrammingError
import time

from app.db.database import SessionLocal, engine, Base
from app.models.models import Region, Etablissement, UrgencesEtatActuel, UrgencesHistorique
from app.utils.encoding_utils import read_csv_with_encoding_detection, normalize_text, fix_text_encoding
from app.utils.data_validation import DataValidator, DataProcessor

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/data_updater.log")
    ]
)
logger = logging.getLogger(__name__)

# Créer les répertoires nécessaires
os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# URLs des données d'urgence
URGENCES_URL = "https://www.msss.gouv.qc.ca/professionnels/statistiques/documents/urgences/Releve_horaire_urgences_7jours.csv"
URGENCES_DETAILS_URL = "https://www.msss.gouv.qc.ca/professionnels/statistiques/documents/urgences/Releve_horaire_urgences_7jours_nbpers.csv"

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

class DataUpdater:
    """
    Classe améliorée pour gérer les mises à jour périodiques des données.
    """
    
    def __init__(self):
        """
        Initialise le gestionnaire de mises à jour.
        """
        self.validator = DataValidator(log_dir="logs")
        self.processor = DataProcessor(self.validator)
        
        # S'assurer que les tables et données de base existent
        self.ensure_database_setup()
    
    def wait_for_database(self, max_retries=30, delay=5):
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
    
    def ensure_database_setup(self):
        """
        S'assure que la base de données est correctement configurée.
        Crée les tables si elles n'existent pas et initialise les données de base.
        """
        logger.info("Vérification de la configuration de la base de données...")
        
        # Attendre que la base de données soit disponible
        if not self.wait_for_database():
            logger.error("Base de données non disponible. Impossible de continuer.")
            return False
        
        try:
            # Créer toutes les tables définies dans les modèles
            Base.metadata.create_all(bind=engine)
            logger.info("Tables créées ou déjà existantes.")
            
            # Vérifier si la colonne geom existe dans la table regions (pour PostGIS)
            inspector = inspect(engine)
            if 'regions' in inspector.get_table_names():
                columns = [c['name'] for c in inspector.get_columns('regions')]
                if 'geom' not in columns:
                    logger.info("Ajout de la colonne geom à la table regions")
                    try:
                        with engine.connect() as connection:
                            connection.execute(text("ALTER TABLE regions ADD COLUMN IF NOT EXISTS geom geometry(MULTIPOLYGON, 4326);"))
                            connection.commit()
                        logger.info("Colonne geom ajoutée avec succès")
                    except Exception as e:
                        logger.warning(f"Erreur lors de l'ajout de la colonne geom: {str(e)}")
            
            # Initialiser les régions si nécessaire
            self._initialize_regions()
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la configuration de la base de données: {str(e)}")
            return False
    
    def _initialize_regions(self):
        """
        Initialise les régions du Québec dans la base de données.
        """
        db = SessionLocal()
        try:
            # Vérifier si des régions existent déjà
            regions_count = db.query(Region).count()
            
            if regions_count > 0:
                logger.info(f"{regions_count} régions existent déjà dans la base de données.")
                return
            
            logger.info("Initialisation des régions du Québec...")
            
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
            
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur lors de l'initialisation des régions: {str(e)}")
        finally:
            db.close()
    
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
        
        # S'assurer que la base de données est correctement configurée
        if not self.ensure_database_setup():
            logger.error("La base de données n'est pas correctement configurée. Abandon de la mise à jour.")
            return
        
        # Télécharger les fichiers
        urgences_file = "data/Releve_horaire_urgences_7jours.csv"
        urgences_details_file = "data/Releve_horaire_urgences_7jours_nbpers.csv"
        
        if not self.download_file(URGENCES_URL, urgences_file):
            logger.error("Échec du téléchargement des données d'urgence, abandon de la mise à jour")
            return
        
        if not self.download_file(URGENCES_DETAILS_URL, urgences_details_file):
            logger.warning("Échec du téléchargement des données détaillées d'urgence, poursuite avec les données principales uniquement")
        
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
    
    def _update_database(self, df_urgences: pd.DataFrame) -> None:
        """
        Met à jour la base de données avec les nouvelles données d'urgence.
        
        Args:
            df_urgences: DataFrame contenant les données d'urgence
        """
        # Créer une session de base de données
        db = SessionLocal()
        
        try:
            # Date d'extraction actuelle
            date_extraction = datetime.now()
            
            # Parcourir les données d'urgence
            for _, row in df_urgences.iterrows():
                try:
                    # Trouver l'établissement correspondant
                    etablissement = self._find_etablissement(db, row)
                    
                    if etablissement:
                        # Archiver les données actuelles dans l'historique
                        self._archive_current_data(db, etablissement.id)
                        
                        # Mettre à jour ou créer les données actuelles
                        self._update_current_data(db, etablissement.id, row, date_extraction)
                except Exception as e:
                    logger.error(f"Erreur lors du traitement de la ligne {row.get('nom_etablissement', '')}: {str(e)}")
                    # Continuer avec la ligne suivante au lieu d'abandonner complètement
                    db.rollback()
            
            # Valider les changements
            db.commit()
            logger.info(f"Base de données mise à jour avec succès à {date_extraction}")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur lors de la mise à jour de la base de données: {str(e)}")
            
        finally:
            db.close()
    
    def _find_etablissement(self, db, row):
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
        
        # Standardiser les noms pour la recherche
        nom_etablissement = fix_text_encoding(nom_etablissement)
        nom_installation = fix_text_encoding(nom_installation)
        
        nom_etablissement_std = normalize_text(nom_etablissement, remove_accents=True)
        nom_installation_std = normalize_text(nom_installation, remove_accents=True)
        
        # Rechercher l'établissement
        etablissement = None
        
        try:
            # Essayer d'abord avec le nom de l'installation
            if nom_installation_std:
                etablissement = db.query(Etablissement).filter(
                    Etablissement.nom_installation_normalise.ilike(f"%{nom_installation_std}%")
                ).first()
            
            # Si non trouvé, essayer avec le nom de l'établissement
            if not etablissement and nom_etablissement_std:
                etablissement = db.query(Etablissement).filter(
                    Etablissement.nom_etablissement_normalise.ilike(f"%{nom_etablissement_std}%")
                ).first()
        except Exception as e:
            logger.error(f"Erreur lors de la recherche d'établissement: {str(e)}")
            raise
        
        # Si toujours pas trouvé, créer un nouvel établissement
        if not etablissement:
            logger.info(f"Création automatique de l'établissement: {nom_etablissement} / {nom_installation}")
            
            # Chercher une région par défaut (Montréal si disponible, sinon la première)
            region_id = None
            try:
                # Vérifier d'abord que la table regions existe
                inspector = inspect(engine)
                if 'regions' not in inspector.get_table_names():
                    raise Exception("La table regions n'existe pas encore.")
                
                region = db.query(Region).filter(Region.rss == "06").first()  # Montréal
                if not region:
                    region = db.query(Region).first()
                
                region_id = region.id if region else None
            except Exception as e:
                logger.warning(f"Erreur lors de la recherche de région: {str(e)}")
                # Plutôt que de continuer avec une transaction échouée,
                # on ne tentera pas de créer l'établissement sans région
                raise
            
            # Créer le nouvel établissement
            try:
                etablissement = Etablissement(
                    nom_etablissement=nom_etablissement,
                    nom_etablissement_normalise=nom_etablissement_std,
                    nom_installation=nom_installation,
                    nom_installation_normalise=nom_installation_std,
                    region_id=region_id,
                    province="Québec",
                    date_maj=datetime.now()
                )
                
                db.add(etablissement)
                db.flush()  # Flush mais pas commit pour pouvoir faire un rollback en cas d'erreur
                logger.info(f"Établissement créé avec l'ID: {etablissement.id}")
            except Exception as e:
                logger.error(f"Erreur lors de la création de l'établissement: {str(e)}")
                raise
        
        return etablissement
    
    def _archive_current_data(self, db, etablissement_id):
        """
        Archive les données actuelles d'urgence dans l'historique.
        
        Args:
            db: Session de base de données
            etablissement_id: ID de l'établissement
        """
        try:
            # Récupérer les données actuelles
            current_data = db.query(UrgencesEtatActuel).filter(
                UrgencesEtatActuel.etablissement_id == etablissement_id
            ).first()
            
            if current_data:
                # Créer un enregistrement historique
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
            raise
    
    def _update_current_data(self, db, etablissement_id, row, date_extraction):
        """
        Met à jour ou crée les données actuelles d'urgence.
        
        Args:
            db: Session de base de données
            etablissement_id: ID de l'établissement
            row: Ligne de données d'urgence
            date_extraction: Date d'extraction des données
        """
        try:
            # Récupérer les données actuelles ou créer un nouvel enregistrement
            current_data = db.query(UrgencesEtatActuel).filter(
                UrgencesEtatActuel.etablissement_id == etablissement_id
            ).first()
            
            if not current_data:
                current_data = UrgencesEtatActuel(etablissement_id=etablissement_id)
                db.add(current_data)
            
            # Mettre à jour les données
            current_data.civieres_fonctionnelles = row.get('civieres_fonctionnelles')
            current_data.civieres_occupees = row.get('civieres_occupees')
            current_data.patients_24h = row.get('patients_24h')
            current_data.patients_48h = row.get('patients_48h')
            current_data.total_patients = row.get('total_patients')
            current_data.patients_en_attente = row.get('patients_en_attente')
            current_data.dms_civiere = row.get('dms_civiere')
            current_data.dms_ambulatoire = row.get('dms_ambulatoire')
            
            # Calculer le taux d'occupation
            if current_data.civieres_fonctionnelles and current_data.civieres_fonctionnelles > 0:
                current_data.taux_occupation = (current_data.civieres_occupees / current_data.civieres_fonctionnelles) * 100
            else:
                current_data.taux_occupation = None
            
            current_data.date_extraction = date_extraction
            current_data.date_maj = datetime.now()
            current_data.statut_validation = "validé"
            
            logger.debug(f"Données mises à jour pour l'établissement {etablissement_id}")
        except Exception as e:
            logger.error(f"Erreur lors de la mise à jour des données pour l'établissement {etablissement_id}: {str(e)}")
            raise


# Fonction d'entrée pour l'utilisation directe du script
def update_data():
    """
    Fonction d'entrée pour la mise à jour des données.
    """
    updater = DataUpdater()
    updater.update_urgences_data()


if __name__ == "__main__":
    update_data()