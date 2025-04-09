"""
Module pour la validation des données et la journalisation des anomalies.
"""
import logging
import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Callable

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Classe pour valider les données selon des règles métier spécifiques.
    """
    
    def __init__(self, log_dir: str = "logs"):
        """
        Initialise le validateur de données.
        
        Args:
            log_dir: Répertoire pour stocker les journaux d'anomalies
        """
        self.log_dir = log_dir
        self.anomalies = []
        
        # Créer le répertoire de logs s'il n'existe pas
        os.makedirs(log_dir, exist_ok=True)
        
        # Configurer un logger spécifique pour les anomalies
        self.anomaly_logger = self._setup_anomaly_logger()
    
    def _setup_anomaly_logger(self) -> logging.Logger:
        """
        Configure un logger spécifique pour les anomalies.
        
        Returns:
            Logger: Logger configuré pour les anomalies
        """
        anomaly_logger = logging.getLogger("anomalies")
        
        # Éviter la duplication des handlers
        if not anomaly_logger.handlers:
            log_file = os.path.join(self.log_dir, f"anomalies_{datetime.now().strftime('%Y%m%d')}.log")
            file_handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            anomaly_logger.addHandler(file_handler)
            anomaly_logger.setLevel(logging.INFO)
        
        return anomaly_logger
    
    def validate_numeric_range(self, df: pd.DataFrame, column: str, min_val: float, max_val: float) -> pd.DataFrame:
        """
        Valide que les valeurs d'une colonne sont dans une plage numérique.
        
        Args:
            df: DataFrame à valider
            column: Nom de la colonne à valider
            min_val: Valeur minimale acceptable
            max_val: Valeur maximale acceptable
            
        Returns:
            DataFrame: DataFrame avec les lignes valides
        """
        if column not in df.columns:
            self._log_anomaly("validation", f"Colonne {column} non trouvée dans le DataFrame")
            return df
        
        df[column] = pd.to_numeric(df[column], errors='coerce')
        invalid_mask = (df[column] < min_val) | (df[column] > max_val) | df[column].isna()
        invalid_rows = df[invalid_mask]
        
        if not invalid_rows.empty:
            self._log_anomaly(
                "validation", 
                f"Trouvé {len(invalid_rows)} lignes avec des valeurs hors plage dans la colonne {column}",
                invalid_rows
            )
        
        return df[~invalid_mask]
    
    def validate_required_fields(self, df: pd.DataFrame, required_columns: List[str]) -> pd.DataFrame:
        """
        Valide que les champs requis ne sont pas vides.
        
        Args:
            df: DataFrame à valider
            required_columns: Liste des colonnes requises
            
        Returns:
            DataFrame: DataFrame avec les lignes valides
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self._log_anomaly("validation", f"Colonnes requises manquantes: {missing_columns}")
            # On ne peut valider que les colonnes existantes
            required_columns = [col for col in required_columns if col in df.columns]
        
        if not required_columns:
            return df
        
        invalid_mask = df[required_columns].isna().any(axis=1)
        invalid_rows = df[invalid_mask]
        
        if not invalid_rows.empty:
            self._log_anomaly(
                "validation", 
                f"Trouvé {len(invalid_rows)} lignes avec des valeurs manquantes dans les champs requis",
                invalid_rows
            )
        
        return df[~invalid_mask]
    
    def validate_date_format(self, df: pd.DataFrame, date_column: str, date_format: str = "%Y-%m-%d") -> pd.DataFrame:
        """
        Valide que les dates sont dans un format valide.
        
        Args:
            df: DataFrame à valider
            date_column: Nom de la colonne de date à valider
            date_format: Format de date attendu
            
        Returns:
            DataFrame: DataFrame avec les lignes valides et la colonne convertie en datetime
        """
        if date_column not in df.columns:
            self._log_anomaly("validation", f"Colonne de date {date_column} non trouvée dans le DataFrame")
            return df
        
        df_copy = df.copy()
        try:
            df_copy[date_column] = pd.to_datetime(df_copy[date_column], format=date_format, errors='coerce')
            invalid_mask = df_copy[date_column].isna()
            invalid_rows = df[invalid_mask]
            
            if not invalid_rows.empty:
                self._log_anomaly(
                    "validation", 
                    f"Trouvé {len(invalid_rows)} lignes avec des dates invalides dans la colonne {date_column}",
                    invalid_rows
                )
            
            return df_copy[~invalid_mask]
        except Exception as e:
            self._log_anomaly("validation", f"Erreur lors de la validation des dates: {str(e)}")
            return df_copy
    
    def validate_with_custom_rule(self, df: pd.DataFrame, rule_func: Callable[[pd.DataFrame], pd.Series], rule_name: str) -> pd.DataFrame:
        """
        Valide les données avec une fonction de règle personnalisée.
        
        Args:
            df: DataFrame à valider
            rule_func: Fonction qui prend un DataFrame et retourne une Series booléenne (True pour valide)
            rule_name: Nom de la règle pour la journalisation
            
        Returns:
            DataFrame: DataFrame avec les lignes valides
        """
        try:
            valid_mask = rule_func(df)
            if not isinstance(valid_mask, pd.Series):
                self._log_anomaly("validation", f"La fonction de règle {rule_name} n'a pas retourné une Series")
                return df
            
            invalid_rows = df[~valid_mask]
            if not invalid_rows.empty:
                self._log_anomaly(
                    "validation", 
                    f"Trouvé {len(invalid_rows)} lignes ne respectant pas la règle {rule_name}",
                    invalid_rows
                )
            
            return df[valid_mask]
        except Exception as e:
            self._log_anomaly("validation", f"Erreur lors de l'application de la règle {rule_name}: {str(e)}")
            return df
    
    def _log_anomaly(self, anomaly_type: str, message: str, data: Optional[pd.DataFrame] = None) -> None:
        """
        Journalise une anomalie.
        
        Args:
            anomaly_type: Type d'anomalie (validation, encodage, etc.)
            message: Description de l'anomalie
            data: Données associées à l'anomalie (optionnel)
        """
        anomaly = {
            "timestamp": datetime.now().isoformat(),
            "type": anomaly_type,
            "message": message
        }
        
        self.anomalies.append(anomaly)
        self.anomaly_logger.warning(f"{anomaly_type}: {message}")
        
        if data is not None and not data.empty:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = f"{anomaly_type}_{timestamp}.csv"
                file_path = os.path.join(self.log_dir, file_name)
                data.to_csv(file_path, index=False)
                anomaly["data_file"] = file_path
                self.anomaly_logger.info(f"Données d'anomalie sauvegardées dans {file_path}")
            except Exception as e:
                self.anomaly_logger.error(f"Erreur lors de la sauvegarde des données d'anomalie: {str(e)}")
    
    def save_anomaly_report(self) -> str:
        """
        Sauvegarde un rapport des anomalies détectées.
        
        Returns:
            str: Chemin vers le fichier de rapport
        """
        if not self.anomalies:
            logger.info("Aucune anomalie à rapporter")
            return ""
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"anomaly_report_{timestamp}.json"
            file_path = os.path.join(self.log_dir, file_name)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(self.anomalies, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Rapport d'anomalies sauvegardé dans {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du rapport d'anomalies: {str(e)}")
            return ""
    
    def get_anomaly_summary(self) -> Dict[str, int]:
        """
        Retourne un résumé des anomalies par type.
        
        Returns:
            Dict[str, int]: Nombre d'anomalies par type
        """
        summary = {}
        for anomaly in self.anomalies:
            anomaly_type = anomaly["type"]
            summary[anomaly_type] = summary.get(anomaly_type, 0) + 1
        return summary


class DataProcessor:
    """
    Classe pour traiter les données avec validation et journalisation intégrées.
    """
    
    def __init__(self, validator: DataValidator):
        """
        Initialise le processeur de données.
        
        Args:
            validator: Instance de DataValidator pour la validation des données
        """
        self.validator = validator
    
    def process_etablissements_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Traite les données d'établissements avec validation et nettoyage.
        
        Args:
            df: DataFrame contenant les données d'établissements
            
        Returns:
            DataFrame: DataFrame traité
        """
        from .encoding_utils import clean_dataframe, standardize_establishment_names
        
        # Exemple de colonnes requises
        required_columns = ["nom_etablissement"]
        
        # Nettoyer le contenu texte
        text_columns = [col for col in df.columns if df[col].dtype == 'object']
        df_clean = clean_dataframe(df, text_columns)
        
        # Standardiser
        df_clean = standardize_establishment_names(df_clean, "nom_etablissement")
        if "nom_installation" in df_clean.columns:
            df_clean = standardize_establishment_names(df_clean, "nom_installation")
        
        # Valider les champs requis
        df_valid = self.validator.validate_required_fields(df_clean, required_columns)
        
        # S'assurer que les colonnes importantes ne sont pas NULL
        if "adresse" not in df_valid.columns:
            df_valid["adresse"] = "À compléter"
        else:
            df_valid["adresse"] = df_valid["adresse"].fillna("À compléter")
            
        if "ville" not in df_valid.columns:
            df_valid["ville"] = "À compléter"
        else:
            df_valid["ville"] = df_valid["ville"].fillna("À compléter")
            
        if "province" not in df_valid.columns:
            df_valid["province"] = "Québec"
        else:
            # Standardiser les provinces
            province_mapping = {
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
            
            df_valid["province"] = df_valid["province"].apply(
                lambda x: province_mapping.get(x, x) if x in province_mapping else x
            )
            df_valid["province"] = df_valid["province"].fillna("Québec")
            
        if "type" not in df_valid.columns:
            df_valid["type"] = "Hôpital"
        else:
            df_valid["type"] = df_valid["type"].fillna("Hôpital")
            
        if "code_postal" not in df_valid.columns:
            df_valid["code_postal"] = ""
        else:
            df_valid["code_postal"] = df_valid["code_postal"].fillna("")
            
        # Générer un source_id si nécessaire
        if "source_id" not in df_valid.columns:
            df_valid["source_id"] = df_valid.index.map(lambda i: f"SRC-{i+1:06d}")
        else:
            df_valid["source_id"] = df_valid["source_id"].fillna(df_valid.index.map(lambda i: f"SRC-{i+1:06d}"))
        
        return df_valid
    
    def process_urgences_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Traite les données d'urgences (MSSS) avec mappage des colonnes + correction d'espaces.
        
        Args:
            df: DataFrame contenant les données d'urgences
            
        Returns:
            DataFrame: DataFrame traité
        """
        from .encoding_utils import clean_dataframe, standardize_establishment_names
        logger.info(f"Colonnes originales du CSV: {df.columns.tolist()}")

        # -- NOUVEAUTÉ : enlever espaces autour des noms de colonnes --
        df.columns = [col.strip() for col in df.columns]
        logger.info(f"Colonnes après strip: {df.columns.tolist()}")

        # Mappage des colonnes
        column_mapping = {
            'RSS': 'rss',
            'Region': 'region',
            'Nom_etablissement': 'nom_etablissement',
            'Nom_installation': 'nom_installation',
            'No_permis_installation': 'no_permis_installation',
            'Nombre_de_civieres_fonctionnelles': 'civieres_fonctionnelles',
            'Nombre_de_civieres_occupees': 'civieres_occupees',
            'Nombre_de_patients_sur_civiere_plus_de_24_heures': 'patients_24h',
            'Nombre_de_patients_sur_civiere_plus_de_48_heures': 'patients_48h',
            'Nombre_total_de_patients_presents_a_lurgence': 'total_patients',
            'Nombre_total_de_patients_en_attente_de_PEC': 'patients_en_attente',
            'DMS_sur_civiere': 'dms_civiere',
            'DMS_ambulatoire': 'dms_ambulatoire',
            'DMS_sur_civiere_horaire': 'dms_civiere_horaire',
            'DMS_ambulatoire_horaire': 'dms_ambulatoire_horaire',
            "Heure_de_l'extraction_(image)": 'heure_extraction',
            'Mise_a_jour': 'date_maj'
        }
        
        # Renommer
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})

        logger.info(f"Colonnes après renommage: {df.columns.tolist()}")
        
        # Nettoyer l'encodage dans les colonnes texte
        text_cols = [col for col in df.columns if df[col].dtype == 'object']
        df_clean = clean_dataframe(df, text_cols)
        
        # Convertir en numérique
        numeric_columns = [
            'civieres_fonctionnelles',
            'civieres_occupees',
            'patients_24h',
            'patients_48h',
            'total_patients',
            'patients_en_attente',
            'dms_civiere',
            'dms_ambulatoire'
        ]
        for col in numeric_columns:
            if col in df_clean.columns:
                # Remplacement des valeurs non numériques
                if col in ['dms_civiere', 'dms_ambulatoire']:
                    df_clean[col] = df_clean[col].apply(
                        lambda x: float(str(x).replace(',', '.')) if isinstance(x, str) else x
                    )
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                # Remplir les valeurs NaN avec 0
                df_clean[col] = df_clean[col].fillna(0)
        
        # Valider plages (exemple : 0 à 1000)
        if "civieres_fonctionnelles" in df_clean.columns:
            df_clean = self.validator.validate_numeric_range(df_clean, "civieres_fonctionnelles", 0, 1000)
        if "civieres_occupees" in df_clean.columns:
            df_clean = self.validator.validate_numeric_range(df_clean, "civieres_occupees", 0, 1000)
        
        # Calcul taux d'occupation
        if "civieres_fonctionnelles" in df_clean.columns and "civieres_occupees" in df_clean.columns:
            df_clean["taux_occupation"] = df_clean.apply(
                lambda row: (
                    row["civieres_occupees"] / row["civieres_fonctionnelles"] * 100
                ) if pd.notna(row["civieres_occupees"])
                     and pd.notna(row["civieres_fonctionnelles"])
                     and row["civieres_fonctionnelles"] > 0
                else 0,  # Valeur par défaut à 0 au lieu de None
                axis=1
            )
        
        # Standardiser les noms (pour la recherche en base)
        if "nom_etablissement" in df_clean.columns:
            df_clean = standardize_establishment_names(df_clean, "nom_etablissement")
        if "nom_installation" in df_clean.columns:
            df_clean = standardize_establishment_names(df_clean, "nom_installation")
        
        # Génération d'un source_id pour les données d'urgence
        if "source_id" not in df_clean.columns:
            # Créer un identifiant unique basé sur l'installation et la date
            df_clean["source_id"] = df_clean.apply(
                lambda row: f"URG-{row.get('no_permis_installation', '')}-{datetime.now().strftime('%Y%m%d')}",
                axis=1
            )
        
        # Conversion du numéro de permis en string
        if "no_permis_installation" in df_clean.columns:
            df_clean["no_permis_installation"] = df_clean["no_permis_installation"].astype(str)
        else:
            df_clean["no_permis_installation"] = ""
        
        return df_clean
    
    def merge_etablissements_urgences(
        self, etablissements_df: pd.DataFrame, urgences_df: pd.DataFrame,
        etablissement_key: str, urgence_key: str
    ) -> pd.DataFrame:
        """
        Fusionne les données d'établissements et d'urgences.
        """
        if etablissement_key + "_normalise" in etablissements_df.columns:
            etablissements_key_col = etablissement_key + "_normalise"
        else:
            etablissements_key_col = etablissement_key
        
        if urgence_key + "_normalise" in urgences_df.columns:
            urgences_key_col = urgence_key + "_normalise"
        else:
            urgences_key_col = urgence_key
        
        merged_df = pd.merge(
            etablissements_df,
            urgences_df,
            left_on=etablissements_key_col,
            right_on=urgences_key_col,
            how="left",
            suffixes=("_etab", "_urg")
        )
        
        missing_urgences = merged_df[merged_df[urgences_key_col].isna()]
        if not missing_urgences.empty:
            self.validator._log_anomaly(
                "fusion", 
                f"{len(missing_urgences)} établissements sans données d'urgence correspondantes",
                missing_urgences[[etablissement_key, "adresse", "ville", "province"]]
            )
        
        return merged_df