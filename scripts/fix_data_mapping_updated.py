"""
Module amélioré pour la mise à jour des données avec mappage correct des colonnes du CSV.
Pour remplacer la fonction process_urgences_data dans la classe DataProcessor.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def process_urgences_data(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Traite les données d'urgences avec mappage correct des colonnes.
    
    Args:
        df: DataFrame contenant les données d'urgences
        
    Returns:
        DataFrame: DataFrame traité
    """
    from .encoding_utils import clean_dataframe
    
    # Afficher les colonnes originales pour débogage
    logger.info(f"Colonnes originales du CSV: {df.columns.tolist()}")
    
    # Mappage des colonnes basé sur le CSV réel
    column_mapping = {
        'Nom_etablissement': 'nom_etablissement',
        'Nom_installation': 'nom_installation',
        'No_permis_installation': 'no_permis_installation',
        'Nombre_de_civieres_fonctionnelles': 'civieres_fonctionnelles',
        'Nombre_de_civieres_occupees': 'civieres_occupees',
        'Nombre_de_patients_sur_civiere_plus_de_24_heures': 'patients_24h',
        'Nombre_de_patients_sur_civiere_plus_de_48_heures': 'patients_48h',
        'Heure_de_l\'extraction_(image)': 'heure_extraction',
        'Mise_a_jour': 'date_maj'
    }
    
    # Renommer les colonnes
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # Afficher les colonnes après renommage
    logger.info(f"Colonnes après renommage: {df.columns.tolist()}")
    
    # Nettoyer les problèmes d'encodage
    text_columns = [col for col in df.columns if df[col].dtype == 'object']
    df_clean = clean_dataframe(df, text_columns)
    
    # Convertir les colonnes numériques
    numeric_columns = ['civieres_fonctionnelles', 'civieres_occupees', 'patients_24h', 'patients_48h']
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Valider les plages numériques
    if "civieres_fonctionnelles" in df_clean.columns:
        df_clean = self.validator.validate_numeric_range(df_clean, "civieres_fonctionnelles", 0, 1000)
    
    if "civieres_occupees" in df_clean.columns:
        df_clean = self.validator.validate_numeric_range(df_clean, "civieres_occupees", 0, 1000)
    
    # Calculer le taux d'occupation si possible
    if "civieres_fonctionnelles" in df_clean.columns and "civieres_occupees" in df_clean.columns:
        # Éviter la division par zéro
        df_clean["taux_occupation"] = df_clean.apply(
            lambda row: (row["civieres_occupees"] / row["civieres_fonctionnelles"]) * 100 
            if pd.notna(row["civieres_fonctionnelles"]) and row["civieres_fonctionnelles"] > 0 
               and pd.notna(row["civieres_occupees"]) else None, 
            axis=1
        )
    
    # Standardiser les noms pour la recherche
    from .encoding_utils import standardize_establishment_names
    if "nom_etablissement" in df_clean.columns:
        df_clean = standardize_establishment_names(df_clean, "nom_etablissement")
    if "nom_installation" in df_clean.columns:
        df_clean = standardize_establishment_names(df_clean, "nom_installation")
    
    return df_clean