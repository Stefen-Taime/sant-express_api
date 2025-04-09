"""
Module pour la détection et correction des problèmes d'encodage dans les fichiers CSV.
"""
import chardet
import ftfy
import pandas as pd
import re
import unicodedata
import logging
from typing import Dict, List, Tuple
import io

logger = logging.getLogger(__name__)

def detect_file_encoding(file_path: str) -> Dict:
    """
    Détecte l'encodage d'un fichier en utilisant chardet.
    """
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
        logger.info(f"Encodage détecté pour {file_path}: {result}")
        return result

def read_csv_with_encoding_detection(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Lit un fichier CSV en détectant automatiquement son encodage.
    Utilise plusieurs encodages possibles si la confiance est faible.
    """
    try:
        encoding_result = detect_file_encoding(file_path)
        encoding = encoding_result['encoding']
        confidence = encoding_result['confidence']
        
        if confidence < 0.7:
            logger.warning(f"Confiance faible pour l'encodage détecté ({confidence}). Essai avec encodages courants.")
            encodings_to_try = ['utf-8', 'latin-1', 'ISO-8859-1', 'cp1252']
        else:
            encodings_to_try = [encoding]
            
        for enc in encodings_to_try:
            try:
                df = pd.read_csv(file_path, encoding=enc, **kwargs)
                logger.info(f"Fichier {file_path} lu avec succès en utilisant l'encodage {enc}")
                return df
            except UnicodeDecodeError:
                logger.warning(f"Échec de lecture avec l'encodage {enc}")
                continue
        
        # Si aucun encodage ne marche, on tente errors='replace'
        logger.warning("Tentative de lecture avec remplacement des caractères non décodables")
        df = pd.read_csv(file_path, encoding='latin-1', errors='replace', **kwargs)
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors de la lecture du fichier {file_path}: {str(e)}")
        raise

def fix_text_encoding(text: str) -> str:
    """
    Corrige les problèmes d'encodage dans un texte en utilisant ftfy.
    """
    if not isinstance(text, str) or pd.isna(text):
        return text
    return ftfy.fix_text(text)

def normalize_text(text: str, remove_accents: bool = False) -> str:
    """
    Normalise un texte en minuscules et (optionnel) sans accents.
    """
    if not isinstance(text, str) or pd.isna(text):
        return ""
    
    text = fix_text_encoding(text)
    text = text.lower()
    
    if remove_accents:
        text = ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )
    
    # Supprime ponctuation / caractères spéciaux
    text = re.sub(r'[^\w\s]', ' ', text)
    # Réduit les espaces multiples
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def clean_dataframe(df: pd.DataFrame, text_columns: List[str]) -> pd.DataFrame:
    """
    Nettoie un DataFrame en corrigeant les problèmes d'encodage
    et en créant des colonnes *normalisées* pour chaque colonne texte.
    """
    df_clean = df.copy()
    for col in text_columns:
        if col in df_clean.columns:
            # Remplace les valeurs NA par des chaînes vides pour éviter les erreurs
            df_clean[col] = df_clean[col].fillna("")
            df_clean[col] = df_clean[col].apply(fix_text_encoding)
            norm_col = f"{col}_normalise"
            df_clean[norm_col] = df_clean[col].apply(lambda x: normalize_text(x, remove_accents=True))
    return df_clean

def validate_csv_structure(df: pd.DataFrame, expected_columns: List[str]) -> Tuple[bool, List[str]]:
    """
    Vérifie que le DataFrame contient bien les colonnes attendues.
    """
    missing_columns = [col for col in expected_columns if col not in df.columns]
    is_valid = (len(missing_columns) == 0)
    if not is_valid:
        logger.warning(f"Colonnes manquantes dans le DataFrame: {missing_columns}")
    return is_valid, missing_columns

def handle_truncated_lines(file_path: str, expected_columns: int, **kwargs) -> pd.DataFrame:
    """
    Gère les lignes tronquées dans un CSV.
    Lit le fichier ligne par ligne et conserve celles qui ont le bon nombre de colonnes.
    """
    encoding_result = detect_file_encoding(file_path)
    encoding = encoding_result['encoding']
    
    valid_lines = []
    invalid_lines = []
    
    with open(file_path, 'r', encoding=encoding, errors='replace') as f:
        for i, line in enumerate(f):
            fields = line.strip().split(',')
            if len(fields) == expected_columns:
                valid_lines.append(line)
            else:
                invalid_lines.append((i+1, line))
                logger.warning(f"Ligne {i+1} tronquée ou mal formatée: {line[:50]}...")
    
    if invalid_lines:
        logger.warning(f"{len(invalid_lines)} lignes invalides sur un total de {i+1}")
        with open(f"{file_path}.invalid_lines.log", 'w', encoding='utf-8') as log_file:
            for line_num, content in invalid_lines:
                log_file.write(f"Ligne {line_num}: {content}\n")
    
    valid_data = io.StringIO(''.join(valid_lines))
    df = pd.read_csv(valid_data, **kwargs)
    return df

def standardize_establishment_names(df: pd.DataFrame, name_column: str) -> pd.DataFrame:
    """
    Standardise les noms d'établissements en supprimant les variations
    de vocabulaire (hôpital -> hopital, centre hospitalier -> ch, etc.).
    """
    if name_column not in df.columns:
        logger.warning(f"Colonne {name_column} non trouvée dans le DataFrame")
        return df
    
    df_std = df.copy()
    # Remplacer les valeurs nulles par des chaînes vides
    df_std[name_column] = df_std[name_column].fillna("")
    df_std[name_column] = df_std[name_column].apply(fix_text_encoding)
    
    # Liste de remplacements courants
    replacements = {
        r'h[ôo]pital': 'hopital',
        r'centre hospitalier': 'ch',
        r'centre de sant[ée]': 'cs',
        r'centre int[ée]gr[ée] de sant[ée] et de services sociaux': 'cisss',
        r'centre int[ée]gr[ée] universitaire de sant[ée] et de services sociaux': 'ciusss',
        r'r[ée]gional': 'regional',
        r'g[ée]n[ée]ral': 'general',
        r'universit[ée]': 'universite',
        r'p[ée]diatrique': 'pediatrique',
        r'sant[ée]': 'sante',
        r'r[ée]adaptation': 'readaptation',
        r'd[ée]pendance': 'dependance'
    }
    
    std_name_col = f"{name_column}_std"
    df_std[std_name_col] = df_std[name_column].str.lower()
    
    for pattern, replacement in replacements.items():
        df_std[std_name_col] = df_std[std_name_col].str.replace(pattern, replacement, regex=True)
    
    df_std[std_name_col] = df_std[std_name_col].apply(lambda x: re.sub(r'[^\w\s]', ' ', str(x)))
    df_std[std_name_col] = df_std[std_name_col].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip())
    
    return df_std