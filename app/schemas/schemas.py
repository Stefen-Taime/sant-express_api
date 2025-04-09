"""
Schémas Pydantic pour la validation des données d'entrée et de sortie de l'API.
"""
from pydantic import BaseModel, Field, field_validator, model_validator, constr
from typing import Optional, List, Dict, Any, ClassVar
from datetime import datetime
from enum import Enum


class RegionBase(BaseModel):
    """Schéma de base pour les régions."""
    rss: str = Field(..., description="Code de la région sociosanitaire")
    nom: str = Field(..., description="Nom de la région")


class RegionCreate(RegionBase):
    """Schéma pour la création d'une région."""
    pass


class RegionResponse(RegionBase):
    """Schéma pour la réponse d'une région."""
    id: int
    nom_normalise: str = Field(..., description="Nom normalisé sans accents pour la recherche")

    model_config = {"from_attributes": True}


class EtablissementBase(BaseModel):
    """Schéma de base pour les établissements."""
    source_id: Optional[str] = Field(None, description="Identifiant dans le fichier source")
    no_permis_installation: Optional[str] = Field(None, description="Numéro de permis de l'installation")
    nom_etablissement: str = Field(..., description="Nom de l'établissement")
    nom_installation: Optional[str] = Field(None, description="Nom de l'installation")
    type: Optional[str] = Field(None, description="Type d'établissement")
    adresse: Optional[str] = Field(None, description="Adresse de l'établissement")
    code_postal: Optional[str] = Field(None, description="Code postal")
    ville: Optional[str] = Field(None, description="Ville")
    province: Optional[str] = Field(None, description="Province")
    region_id: Optional[int] = Field(None, description="ID de la région")


class EtablissementCreate(EtablissementBase):
    """Schéma pour la création d'un établissement."""
    latitude: Optional[float] = Field(None, description="Latitude de l'établissement")
    longitude: Optional[float] = Field(None, description="Longitude de l'établissement")


class EtablissementResponse(EtablissementBase):
    """Schéma pour la réponse d'un établissement."""
    id: int
    nom_etablissement_normalise: str = Field(..., description="Nom normalisé sans accents pour la recherche")
    nom_installation_normalise: Optional[str] = Field(None, description="Nom normalisé sans accents pour la recherche")
    date_maj: datetime = Field(..., description="Date de dernière mise à jour")
    latitude: Optional[float] = Field(None, description="Latitude de l'établissement")
    longitude: Optional[float] = Field(None, description="Longitude de l'établissement")
    
    model_config = {"from_attributes": True}


class UrgenceBase(BaseModel):
    """Schéma de base pour les urgences."""
    etablissement_id: int = Field(..., description="ID de l'établissement")
    civieres_fonctionnelles: Optional[int] = Field(None, description="Nombre de civières fonctionnelles")
    civieres_occupees: Optional[int] = Field(None, description="Nombre de civières occupées")
    patients_24h: Optional[int] = Field(None, description="Nombre de patients sur civière depuis plus de 24h")
    patients_48h: Optional[int] = Field(None, description="Nombre de patients sur civière depuis plus de 48h")
    total_patients: Optional[int] = Field(None, description="Nombre total de patients")
    patients_en_attente: Optional[int] = Field(None, description="Nombre de patients en attente")
    dms_civiere: Optional[float] = Field(None, description="Durée moyenne de séjour sur civière (heures)")
    dms_ambulatoire: Optional[float] = Field(None, description="Durée moyenne de séjour ambulatoire (heures)")
    date_extraction: datetime = Field(..., description="Date d'extraction des données")
    statut_validation: Optional[str] = Field("non_validé", description="Statut de validation des données")

    @field_validator('civieres_occupees')
    def civieres_occupees_must_be_positive(cls, v):
        if v is not None and v < 0:
            raise ValueError('Le nombre de civières occupées doit être positif ou nul')
        return v

    @model_validator(mode='after')
    def check_taux_occupation(self) -> 'UrgenceBase':
        civieres_fonctionnelles = self.civieres_fonctionnelles
        civieres_occupees = self.civieres_occupees
        
        if civieres_fonctionnelles and civieres_occupees:
            if civieres_fonctionnelles > 0:
                self.taux_occupation = (civieres_occupees / civieres_fonctionnelles) * 100
            else:
                self.taux_occupation = None
        else:
            self.taux_occupation = None
            
        return self


class UrgenceCreate(UrgenceBase):
    """Schéma pour la création d'une urgence."""
    pass


class UrgenceResponse(UrgenceBase):
    """Schéma pour la réponse d'une urgence."""
    id: int
    taux_occupation: Optional[float] = Field(None, description="Taux d'occupation (%)")
    date_maj: datetime = Field(..., description="Date de dernière mise à jour")
    
    model_config = {"from_attributes": True}


class UrgenceWithEtablissement(UrgenceResponse):
    """Schéma pour la réponse d'une urgence avec les détails de l'établissement."""
    etablissement: EtablissementResponse
    
    model_config = {"from_attributes": True}


class ProximiteRequest(BaseModel):
    """Schéma pour la requête de proximité."""
    latitude: float = Field(..., description="Latitude de la position")
    longitude: float = Field(..., description="Longitude de la position")
    rayon: Optional[float] = Field(10.0, description="Rayon de recherche en kilomètres")
    limit: Optional[int] = Field(10, description="Nombre maximum de résultats")


class EtablissementProximite(EtablissementResponse):
    """Schéma pour la réponse d'un établissement avec la distance."""
    distance: float = Field(..., description="Distance en kilomètres")
    
    model_config = {"from_attributes": True}


class RecommandationResponse(BaseModel):
    """Schéma pour la réponse de recommandation d'urgence."""
    etablissement: EtablissementResponse
    urgence: UrgenceResponse
    distance: float = Field(..., description="Distance en kilomètres")
    score: float = Field(..., description="Score de recommandation (plus élevé = meilleur)")
    
    model_config = {"from_attributes": True}


class StatistiqueRegion(BaseModel):
    """Schéma pour les statistiques par région."""
    region: RegionResponse
    nb_etablissements: int = Field(..., description="Nombre d'établissements dans la région")
    taux_occupation_moyen: Optional[float] = Field(None, description="Taux d'occupation moyen des urgences")
    nb_patients_attente: Optional[int] = Field(None, description="Nombre total de patients en attente")
    
    model_config = {"from_attributes": True}


class PaginationParams(BaseModel):
    """Paramètres de pagination."""
    page: int = Field(1, ge=1, description="Numéro de page")
    page_size: int = Field(20, ge=1, le=100, description="Nombre d'éléments par page")


class PaginatedResponse(BaseModel):
    """Réponse paginée générique."""
    items: List[Any]
    total: int
    page: int
    page_size: int
    pages: int


class GeoJSONGeometry(BaseModel):
    """Schéma pour la géométrie GeoJSON."""
    type: str
    coordinates: Any


class GeoJSONFeature(BaseModel):
    """Schéma pour un feature GeoJSON."""
    type: str = "Feature"
    geometry: GeoJSONGeometry
    properties: Dict[str, Any]


class GeoJSONFeatureCollection(BaseModel):
    """Schéma pour une collection de features GeoJSON."""
    type: str = "FeatureCollection"
    features: List[GeoJSONFeature]


class MRCBase(BaseModel):
    """Schéma de base pour les MRC."""
    code: Optional[str] = Field(None, description="Code de la MRC")
    nom: Optional[str] = Field(None, description="Nom de la MRC")
    region_code: Optional[str] = Field(None, description="Code de la région administrative")
    region_nom: Optional[str] = Field(None, description="Nom de la région administrative")


class MRCResponse(MRCBase):
    """Schéma pour la réponse des MRC avec leurs limites géographiques."""
    id: int
    geometrie: Dict[str, Any] = Field(..., description="Géométrie GeoJSON de la MRC")
    
    model_config = {"from_attributes": True}