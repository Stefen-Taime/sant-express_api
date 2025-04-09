"""
Endpoints API pour les données géographiques.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Dict, Any, Optional
import json

from app.db.database import get_db
from app.models.models import TempMRC, Etablissement, Region
from app.schemas.schemas import MRCResponse, GeoJSONFeatureCollection, GeoJSONFeature

router = APIRouter(
    prefix="/api/geo",
    tags=["géographie"],
    responses={404: {"description": "Données géographiques non trouvées"}},
)

@router.get("/mrc", response_model=List[MRCResponse])
def get_mrc_boundaries(
    db: Session = Depends(get_db),
    region_code: Optional[str] = Query(None, description="Filtrer par code de région administrative")
):
    """
    Récupère les limites géographiques des MRC.
    
    Permet d'obtenir les polygones représentant les limites des MRC (Municipalités Régionales de Comté)
    pour l'affichage sur une carte.
    """
    # Construire la requête de base
    query = db.query(TempMRC)
    
    # Appliquer le filtre par région si fourni
    if region_code:
        query = query.filter(TempMRC.mrs_co_reg == region_code)
    
    # Exécuter la requête
    mrc_list = query.all()
    
    if not mrc_list:
        raise HTTPException(status_code=404, detail="Aucune donnée MRC trouvée")
    
    # Construire la liste des résultats
    result = []
    for mrc in mrc_list:
        # Convertir la géométrie en GeoJSON
        geojson = db.scalar(func.ST_AsGeoJSON(mrc.geom))
        
        if geojson:
            mrc_dict = {
                "id": mrc.gid,
                "code": mrc.mrs_co_mrc,
                "nom": mrc.mrs_nm_mrc,
                "region_code": mrc.mrs_co_reg,
                "region_nom": mrc.mrs_nm_reg,
                "geometrie": json.loads(geojson)
            }
            result.append(MRCResponse(**mrc_dict))
    
    return result

@router.get("/mrc/geojson", response_model=GeoJSONFeatureCollection)
def get_mrc_geojson(
    db: Session = Depends(get_db),
    region_code: Optional[str] = Query(None, description="Filtrer par code de région administrative")
):
    """
    Récupère les limites géographiques des MRC au format GeoJSON.
    
    Retourne une collection de features GeoJSON pour une utilisation directe avec des bibliothèques
    de cartographie comme Leaflet ou Mapbox.
    """
    # Construire la requête de base
    query = db.query(TempMRC)
    
    # Appliquer le filtre par région si fourni
    if region_code:
        query = query.filter(TempMRC.mrs_co_reg == region_code)
    
    # Exécuter la requête
    mrc_list = query.all()
    
    if not mrc_list:
        raise HTTPException(status_code=404, detail="Aucune donnée MRC trouvée")
    
    # Construire la collection GeoJSON
    features = []
    for mrc in mrc_list:
        # Convertir la géométrie en GeoJSON
        geojson = db.scalar(func.ST_AsGeoJSON(mrc.geom))
        
        if geojson:
            geometry = json.loads(geojson)
            
            # Créer les propriétés du feature
            properties = {
                "id": mrc.gid,
                "code": mrc.mrs_co_mrc,
                "nom": mrc.mrs_nm_mrc,
                "region_code": mrc.mrs_co_reg,
                "region_nom": mrc.mrs_nm_reg
            }
            
            # Créer le feature
            feature = GeoJSONFeature(
                type="Feature",
                geometry=geometry,
                properties=properties
            )
            
            features.append(feature)
    
    # Créer la collection de features
    feature_collection = GeoJSONFeatureCollection(
        type="FeatureCollection",
        features=features
    )
    
    return feature_collection

@router.get("/regions/geojson", response_model=GeoJSONFeatureCollection)
def get_regions_geojson(db: Session = Depends(get_db)):
    """
    Récupère les limites géographiques des régions sociosanitaires au format GeoJSON.
    
    Retourne une collection de features GeoJSON pour une utilisation directe avec des bibliothèques
    de cartographie comme Leaflet ou Mapbox.
    """
    # Récupérer toutes les régions avec leurs géométries
    regions = db.query(Region).filter(Region.geom.isnot(None)).all()
    
    if not regions:
        raise HTTPException(status_code=404, detail="Aucune donnée géographique de région trouvée")
    
    # Construire la collection GeoJSON
    features = []
    for region in regions:
        # Convertir la géométrie en GeoJSON
        geojson = db.scalar(func.ST_AsGeoJSON(region.geom))
        
        if geojson:
            geometry = json.loads(geojson)
            
            # Compter le nombre d'établissements dans la région
            nb_etablissements = db.query(Etablissement).filter(Etablissement.region_id == region.id).count()
            
            # Créer les propriétés du feature
            properties = {
                "id": region.id,
                "rss": region.rss,
                "nom": region.nom,
                "nb_etablissements": nb_etablissements
            }
            
            # Créer le feature
            feature = GeoJSONFeature(
                type="Feature",
                geometry=geometry,
                properties=properties
            )
            
            features.append(feature)
    
    # Créer la collection de features
    feature_collection = GeoJSONFeatureCollection(
        type="FeatureCollection",
        features=features
    )
    
    return feature_collection

@router.get("/etablissements/geojson", response_model=GeoJSONFeatureCollection)
def get_etablissements_geojson(
    db: Session = Depends(get_db),
    region_id: Optional[int] = Query(None, description="Filtrer par ID de région")
):
    """
    Récupère les coordonnées des établissements au format GeoJSON.
    
    Retourne une collection de points GeoJSON représentant les établissements pour une utilisation
    directe avec des bibliothèques de cartographie comme Leaflet ou Mapbox.
    """
    # Construire la requête de base
    query = db.query(Etablissement).filter(Etablissement.point_geo.isnot(None))
    
    # Appliquer le filtre par région si fourni
    if region_id:
        query = query.filter(Etablissement.region_id == region_id)
    
    # Exécuter la requête
    etablissements = query.all()
    
    if not etablissements:
        raise HTTPException(status_code=404, detail="Aucun établissement avec coordonnées trouvé")
    
    # Construire la collection GeoJSON
    features = []
    for etab in etablissements:
        # Convertir la géométrie en GeoJSON
        geojson = db.scalar(func.ST_AsGeoJSON(etab.point_geo))
        
        if geojson:
            geometry = json.loads(geojson)
            
            # Créer les propriétés du feature
            properties = {
                "id": etab.id,
                "nom_etablissement": etab.nom_etablissement,
                "nom_installation": etab.nom_installation,
                "type": etab.type,
                "adresse": etab.adresse,
                "ville": etab.ville,
                "code_postal": etab.code_postal,
                "province": etab.province
            }
            
            # Créer le feature
            feature = GeoJSONFeature(
                type="Feature",
                geometry=geometry,
                properties=properties
            )
            
            features.append(feature)
    
    # Créer la collection de features
    feature_collection = GeoJSONFeatureCollection(
        type="FeatureCollection",
        features=features
    )
    
    return feature_collection