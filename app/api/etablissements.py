"""
Routes API pour les établissements de santé.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, List
from geoalchemy2 import functions as geo_func

from app.db.database import get_db
from app.models import models
from app.schemas import schemas
from app.utils.encoding_utils import normalize_text

router = APIRouter(
    prefix="/api/etablissements",
    tags=["établissements"],
    responses={404: {"description": "Not found"}},
)

@router.get("/", response_model=schemas.PaginatedResponse)
def get_etablissements(
    pagination: schemas.PaginationParams = Depends(),
    province: Optional[str] = None,
    region_id: Optional[int] = None,
    type: Optional[str] = None,
    nom: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Récupère la liste des établissements avec pagination et filtrage.
    """
    # Effectuer la requête à la base de données avec les filtres
    query = db.query(models.Etablissement)
    
    # Appliquer les filtres si fournis
    if province:
        query = query.filter(models.Etablissement.province == province)
    if region_id:
        query = query.filter(models.Etablissement.region_id == region_id)
    if type:
        query = query.filter(models.Etablissement.type == type)
    if nom:
        # Normaliser le terme de recherche pour une recherche insensible aux accents
        nom_normalise = normalize_text(nom)
        query = query.filter(
            models.Etablissement.nom_etablissement_normalise.ilike(f"%{nom_normalise}%") |
            models.Etablissement.nom_installation_normalise.ilike(f"%{nom_normalise}%")
        )
    
    # Compter le total
    total = query.count()
    
    # Appliquer la pagination
    query = query.offset((pagination.page - 1) * pagination.page_size).limit(pagination.page_size)
    
    # Récupérer les résultats
    etablissements_db = query.all()
    
    # Convertir les objets SQLAlchemy en modèles Pydantic avec traitement des coordonnées géographiques
    etablissements = []
    for etab in etablissements_db:
        etab_dict = {
            "id": etab.id,
            "source_id": etab.source_id,
            "no_permis_installation": etab.no_permis_installation,
            "nom_etablissement": etab.nom_etablissement,
            "nom_etablissement_normalise": etab.nom_etablissement_normalise,
            "nom_installation": etab.nom_installation,
            "nom_installation_normalise": etab.nom_installation_normalise,
            "type": etab.type,
            "adresse": etab.adresse,
            "code_postal": etab.code_postal,
            "ville": etab.ville,
            "province": etab.province,
            "region_id": etab.region_id,
            "date_maj": etab.date_maj,
            "latitude": None,
            "longitude": None
        }
        
        # Extraire les coordonnées du point géographique si disponible
        if etab.point_geo is not None:
            # Convertir le point géo en WKT (Well-Known Text)
            point_wkt = db.scalar(geo_func.ST_AsText(etab.point_geo))
            if point_wkt and point_wkt.startswith("POINT("):
                # Extraire les coordonnées du format POINT(long lat)
                coords = point_wkt.replace("POINT(", "").replace(")", "").split()
                if len(coords) == 2:
                    etab_dict["longitude"] = float(coords[0])
                    etab_dict["latitude"] = float(coords[1])
        
        etablissements.append(schemas.EtablissementResponse(**etab_dict))
    
    # Calculer le nombre total de pages
    pages = (total + pagination.page_size - 1) // pagination.page_size
    
    # Préparer la réponse paginée
    response = {
        "items": etablissements,
        "total": total,
        "page": pagination.page,
        "page_size": pagination.page_size,
        "pages": pages
    }
    
    return response

@router.get("/{etablissement_id}", response_model=schemas.EtablissementResponse)
def get_etablissement(etablissement_id: int, db: Session = Depends(get_db)):
    """
    Récupère un établissement par son ID.
    """
    etablissement = db.query(models.Etablissement).filter(models.Etablissement.id == etablissement_id).first()
    if etablissement is None:
        raise HTTPException(status_code=404, detail="Établissement non trouvé")
    
    # Créer le dictionnaire de base avec les attributs de l'établissement
    etab_dict = {
        "id": etablissement.id,
        "source_id": etablissement.source_id,
        "no_permis_installation": etablissement.no_permis_installation,
        "nom_etablissement": etablissement.nom_etablissement,
        "nom_etablissement_normalise": etablissement.nom_etablissement_normalise,
        "nom_installation": etablissement.nom_installation,
        "nom_installation_normalise": etablissement.nom_installation_normalise,
        "type": etablissement.type,
        "adresse": etablissement.adresse,
        "code_postal": etablissement.code_postal,
        "ville": etablissement.ville,
        "province": etablissement.province,
        "region_id": etablissement.region_id,
        "date_maj": etablissement.date_maj,
        "latitude": None,
        "longitude": None
    }
    
    # Extraire les coordonnées du point géographique si disponible
    if etablissement.point_geo is not None:
        # Convertir le point géo en WKT (Well-Known Text)
        point_wkt = db.scalar(geo_func.ST_AsText(etablissement.point_geo))
        if point_wkt and point_wkt.startswith("POINT("):
            # Extraire les coordonnées du format POINT(long lat)
            coords = point_wkt.replace("POINT(", "").replace(")", "").split()
            if len(coords) == 2:
                etab_dict["longitude"] = float(coords[0])
                etab_dict["latitude"] = float(coords[1])
    
    return schemas.EtablissementResponse(**etab_dict)

@router.get("/recherche/proximite", response_model=List[schemas.EtablissementProximite])
def recherche_proximite(
    params: schemas.ProximiteRequest = Depends(),
    db: Session = Depends(get_db)
):
    """
    Recherche les établissements à proximité d'un point géographique.
    """
    # Créer un point géo à partir des coordonnées
    point = f"POINT({params.longitude} {params.latitude})"
    
    # Requête pour trouver les établissements à proximité
    query = db.query(
        models.Etablissement,
        geo_func.ST_Distance(
            geo_func.ST_Transform(models.Etablissement.point_geo, 3857),
            geo_func.ST_Transform(func.ST_GeomFromText(point, 4326), 3857)
        ).label('distance_meters')
    ).filter(
        models.Etablissement.point_geo.isnot(None)
    ).order_by(
        'distance_meters'
    ).limit(params.limit)
    
    # Convertir les mètres en kilomètres et assembler le résultat
    resultats = []
    for etab, distance_meters in query:
        # Convertir le point géo en WKT (Well-Known Text)
        point_wkt = db.scalar(geo_func.ST_AsText(etab.point_geo))
        lat, lon = None, None
        
        if point_wkt and point_wkt.startswith("POINT("):
            # Extraire les coordonnées du format POINT(long lat)
            coords = point_wkt.replace("POINT(", "").replace(")", "").split()
            if len(coords) == 2:
                lon = float(coords[0])
                lat = float(coords[1])
        
        # Créer le dictionnaire avec toutes les propriétés nécessaires
        etab_dict = {
            "id": etab.id,
            "source_id": etab.source_id,
            "no_permis_installation": etab.no_permis_installation,
            "nom_etablissement": etab.nom_etablissement,
            "nom_etablissement_normalise": etab.nom_etablissement_normalise,
            "nom_installation": etab.nom_installation,
            "nom_installation_normalise": etab.nom_installation_normalise,
            "type": etab.type,
            "adresse": etab.adresse,
            "code_postal": etab.code_postal,
            "ville": etab.ville,
            "province": etab.province,
            "region_id": etab.region_id,
            "date_maj": etab.date_maj,
            "latitude": lat,
            "longitude": lon,
            "distance": distance_meters / 1000  # Convertir en kilomètres
        }
        
        # Créer l'objet Pydantic
        etab_proximite = schemas.EtablissementProximite(**etab_dict)
        resultats.append(etab_proximite)
    
    return resultats

@router.post("/", response_model=schemas.EtablissementResponse, status_code=201)
def create_etablissement(etablissement: schemas.EtablissementCreate, db: Session = Depends(get_db)):
    """
    Crée un nouvel établissement.
    """
    # Normaliser les noms pour la recherche
    nom_etablissement_normalise = normalize_text(etablissement.nom_etablissement)
    nom_installation_normalise = normalize_text(etablissement.nom_installation) if etablissement.nom_installation else None
    
    # Créer un nouvel objet Etablissement
    db_etablissement = models.Etablissement(
        source_id=etablissement.source_id,
        no_permis_installation=etablissement.no_permis_installation,
        nom_etablissement=etablissement.nom_etablissement,
        nom_etablissement_normalise=nom_etablissement_normalise,
        nom_installation=etablissement.nom_installation,
        nom_installation_normalise=nom_installation_normalise,
        type=etablissement.type,
        adresse=etablissement.adresse,
        code_postal=etablissement.code_postal,
        ville=etablissement.ville,
        province=etablissement.province,
        region_id=etablissement.region_id
    )
    
    # Ajouter le point géo si latitude et longitude sont fournies
    if etablissement.latitude is not None and etablissement.longitude is not None:
        point_wkt = f"POINT({etablissement.longitude} {etablissement.latitude})"
        db_etablissement.point_geo = func.ST_GeomFromText(point_wkt, 4326)
    
    # Ajouter à la base de données et commit
    db.add(db_etablissement)
    db.commit()
    db.refresh(db_etablissement)
    
    # Préparer la réponse
    result = {
        "id": db_etablissement.id,
        "source_id": db_etablissement.source_id,
        "no_permis_installation": db_etablissement.no_permis_installation,
        "nom_etablissement": db_etablissement.nom_etablissement,
        "nom_etablissement_normalise": db_etablissement.nom_etablissement_normalise,
        "nom_installation": db_etablissement.nom_installation,
        "nom_installation_normalise": db_etablissement.nom_installation_normalise,
        "type": db_etablissement.type,
        "adresse": db_etablissement.adresse,
        "code_postal": db_etablissement.code_postal,
        "ville": db_etablissement.ville,
        "province": db_etablissement.province,
        "region_id": db_etablissement.region_id,
        "date_maj": db_etablissement.date_maj,
        "latitude": etablissement.latitude,
        "longitude": etablissement.longitude
    }
    
    return schemas.EtablissementResponse(**result)

@router.put("/{etablissement_id}", response_model=schemas.EtablissementResponse)
def update_etablissement(
    etablissement_id: int, 
    etablissement: schemas.EtablissementCreate, 
    db: Session = Depends(get_db)
):
    """
    Met à jour un établissement existant.
    """
    # Vérifier si l'établissement existe
    db_etablissement = db.query(models.Etablissement).filter(models.Etablissement.id == etablissement_id).first()
    if db_etablissement is None:
        raise HTTPException(status_code=404, detail="Établissement non trouvé")
    
    # Normaliser les noms pour la recherche
    nom_etablissement_normalise = normalize_text(etablissement.nom_etablissement)
    nom_installation_normalise = normalize_text(etablissement.nom_installation) if etablissement.nom_installation else None
    
    # Mettre à jour les attributs
    db_etablissement.source_id = etablissement.source_id
    db_etablissement.no_permis_installation = etablissement.no_permis_installation
    db_etablissement.nom_etablissement = etablissement.nom_etablissement
    db_etablissement.nom_etablissement_normalise = nom_etablissement_normalise
    db_etablissement.nom_installation = etablissement.nom_installation
    db_etablissement.nom_installation_normalise = nom_installation_normalise
    db_etablissement.type = etablissement.type
    db_etablissement.adresse = etablissement.adresse
    db_etablissement.code_postal = etablissement.code_postal
    db_etablissement.ville = etablissement.ville
    db_etablissement.province = etablissement.province
    db_etablissement.region_id = etablissement.region_id
    
    # Mettre à jour le point géo si latitude et longitude sont fournies
    if etablissement.latitude is not None and etablissement.longitude is not None:
        point_wkt = f"POINT({etablissement.longitude} {etablissement.latitude})"
        db_etablissement.point_geo = func.ST_GeomFromText(point_wkt, 4326)
    
    # Commit les changements
    db.commit()
    db.refresh(db_etablissement)
    
    # Préparer la réponse
    point_wkt = db.scalar(geo_func.ST_AsText(db_etablissement.point_geo)) if db_etablissement.point_geo else None
    lat, lon = None, None
    
    if point_wkt and point_wkt.startswith("POINT("):
        coords = point_wkt.replace("POINT(", "").replace(")", "").split()
        if len(coords) == 2:
            lon = float(coords[0])
            lat = float(coords[1])
    
    result = {
        "id": db_etablissement.id,
        "source_id": db_etablissement.source_id,
        "no_permis_installation": db_etablissement.no_permis_installation,
        "nom_etablissement": db_etablissement.nom_etablissement,
        "nom_etablissement_normalise": db_etablissement.nom_etablissement_normalise,
        "nom_installation": db_etablissement.nom_installation,
        "nom_installation_normalise": db_etablissement.nom_installation_normalise,
        "type": db_etablissement.type,
        "adresse": db_etablissement.adresse,
        "code_postal": db_etablissement.code_postal,
        "ville": db_etablissement.ville,
        "province": db_etablissement.province,
        "region_id": db_etablissement.region_id,
        "date_maj": db_etablissement.date_maj,
        "latitude": lat,
        "longitude": lon
    }
    
    return schemas.EtablissementResponse(**result)

@router.delete("/{etablissement_id}", status_code=204)
def delete_etablissement(etablissement_id: int, db: Session = Depends(get_db)):
    """
    Supprime un établissement.
    """
    # Vérifier si l'établissement existe
    db_etablissement = db.query(models.Etablissement).filter(models.Etablissement.id == etablissement_id).first()
    if db_etablissement is None:
        raise HTTPException(status_code=404, detail="Établissement non trouvé")
    
    # Supprimer l'établissement
    db.delete(db_etablissement)
    db.commit()
    
    return None