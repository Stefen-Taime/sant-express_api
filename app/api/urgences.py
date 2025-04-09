"""
Endpoints API pour la gestion des urgences.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, desc, and_
from typing import List, Optional
from geoalchemy2.functions import ST_Distance, ST_SetSRID, ST_MakePoint
from datetime import datetime, timedelta
from app.db.database import get_db
from sqlalchemy import func, desc, and_, text
from app.models.models import Etablissement, Region, UrgencesEtatActuel, UrgencesHistorique
from app.schemas.schemas import (
    UrgenceResponse, UrgenceWithEtablissement, RecommandationResponse,
    ProximiteRequest, PaginatedResponse, StatistiqueRegion, RegionResponse
)
import math

router = APIRouter(
    prefix="/api/urgences",
    tags=["urgences"],
    responses={404: {"description": "Données d'urgence non trouvées"}},
)

# Modifier le chemin de l'endpoint des régions pour éviter tout conflit
@router.get("/statistiques-par-regions", response_model=List[StatistiqueRegion])
def get_statistiques_regions(db: Session = Depends(get_db)):
    """
    Récupère les statistiques par région.
    """
    # Requête pour les statistiques par région
    stats = (
        db.query(
            Region,
            func.count(Etablissement.id).label("nb_etablissements"),
            func.avg(UrgencesEtatActuel.taux_occupation).label("taux_occupation_moyen"),
            func.sum(UrgencesEtatActuel.patients_en_attente).label("nb_patients_attente")
        )
        .join(Etablissement, Region.id == Etablissement.region_id)
        .outerjoin(UrgencesEtatActuel, Etablissement.id == UrgencesEtatActuel.etablissement_id)
        .group_by(Region.id)
        .all()
    )
    
    # Construire la liste des résultats
    resultats = []
    for region, nb_etablissements, taux_occupation_moyen, nb_patients_attente in stats:
        # Créer l'objet région
        region_dict = {
            "id": region.id,
            "rss": region.rss,
            "nom": region.nom,
            "nom_normalise": region.nom_normalise
        }
        
        # Arrondir le taux d'occupation moyen à 2 décimales
        if taux_occupation_moyen is not None:
            taux_occupation_moyen = round(taux_occupation_moyen, 2)
        
        # Créer l'objet de statistiques
        stat_dict = {
            "region": RegionResponse(**region_dict),
            "nb_etablissements": nb_etablissements,
            "taux_occupation_moyen": taux_occupation_moyen,
            "nb_patients_attente": int(nb_patients_attente) if nb_patients_attente is not None else None
        }
        
        resultats.append(StatistiqueRegion(**stat_dict))
    
    return resultats

@router.get("/", response_model=PaginatedResponse)
def get_urgences(
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1, description="Numéro de page"),
    page_size: int = Query(20, ge=1, le=100, description="Nombre d'éléments par page"),
    region_id: Optional[int] = Query(None, description="Filtrer par ID de région"),
    taux_min: Optional[float] = Query(None, ge=0, le=200, description="Taux d'occupation minimum (%)"),
    taux_max: Optional[float] = Query(None, ge=0, le=200, description="Taux d'occupation maximum (%)"),
    sort_by: Optional[str] = Query("taux_occupation", description="Champ de tri (taux_occupation, date_extraction)"),
    sort_desc: bool = Query(True, description="Tri descendant")
):
    """
    Récupère les données actuelles de toutes les urgences avec pagination et filtrage.
    """
    # Construire la requête de base avec jointure sur les établissements
    query = (
        db.query(UrgencesEtatActuel)
        .join(Etablissement, UrgencesEtatActuel.etablissement_id == Etablissement.id)
        .options(joinedload(UrgencesEtatActuel.etablissement))
    )
    
    # Appliquer les filtres
    if region_id:
        query = query.filter(Etablissement.region_id == region_id)
    
    if taux_min is not None:
        query = query.filter(UrgencesEtatActuel.taux_occupation >= taux_min)
    
    if taux_max is not None:
        query = query.filter(UrgencesEtatActuel.taux_occupation <= taux_max)
    
    # Appliquer le tri
    if sort_by == "taux_occupation":
        if sort_desc:
            query = query.order_by(desc(UrgencesEtatActuel.taux_occupation))
        else:
            query = query.order_by(UrgencesEtatActuel.taux_occupation)
    elif sort_by == "date_extraction":
        if sort_desc:
            query = query.order_by(desc(UrgencesEtatActuel.date_extraction))
        else:
            query = query.order_by(UrgencesEtatActuel.date_extraction)
    
    # Compter le nombre total d'éléments
    total = query.count()
    
    # Calculer le nombre total de pages
    pages = math.ceil(total / page_size)
    
    # Appliquer la pagination
    query = query.offset((page - 1) * page_size).limit(page_size)
    
    # Exécuter la requête
    urgences = query.all()
    
    # Construire la réponse
    items = []
    for urgence in urgences:
        # Extraire les données de l'établissement
        etab = urgence.etablissement
        
        # Extraire les coordonnées si disponibles
        lat, lon = None, None
        if etab.point_geo is not None:
            point_wkt = db.scalar(func.ST_AsText(etab.point_geo))
            if point_wkt and point_wkt.startswith("POINT("):
                coords = point_wkt.replace("POINT(", "").replace(")", "").split()
                if len(coords) == 2:
                    lon = float(coords[0])
                    lat = float(coords[1])
        
        # Créer le dictionnaire de l'établissement
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
            "longitude": lon
        }
        
        # Créer le dictionnaire de l'urgence
        urgence_dict = {
            "id": urgence.id,
            "etablissement_id": urgence.etablissement_id,
            "civieres_fonctionnelles": urgence.civieres_fonctionnelles,
            "civieres_occupees": urgence.civieres_occupees,
            "patients_24h": urgence.patients_24h,
            "patients_48h": urgence.patients_48h,
            "total_patients": urgence.total_patients,
            "patients_en_attente": urgence.patients_en_attente,
            "dms_civiere": urgence.dms_civiere,
            "dms_ambulatoire": urgence.dms_ambulatoire,
            "taux_occupation": urgence.taux_occupation,
            "date_extraction": urgence.date_extraction,
            "date_maj": urgence.date_maj,
            "statut_validation": urgence.statut_validation,
            "etablissement": etab_dict
        }
        
        items.append(urgence_dict)
    
    # Construire la réponse paginée
    response = {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": pages
    }
    
    return response

@router.get("/etablissement/{etablissement_id}", response_model=UrgenceResponse)
def get_urgence_by_etablissement(etablissement_id: int, db: Session = Depends(get_db)):
    """
    Récupère les données d'urgence pour un établissement spécifique.
    """
    urgence = (
        db.query(UrgencesEtatActuel)
        .filter(UrgencesEtatActuel.etablissement_id == etablissement_id)
        .first()
    )
    
    if urgence is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Données d'urgence non trouvées pour l'établissement {etablissement_id}"
        )
    
    # Créer un dictionnaire pour la sérialisation
    urgence_dict = {
        "id": urgence.id,
        "etablissement_id": urgence.etablissement_id,
        "civieres_fonctionnelles": urgence.civieres_fonctionnelles,
        "civieres_occupees": urgence.civieres_occupees,
        "patients_24h": urgence.patients_24h,
        "patients_48h": urgence.patients_48h,
        "total_patients": urgence.total_patients,
        "patients_en_attente": urgence.patients_en_attente,
        "dms_civiere": urgence.dms_civiere,
        "dms_ambulatoire": urgence.dms_ambulatoire,
        "taux_occupation": urgence.taux_occupation,
        "date_extraction": urgence.date_extraction,
        "date_maj": urgence.date_maj,
        "statut_validation": urgence.statut_validation
    }
    
    return UrgenceResponse(**urgence_dict)

@router.get("/historique/{etablissement_id}", response_model=List[UrgenceResponse])
def get_urgence_historique(
    etablissement_id: int, 
    db: Session = Depends(get_db),
    jours: int = Query(7, ge=1, le=30, description="Nombre de jours d'historique")
):
    """
    Récupère l'historique des données d'urgence pour un établissement spécifique.
    """
    # Calculer la date limite
    date_limite = datetime.utcnow() - timedelta(days=jours)
    
    # Requête pour l'historique
    historique = (
        db.query(UrgencesHistorique)
        .filter(
            UrgencesHistorique.etablissement_id == etablissement_id,
            UrgencesHistorique.date_extraction >= date_limite
        )
        .order_by(desc(UrgencesHistorique.date_extraction))
        .all()
    )
    
    if not historique:
        raise HTTPException(
            status_code=404, 
            detail=f"Historique des données d'urgence non trouvé pour l'établissement {etablissement_id}"
        )
    
    # Convertir les résultats pour la sérialisation
    resultats = []
    for hist in historique:
        hist_dict = {
            "id": hist.id,
            "etablissement_id": hist.etablissement_id,
            "civieres_fonctionnelles": hist.civieres_fonctionnelles,
            "civieres_occupees": hist.civieres_occupees,
            "patients_24h": hist.patients_24h,
            "patients_48h": hist.patients_48h,
            "total_patients": hist.total_patients,
            "patients_en_attente": hist.patients_en_attente,
            "dms_civiere": hist.dms_civiere,
            "dms_ambulatoire": hist.dms_ambulatoire,
            "taux_occupation": hist.taux_occupation,
            "date_extraction": hist.date_extraction,
            "date_maj": hist.date_maj,
            "statut_validation": hist.statut_validation
        }
        resultats.append(UrgenceResponse(**hist_dict))
    
    return resultats

@router.get("/historique/graphique/{etablissement_id}", response_model=List[dict])
def get_urgence_historique_graphique(
    etablissement_id: int, 
    db: Session = Depends(get_db),
    jours: int = Query(7, ge=1, le=30, description="Nombre de jours d'historique"),
    interval: str = Query("jour", description="Intervalle de temps (heure, jour)")
):
    """
    Récupère l'historique des données d'urgence formatées pour un graphique.
    
    Retourne les données d'occupation des urgences pour un établissement sur les derniers jours,
    regroupées par l'intervalle spécifié.
    """
    # Calculer la date limite
    date_limite = datetime.utcnow() - timedelta(days=jours)
    
    # Requête de base pour l'historique
    query = (
        db.query(UrgencesHistorique)
        .filter(
            UrgencesHistorique.etablissement_id == etablissement_id,
            UrgencesHistorique.date_extraction >= date_limite
        )
    )
    
    # Si l'intervalle est 'jour', regrouper par jour
    if interval == "jour":
        # Utiliser une requête SQL directe pour extraire les données agrégées par jour
        sql_query = text("""
SELECT 
    DATE_TRUNC('day', date_extraction) AS jour,
    AVG(taux_occupation) AS taux_moyen,
    MAX(taux_occupation) AS taux_max,
    MIN(taux_occupation) AS taux_min,
    AVG(patients_en_attente) AS patients_attente_moyen
FROM 
    urgences_historique
WHERE 
    etablissement_id = :etablissement_id AND
    date_extraction >= :date_limite
GROUP BY 
    DATE_TRUNC('day', date_extraction)
ORDER BY 
    jour
""")
        result = db.execute(sql_query, {
            "etablissement_id": etablissement_id,
            "date_limite": date_limite
        }).fetchall()
        
        # Formater les résultats pour le graphique
        data = []
        for jour, taux_moyen, taux_max, taux_min, patients_attente_moyen in result:
            data.append({
                "date": jour.isoformat(),
                "taux_occupation": round(taux_moyen, 2) if taux_moyen else None,
                "taux_max": round(taux_max, 2) if taux_max else None,
                "taux_min": round(taux_min, 2) if taux_min else None,
                "patients_en_attente": round(patients_attente_moyen, 2) if patients_attente_moyen else None
            })
    
    # Si l'intervalle est 'heure', renvoyer toutes les données
    else:
        # Utiliser la requête ORM standard pour les données horaires
        historique = query.order_by(UrgencesHistorique.date_extraction).all()
        
        if not historique:
            raise HTTPException(
                status_code=404, 
                detail=f"Historique des données d'urgence non trouvé pour l'établissement {etablissement_id}"
            )
        
        # Formater les résultats pour le graphique
        data = []
        for hist in historique:
            data.append({
                "date": hist.date_extraction.isoformat(),
                "taux_occupation": hist.taux_occupation,
                "patients_en_attente": hist.patients_en_attente,
                "patients_24h": hist.patients_24h
            })
    
    return data


@router.get("/{urgence_id}", response_model=UrgenceWithEtablissement)
def get_urgence(urgence_id: int, db: Session = Depends(get_db)):
    """
    Récupère les détails d'une urgence spécifique.
    """
    urgence = (
        db.query(UrgencesEtatActuel)
        .filter(UrgencesEtatActuel.id == urgence_id)
        .options(joinedload(UrgencesEtatActuel.etablissement))
        .first()
    )
    
    if urgence is None:
        raise HTTPException(status_code=404, detail="Données d'urgence non trouvées")
    
    # Extraire les données de l'établissement
    etab = urgence.etablissement
    
    # Extraire les coordonnées si disponibles
    lat, lon = None, None
    if etab.point_geo is not None:
        point_wkt = db.scalar(func.ST_AsText(etab.point_geo))
        if point_wkt and point_wkt.startswith("POINT("):
            coords = point_wkt.replace("POINT(", "").replace(")", "").split()
            if len(coords) == 2:
                lon = float(coords[0])
                lat = float(coords[1])
    
    # Créer le dictionnaire de l'établissement
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
        "longitude": lon
    }
    
    # Créer le dictionnaire de l'urgence
    urgence_dict = {
        "id": urgence.id,
        "etablissement_id": urgence.etablissement_id,
        "civieres_fonctionnelles": urgence.civieres_fonctionnelles,
        "civieres_occupees": urgence.civieres_occupees,
        "patients_24h": urgence.patients_24h,
        "patients_48h": urgence.patients_48h,
        "total_patients": urgence.total_patients,
        "patients_en_attente": urgence.patients_en_attente,
        "dms_civiere": urgence.dms_civiere,
        "dms_ambulatoire": urgence.dms_ambulatoire,
        "taux_occupation": urgence.taux_occupation,
        "date_extraction": urgence.date_extraction,
        "date_maj": urgence.date_maj,
        "statut_validation": urgence.statut_validation,
        "etablissement": etab_dict
    }
    
    return UrgenceWithEtablissement(**urgence_dict)