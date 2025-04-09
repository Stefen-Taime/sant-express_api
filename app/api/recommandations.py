"""
Endpoints API pour les recommandations d'urgences.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, desc, and_
from typing import List, Optional
from geoalchemy2.functions import ST_Distance, ST_SetSRID, ST_MakePoint
from app.db.database import get_db
from app.models.models import Etablissement, UrgencesEtatActuel
from app.schemas.schemas import (
    RecommandationResponse, ProximiteRequest
)

router = APIRouter(
    prefix="/api/recommandations",
    tags=["recommandations"],
    responses={404: {"description": "Aucune recommandation disponible"}},
)

@router.post("/", response_model=List[RecommandationResponse])
def get_recommandations(
    request: ProximiteRequest,
    db: Session = Depends(get_db),
    nb_recommandations: int = Query(5, ge=1, le=10, description="Nombre de recommandations à retourner")
):
    """
    Suggère des urgences moins engorgées à proximité d'une position géographique.
    
    L'algorithme prend en compte:
    - La distance (plus proche = meilleur)
    - Le taux d'occupation (plus bas = meilleur)
    - Le temps d'attente (plus court = meilleur)
    """
    # Convertir le rayon de km en mètres pour le calcul
    rayon_metres = request.rayon * 1000
    
    # Créer un point géographique à partir des coordonnées
    point = ST_SetSRID(ST_MakePoint(request.longitude, request.latitude), 4326)
    
    # Requête pour trouver les établissements à proximité avec leurs données d'urgence
    query = (
        db.query(
            Etablissement,
            UrgencesEtatActuel,
            # Calculer la distance en mètres puis convertir en km
            (ST_Distance(Etablissement.point_geo, point, use_spheroid=True) / 1000.0).label("distance")
        )
        .join(UrgencesEtatActuel, Etablissement.id == UrgencesEtatActuel.etablissement_id)
        .filter(ST_Distance(Etablissement.point_geo, point, use_spheroid=True) <= rayon_metres)
        .filter(UrgencesEtatActuel.taux_occupation.isnot(None))  # Exclure les établissements sans données d'occupation
    )
    
    # Exécuter la requête
    results = query.all()
    
    if not results:
        raise HTTPException(
            status_code=404,
            detail="Aucune urgence avec données disponibles dans le rayon spécifié"
        )
    
    # Calculer le score pour chaque établissement
    recommandations = []
    for etablissement, urgence, distance in results:
        # Normaliser les valeurs (plus le score est élevé, meilleure est la recommandation)
        
        # Distance: plus proche = meilleur score (max 50 points)
        # Formule: 50 * (1 - distance/rayon_max) pour donner un score entre 0 et 50
        score_distance = 50 * (1 - min(distance / request.rayon, 1))
        
        # Taux d'occupation: plus bas = meilleur score (max 40 points)
        # Formule: 40 * (1 - taux/100) pour donner un score entre 0 et 40
        score_occupation = 40 * (1 - min(urgence.taux_occupation / 100, 1)) if urgence.taux_occupation else 0
        
        # Temps d'attente: moins de patients en attente = meilleur score (max 10 points)
        # Formule: 10 * (1 - nb_patients/20) pour donner un score entre 0 et 10
        score_attente = 10 * (1 - min(urgence.patients_en_attente / 20, 1)) if urgence.patients_en_attente else 10
        
        # Score total
        score_total = score_distance + score_occupation + score_attente
        
        # Créer l'objet de recommandation
        recommandation = {
            "etablissement": etablissement,
            "urgence": urgence,
            "distance": round(distance, 2),
            "score": round(score_total, 2)
        }
        
        recommandations.append(recommandation)
    
    # Trier par score décroissant et limiter au nombre demandé
    recommandations.sort(key=lambda x: x["score"], reverse=True)
    recommandations = recommandations[:nb_recommandations]
    
    # Convertir en objets Pydantic
    return [RecommandationResponse(**r) for r in recommandations]
