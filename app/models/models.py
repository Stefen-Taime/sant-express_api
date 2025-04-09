"""
Modèles SQLAlchemy pour la base de données PostgreSQL avec PostGIS.
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Text, func, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
from datetime import datetime

Base = declarative_base()

class Region(Base):
    """
    Modèle pour la table des régions.
    """
    __tablename__ = "regions"
    
    id = Column(Integer, primary_key=True)
    rss = Column(String(10), nullable=False, unique=True, comment="Code de la région sociosanitaire")
    nom = Column(String(255), nullable=False, comment="Nom de la région")
    nom_normalise = Column(String(255), nullable=False, comment="Nom normalisé sans accents pour la recherche")
    geom = Column(Geometry('POLYGON', srid=4326), nullable=True, comment="Géométrie de la région")
    
    # Relations
    etablissements = relationship("Etablissement", back_populates="region")
    
    # Index
    __table_args__ = (
        Index('idx_regions_nom_normalise', nom_normalise),
    )
    
    def __repr__(self):
        return f"<Region(id={self.id}, rss={self.rss}, nom={self.nom})>"


class Etablissement(Base):
    """
    Modèle pour la table des établissements de santé.
    """
    __tablename__ = "etablissements"
    
    id = Column(Integer, primary_key=True)
    source_id = Column(String(50), nullable=True, comment="Identifiant dans le fichier source")
    no_permis_installation = Column(String(50), nullable=True, comment="Numéro de permis de l'installation")
    nom_etablissement = Column(String(255), nullable=False, comment="Nom de l'établissement")
    nom_etablissement_normalise = Column(String(255), nullable=False, comment="Nom normalisé sans accents pour la recherche")
    nom_installation = Column(String(255), nullable=True, comment="Nom de l'installation")
    nom_installation_normalise = Column(String(255), nullable=True, comment="Nom normalisé sans accents pour la recherche")
    type = Column(String(100), nullable=True, comment="Type d'établissement")
    adresse = Column(String(255), nullable=True, comment="Adresse de l'établissement")
    code_postal = Column(String(10), nullable=True, comment="Code postal")
    ville = Column(String(100), nullable=True, comment="Ville")
    province = Column(String(50), nullable=True, comment="Province")
    region_id = Column(Integer, ForeignKey('regions.id'), nullable=True, comment="ID de la région")
    point_geo = Column(Geometry('POINT', srid=4326), nullable=True, comment="Coordonnées géographiques")
    date_maj = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment="Date de dernière mise à jour")
    
    # Relations
    region = relationship("Region", back_populates="etablissements")
    urgences_etat_actuel = relationship("UrgencesEtatActuel", back_populates="etablissement", uselist=False)
    urgences_historique = relationship("UrgencesHistorique", back_populates="etablissement")
    
    # Index
    __table_args__ = (
        Index('idx_etablissements_nom_etablissement_normalise', nom_etablissement_normalise),
        Index('idx_etablissements_nom_installation_normalise', nom_installation_normalise),
        Index('idx_etablissements_point_geo', point_geo, postgresql_using='gist'),
    )
    
    def __repr__(self):
        return f"<Etablissement(id={self.id}, nom={self.nom_etablissement})>"


class UrgencesEtatActuel(Base):
    """
    Modèle pour la table des états actuels des urgences.
    """
    __tablename__ = "urgences_etat_actuel"
    
    id = Column(Integer, primary_key=True)
    etablissement_id = Column(Integer, ForeignKey('etablissements.id'), nullable=False, unique=True, comment="ID de l'établissement")
    civieres_fonctionnelles = Column(Float, nullable=True, comment="Nombre de civières fonctionnelles")
    civieres_occupees = Column(Float, nullable=True, comment="Nombre de civières occupées")
    patients_24h = Column(Float, nullable=True, comment="Nombre de patients sur civière depuis plus de 24h")
    patients_48h = Column(Float, nullable=True, comment="Nombre de patients sur civière depuis plus de 48h")
    total_patients = Column(Float, nullable=True, comment="Nombre total de patients")
    patients_en_attente = Column(Float, nullable=True, comment="Nombre de patients en attente")
    dms_civiere = Column(Float, nullable=True, comment="Durée moyenne de séjour sur civière (heures)")
    dms_ambulatoire = Column(Float, nullable=True, comment="Durée moyenne de séjour ambulatoire (heures)")
    taux_occupation = Column(Float, nullable=True, comment="Taux d'occupation (%)")
    date_extraction = Column(DateTime, nullable=False, comment="Date d'extraction des données")
    date_maj = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, comment="Date de dernière mise à jour")
    statut_validation = Column(String(50), nullable=True, default="non_validé", comment="Statut de validation des données")
    
    # Relations
    etablissement = relationship("Etablissement", back_populates="urgences_etat_actuel")
    
    # Index
    __table_args__ = (
        Index('idx_urgences_etat_actuel_taux_occupation', taux_occupation),
        Index('idx_urgences_etat_actuel_date_extraction', date_extraction),
    )
    
    def __repr__(self):
        return f"<UrgencesEtatActuel(id={self.id}, etablissement_id={self.etablissement_id}, taux_occupation={self.taux_occupation})>"


class UrgencesHistorique(Base):
    """
    Modèle pour la table historique des urgences.
    """
    __tablename__ = "urgences_historique"
    
    id = Column(Integer, primary_key=True)
    etablissement_id = Column(Integer, ForeignKey('etablissements.id'), nullable=False, comment="ID de l'établissement")
    civieres_fonctionnelles = Column(Float, nullable=True, comment="Nombre de civières fonctionnelles")
    civieres_occupees = Column(Float, nullable=True, comment="Nombre de civières occupées")
    patients_24h = Column(Float, nullable=True, comment="Nombre de patients sur civière depuis plus de 24h")
    patients_48h = Column(Float, nullable=True, comment="Nombre de patients sur civière depuis plus de 48h")
    total_patients = Column(Float, nullable=True, comment="Nombre total de patients")
    patients_en_attente = Column(Float, nullable=True, comment="Nombre de patients en attente")
    dms_civiere = Column(Float, nullable=True, comment="Durée moyenne de séjour sur civière (heures)")
    dms_ambulatoire = Column(Float, nullable=True, comment="Durée moyenne de séjour ambulatoire (heures)")
    taux_occupation = Column(Float, nullable=True, comment="Taux d'occupation (%)")
    date_extraction = Column(DateTime, nullable=False, comment="Date d'extraction des données")
    date_maj = Column(DateTime, default=datetime.utcnow, comment="Date de dernière mise à jour")
    statut_validation = Column(String(50), nullable=True, default="non_validé", comment="Statut de validation des données")
    
    # Relations
    etablissement = relationship("Etablissement", back_populates="urgences_historique")
    
    # Index
    __table_args__ = (
        Index('idx_urgences_historique_etablissement_id', etablissement_id),
        Index('idx_urgences_historique_date_extraction', date_extraction),
        Index('idx_urgences_historique_taux_occupation', taux_occupation),
    )
    
    def __repr__(self):
        return f"<UrgencesHistorique(id={self.id}, etablissement_id={self.etablissement_id}, date_extraction={self.date_extraction})>"

class TempMRC(Base):
    """
    Modèle pour la table temporaire des MRC (Municipalités Régionales de Comté).
    """
    __tablename__ = "temp_mrc"
    
    gid = Column(Integer, primary_key=True, comment="Identifiant unique")
    mrs_no_ind = Column(String(50), nullable=True, comment="Indicateur numérique")
    mrs_de_ind = Column(String(50), nullable=True, comment="Indicateur descriptif")
    mrs_co_mrc = Column(String(50), nullable=True, comment="Code de la MRC")
    mrs_nm_mrc = Column(String(255), nullable=True, comment="Nom de la MRC")
    mrs_co_reg = Column(String(10), nullable=True, comment="Code de la région administrative")
    mrs_nm_reg = Column(String(255), nullable=True, comment="Nom de la région administrative")
    mrs_co_ref = Column(String(50), nullable=True, comment="Code de référence")
    mrs_co_ver = Column(String(50), nullable=True, comment="Code de version")
    geom = Column(Geometry('MULTIPOLYGON', srid=4326), nullable=True, comment="Géométrie de la MRC")
    
    # Index
    __table_args__ = (
        Index('idx_temp_mrc_geom', geom, postgresql_using='gist'),
    )
    
    def __repr__(self):
        return f"<TempMRC(gid={self.gid}, nom={self.mrs_nm_mrc})>"