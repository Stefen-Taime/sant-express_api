#!/bin/bash

# Script de démarrage amélioré pour le service scheduler
# Ce script s'assure que la base de données est correctement initialisée
# avant de démarrer le scheduler

echo "Démarrage du service Sante Express Scheduler"

# Définir les variables d'environnement si nécessaire
export PYTHONPATH=/app

# Attendre que la base de données soit disponible
echo "Attente de la base de données..."
for i in {1..30}; do
    if pg_isready -h db -U postgres; then
        echo "Base de données disponible!"
        break
    fi
    
    echo "Tentative $i/30: Base de données non disponible. Attente de 5 secondes..."
    sleep 5
    
    if [ $i -eq 30 ]; then
        echo "Impossible de se connecter à la base de données après plusieurs tentatives."
        exit 1
    fi
done

# Initialiser la base de données avec le nouveau script
echo "Initialisation de la base de données..."
python scripts/create_tables.py

# Vérifier le résultat de l'initialisation
if [ $? -ne 0 ]; then
    echo "Échec de l'initialisation de la base de données. Tentative avec une méthode alternative..."
    
    # Tentative alternative pour créer les tables directement avec psql
    echo "Création des tables avec psql..."
    PGPASSWORD=postgres psql -h db -U postgres -d sante_express -c "
    CREATE EXTENSION IF NOT EXISTS postgis;
    
    DROP TABLE IF EXISTS urgences_historique;
    DROP TABLE IF EXISTS urgences_etat_actuel;
    DROP TABLE IF EXISTS etablissements;
    DROP TABLE IF EXISTS regions;
    
    CREATE TABLE regions (
        id SERIAL PRIMARY KEY,
        rss VARCHAR(2) NOT NULL,
        nom VARCHAR(100) NOT NULL,
        nom_normalise VARCHAR(100) NOT NULL,
        geom GEOMETRY(MULTIPOLYGON, 4326)
    );
    
    CREATE INDEX idx_regions_rss ON regions (rss);
    CREATE INDEX idx_regions_nom_normalise ON regions (nom_normalise);
    CREATE INDEX idx_regions_geom ON regions USING gist (geom);
    
    CREATE TABLE etablissements (
        id SERIAL PRIMARY KEY,
        source_id VARCHAR(100),
        no_permis_installation VARCHAR(100),
        nom_etablissement VARCHAR(255),
        nom_etablissement_normalise VARCHAR(255),
        nom_installation VARCHAR(255),
        nom_installation_normalise VARCHAR(255),
        type VARCHAR(100),
        adresse VARCHAR(255),
        code_postal VARCHAR(10),
        ville VARCHAR(100),
        province VARCHAR(100),
        region_id INTEGER REFERENCES regions(id),
        point_geo GEOMETRY(POINT, 4326),
        date_maj TIMESTAMP WITHOUT TIME ZONE
    );
    
    CREATE INDEX idx_etablissements_nom_etablissement_normalise ON etablissements (nom_etablissement_normalise);
    CREATE INDEX idx_etablissements_nom_installation_normalise ON etablissements (nom_installation_normalise);
    CREATE INDEX idx_etablissements_region_id ON etablissements (region_id);
    CREATE INDEX idx_etablissements_point_geo ON etablissements USING gist (point_geo);
    
    CREATE TABLE urgences_etat_actuel (
        id SERIAL PRIMARY KEY,
        etablissement_id INTEGER NOT NULL REFERENCES etablissements(id),
        civieres_fonctionnelles INTEGER,
        civieres_occupees INTEGER,
        patients_24h INTEGER,
        patients_48h INTEGER,
        total_patients INTEGER,
        patients_en_attente INTEGER,
        dms_civiere FLOAT,
        dms_ambulatoire FLOAT,
        taux_occupation FLOAT,
        date_extraction TIMESTAMP WITHOUT TIME ZONE,
        date_maj TIMESTAMP WITHOUT TIME ZONE,
        statut_validation VARCHAR(50)
    );
    
    CREATE INDEX idx_urgences_etat_actuel_etablissement_id ON urgences_etat_actuel (etablissement_id);
    CREATE INDEX idx_urgences_etat_actuel_date_extraction ON urgences_etat_actuel (date_extraction);
    
    CREATE TABLE urgences_historique (
        id SERIAL PRIMARY KEY,
        etablissement_id INTEGER NOT NULL REFERENCES etablissements(id),
        civieres_fonctionnelles INTEGER,
        civieres_occupees INTEGER,
        patients_24h INTEGER,
        patients_48h INTEGER,
        total_patients INTEGER,
        patients_en_attente INTEGER,
        dms_civiere FLOAT,
        dms_ambulatoire FLOAT,
        taux_occupation FLOAT,
        date_extraction TIMESTAMP WITHOUT TIME ZONE,
        date_maj TIMESTAMP WITHOUT TIME ZONE,
        statut_validation VARCHAR(50)
    );
    
    CREATE INDEX idx_urgences_historique_etablissement_id ON urgences_historique (etablissement_id);
    CREATE INDEX idx_urgences_historique_date_extraction ON urgences_historique (date_extraction);
    
    -- Insérer les régions du Québec
    INSERT INTO regions (rss, nom, nom_normalise) VALUES
    ('01', 'Bas-Saint-Laurent', 'bas saint laurent'),
    ('02', 'Saguenay-Lac-Saint-Jean', 'saguenay lac saint jean'),
    ('03', 'Capitale-Nationale', 'capitale nationale'),
    ('04', 'Mauricie et Centre-du-Québec', 'mauricie et centre du quebec'),
    ('05', 'Estrie', 'estrie'),
    ('06', 'Montréal', 'montreal'),
    ('07', 'Outaouais', 'outaouais'),
    ('08', 'Abitibi-Témiscamingue', 'abitibi temiscamingue'),
    ('09', 'Côte-Nord', 'cote nord'),
    ('10', 'Nord-du-Québec', 'nord du quebec'),
    ('11', 'Gaspésie-Îles-de-la-Madeleine', 'gaspesie iles de la madeleine'),
    ('12', 'Chaudière-Appalaches', 'chaudiere appalaches'),
    ('13', 'Laval', 'laval'),
    ('14', 'Lanaudière', 'lanaudiere'),
    ('15', 'Laurentides', 'laurentides'),
    ('16', 'Montérégie', 'monteregie'),
    ('17', 'Nunavik', 'nunavik'),
    ('18', 'Terres-Cries-de-la-Baie-James', 'terres cries de la baie james');
    "
    
    if [ $? -ne 0 ]; then
        echo "Échec de la création des tables avec psql. Abandon du démarrage."
        exit 1
    else
        echo "Tables créées avec succès via psql."
    fi
fi

# Démarrer le planificateur
echo "Démarrage du planificateur..."
python -m app.core.scheduler

# En cas d'erreur, afficher un message
if [ $? -ne 0 ]; then
    echo "Le planificateur s'est arrêté avec un code d'erreur."
    exit 1
fi