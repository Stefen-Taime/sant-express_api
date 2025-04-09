# Guide d'utilisation et de déploiement de l'API Santé Express

## Présentation du projet

L'API Santé Express est une interface RESTful conçue pour permettre aux utilisateurs de trouver les établissements de santé les plus proches et de connaître l'état des urgences en temps réel. Cette API traite des données ouvertes provenant de plusieurs sources, notamment la Base de données des établissements de santé (BDOES) et les relevés horaires de la situation dans les urgences du Québec.

## Fonctionnalités principales

- **Gestion des établissements de santé** : Recherche, filtrage et détails des établissements
- **Recherche géospatiale** : Trouver les établissements à proximité d'une position géographique
- **Données d'urgence en temps réel** : État actuel des urgences avec taux d'occupation
- **Système de recommandation** : Suggestion d'urgences moins engorgées à proximité
- **Statistiques régionales** : Agrégation des données par région
- **Mise à jour automatique** : Téléchargement et traitement horaire des nouvelles données
- **Historisation des données** : Conservation des données historiques pour analyse

## Architecture technique

L'API est construite avec les technologies suivantes :

- **Backend** : Python avec FastAPI
- **Base de données** : PostgreSQL avec PostGIS pour les données géospatiales
- **ORM** : SQLAlchemy pour l'interaction avec la base de données
- **Validation des données** : Pydantic pour la validation des schémas
- **Traitement de données** : Pandas, chardet et ftfy pour la manipulation et correction d'encodage
- **Déploiement** : Docker et Docker Compose
- **Planification des tâches** : APScheduler pour les mises à jour périodiques

## Structure du projet

```
sante_express_api/
├── app/
│   ├── api/
│   │   ├── etablissements.py    # Endpoints pour les établissements
│   │   ├── urgences.py          # Endpoints pour les urgences
│   │   └── recommandations.py   # Endpoints pour les recommandations
│   ├── core/
│   │   └── scheduler.py         # Planificateur de tâches
│   ├── db/
│   │   └── database.py          # Configuration de la base de données
│   ├── models/
│   │   └── models.py            # Modèles SQLAlchemy
│   ├── schemas/
│   │   └── schemas.py           # Schémas Pydantic
│   ├── utils/
│   │   ├── encoding_utils.py    # Utilitaires pour l'encodage
│   │   └── data_validation.py   # Validation des données
│   └── main.py                  # Point d'entrée de l'application
├── data/                        # Répertoire pour les fichiers CSV
├── logs/                        # Répertoire pour les journaux
├── Dockerfile                   # Configuration Docker
├── docker-compose.yml           # Configuration Docker Compose
└── requirements.txt             # Dépendances Python
```

## Endpoints API

### Établissements

- `GET /api/etablissements` - Liste des établissements avec filtrage
- `GET /api/etablissements/{id}` - Détails d'un établissement spécifique
- `POST /api/etablissements/proximite` - Trouver les établissements proches d'une position

### Urgences

- `GET /api/urgences` - Données actuelles de toutes les urgences
- `GET /api/urgences/{id}` - Données d'une urgence spécifique
- `GET /api/urgences/etablissement/{etablissement_id}` - Données d'urgence pour un établissement
- `GET /api/urgences/historique/{etablissement_id}` - Historique des données d'urgence
- `GET /api/urgences/regions` - Statistiques par région

### Recommandations

- `POST /api/recommandations` - Suggestion d'urgences moins engorgées à proximité

## Traitement des données

L'API implémente une stratégie robuste pour traiter les problèmes d'encodage et de format dans les fichiers CSV :

1. **Détection automatique d'encodage** : Utilisation de chardet pour détecter l'encodage des fichiers
2. **Correction des caractères accentués** : Utilisation de ftfy pour corriger les problèmes d'encodage
3. **Normalisation des noms** : Standardisation des noms d'établissements pour faciliter la correspondance
4. **Validation des données** : Vérification des plages numériques et des champs requis
5. **Journalisation des anomalies** : Enregistrement des problèmes pour analyse ultérieure

## Déploiement

### Prérequis

- Docker et Docker Compose installés
- Git pour cloner le dépôt

### Instructions de déploiement

1. **Cloner le dépôt** :
   ```bash
   git clone <url-du-depot>
   cd sante_express_api
   ```

2. **Configuration** :
   - Vérifier les variables d'environnement dans le fichier `docker-compose.yml`
   - Ajuster les paramètres selon vos besoins (ports, mots de passe, etc.)

3. **Démarrer les services** :
   ```bash
   docker-compose up -d
   ```

4. **Initialiser la base de données** :
   ```bash
   docker-compose exec api python -c "from app.models.models import Base; from app.db.database import engine; Base.metadata.create_all(bind=engine)"
   ```

5. **Vérifier le déploiement** :
   - Accéder à la documentation de l'API : http://localhost:8000/api/docs
   - Vérifier les logs : `docker-compose logs -f api`

### Environnements de déploiement

Le projet est configuré pour fonctionner dans deux environnements :

- **Développement** : Configuration par défaut dans docker-compose.yml
- **Production** : Nécessite des ajustements de sécurité (voir ci-dessous)

#### Ajustements pour la production

Pour un déploiement en production, modifiez les paramètres suivants :

1. **Variables d'environnement** :
   ```yaml
   environment:
     - DATABASE_URL=postgresql://user:password@db:5432/sante_express
     - ENVIRONMENT=production
     - LOG_LEVEL=WARNING
   ```

2. **Sécurité CORS** :
   Modifiez le fichier `app/main.py` pour restreindre les origines CORS :
   ```python
   app.add_middleware(
       CORSMiddleware,
       allow_origins=["https://votre-domaine.com"],
       allow_credentials=True,
       allow_methods=["*"],
       allow_headers=["*"],
   )
   ```

3. **Volumes persistants** :
   Assurez-vous que les volumes sont correctement configurés pour la persistance des données.

## Mise à jour des données

Les données sont mises à jour automatiquement selon la configuration suivante :

- **Fréquence** : Mise à jour horaire des données d'urgence
- **Source** : Téléchargement automatique depuis les portails de données ouvertes
- **Historisation** : Conservation des données historiques pour analyses futures

Le service `scheduler` dans Docker Compose gère ces mises à jour périodiques.

## Performance et optimisation

L'API est optimisée pour gérer environ 50-100 requêtes simultanées avec des temps de réponse inférieurs à 500ms pour les requêtes standard. Les optimisations incluent :

- **Index géospatiaux** : Utilisation d'index GiST pour les requêtes géospatiales
- **Pagination** : Limitation du nombre de résultats par page
- **Pool de connexions** : Configuration optimisée du pool de connexions à la base de données
- **Mise en cache** : Possibilité d'ajouter une couche de cache pour les requêtes fréquentes

## Dépannage

### Problèmes courants

1. **Erreur de connexion à la base de données** :
   - Vérifier que le service PostgreSQL est en cours d'exécution
   - Vérifier les identifiants de connexion dans les variables d'environnement

2. **Erreur lors du téléchargement des données** :
   - Vérifier la connectivité Internet
   - Vérifier que les URLs des sources de données sont correctes

3. **Performances lentes** :
   - Vérifier l'utilisation des ressources (CPU, mémoire)
   - Optimiser les requêtes SQL avec des index supplémentaires si nécessaire

### Journaux

Les journaux sont disponibles dans le répertoire `logs/` et via les commandes Docker :

```bash
# Journaux de l'API
docker-compose logs -f api

# Journaux du planificateur
docker-compose logs -f scheduler

# Journaux de la base de données
docker-compose logs -f db
```

## Conclusion

L'API Santé Express fournit une solution complète pour accéder aux données des établissements de santé et des urgences en temps réel. Sa conception robuste permet de gérer les problèmes d'encodage des données sources tout en offrant des fonctionnalités avancées comme la recherche géospatiale et les recommandations intelligentes.

Pour toute question ou problème, veuillez consulter la documentation ou contacter l'équipe de développement.
# sant-express_api
