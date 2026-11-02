# World Bank → GCS → BigQuery avec Airflow

Pipeline d'ingestion des indicateurs de l'API World Bank vers BigQuery, orchestré par Airflow :

- **en local** avec Docker Compose (test) ;
- **sur GCP** avec Cloud Composer (managed Airflow).

```
World Bank API → (Airflow) CSV → Cloud Storage (raw) → BigQuery (raw_worldbank.indicators)
```

## Prérequis

- Un projet GCP avec facturation activée
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) (`gcloud`, `bq`) installé et authentifié
- Docker Desktop (test local)
- Git

> Remplace partout `PROJECT_ID` par l'ID de ton projet et `REGION` par `europe-west9` (Paris).
> Les commandes sont écrites sur **une seule ligne** pour fonctionner à la fois sous PowerShell, cmd et bash.

---

## 1. Configurer le projet

```
gcloud config set project PROJECT_ID
```

## 2. Activer les APIs

```
gcloud services enable composer.googleapis.com storage.googleapis.com bigquery.googleapis.com iamcredentials.googleapis.com
```

---

## 3. Compte de service

Composer exige un compte de service **explicitement** précisé.

```
gcloud iam service-accounts create composer-sa --display-name="Composer service account"
```

Rôles (moindre privilège) :

| Rôle | Portée | Usage |
| --- | --- | --- |
| `roles/composer.worker` | Projet | Exécution de l'environnement Composer |
| `roles/bigquery.dataEditor` | Projet (ou dataset) | Écriture des tables |
| `roles/bigquery.jobUser` | Projet | Lancer les jobs de chargement |
| `roles/storage.objectAdmin` | **Bucket raw uniquement** | Lire/écrire les fichiers bruts |

```
gcloud projects add-iam-policy-binding PROJECT_ID --member="serviceAccount:composer-sa@PROJECT_ID.iam.gserviceaccount.com" --role="roles/composer.worker"

gcloud projects add-iam-policy-binding PROJECT_ID --member="serviceAccount:composer-sa@PROJECT_ID.iam.gserviceaccount.com" --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding PROJECT_ID --member="serviceAccount:composer-sa@PROJECT_ID.iam.gserviceaccount.com" --role="roles/bigquery.jobUser"
```

Le rôle Storage est attribué après la création du bucket (étape 4).

> Si une erreur de permission apparaît à la création de l'environnement, ton propre compte a besoin de `roles/iam.serviceAccountUser` sur `composer-sa`.

---

## 4. Bucket Cloud Storage (zone brute)

```
gcloud storage buckets create gs://PROJECT_ID-worldbank-raw --location=REGION --default-storage-class=STANDARD --uniform-bucket-level-access --public-access-prevention
```

Droit du compte de service sur ce bucket uniquement :

```
gcloud storage buckets add-iam-policy-binding gs://PROJECT_ID-worldbank-raw --member="serviceAccount:composer-sa@PROJECT_ID.iam.gserviceaccount.com" --role="roles/storage.objectAdmin"
```

> Composer crée aussi son propre bucket (`REGION-<env>-xxxx-bucket`, avec `dags/`, `data/`, `logs/`) : ne pas l'utiliser pour les données brutes.

## 5. Dataset et table BigQuery

```
bq --location=REGION mk --dataset PROJECT_ID:raw_worldbank
```

## 6. Vérifications

```
gcloud storage ls
bq ls --project_id=PROJECT_ID
gcloud composer environments list --locations=REGION
```

---

## 7. Test local (Docker Compose)

### Authentification (PowerShell)

```
gcloud auth login
gcloud config set project PROJECT_ID
gcloud auth application-default login
```

Vérifier que le fichier ADC existe :

```
Test-Path "$env:APPDATA\gcloud\application_default_credentials.json"
```

Vérifier qu'il est bien monté dans le conteneur (sans afficher son contenu) :

```
docker compose exec airflow ls -l /home/airflow/.config/gcloud/application_default_credentials.json
```

> ⚠️ Ce fichier est un secret : le monter en **lecture seule**, ne jamais l'afficher, le copier dans l'image ni le commiter (`.gitignore`).

### Lancement

```
docker compose up -d
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

1. Ouvrir <http://localhost:8080> et se connecter avec le mot de passe affiché.
2. Lancer le DAG `worldbank_gdp_ingestion` (trigger) et vérifier les logs.
3. Arrêter : `docker compose down`

---

## 8. Déploiement sur Cloud Composer

### Créer l'environnement (≈ 20-30 min)

```
gcloud composer environments create wb-bootcamp-env --location REGION --image-version composer-3-airflow-2.11.1-build.5 --service-account composer-sa@PROJECT_ID.iam.gserviceaccount.com
```

Vérifier l'état (`RUNNING` / Healthy) :

```
gcloud composer environments describe wb-bootcamp-env --location REGION
```

### Adaptations du DAG par rapport au local

- Supprimer toute gestion de credentials : le compte `composer-sa` est utilisé automatiquement (`google_cloud_default`).
- Écrire le CSV temporaire dans `/home/airflow/gcs/data/indicators.csv`.
- Ne rien coder en dur (projet, bucket) : utiliser les variables composer  d'environnement avec un préfixe personnel (`WB_...`) : les noms comme `GCP_PROJECT` ou `COMPOSER_*` sont réservés.

### Déployer le DAG

```
gcloud composer environments storage dags import --environment wb-bootcamp-env --location REGION --source dags/worldbank_gdp_ingestion.py
```

Ou via la Console : Composer → environnement → onglet **DAGs** → **Open DAGs folder** → **Upload files**.

### Exécuter et contrôler

1. Console → Composer → **Airflow UI** (le DAG apparaît après 1-2 min).
2. Activer le DAG puis **Trigger DAG**.
3. Vérifier le fichier `worldbank/<ds>/indicators.csv` dans le bucket raw.
4. Vérifier les données :

```
SELECT * FROM raw_worldbank.indicators LIMIT 20;
```

> Le DAG `airflow_monitoring` est un DAG système de Composer (heartbeat toutes les 5 min) : ne pas le mettre en pause.

---

## 9. Nettoyage (coûts)

Un environnement Composer est facturé tant qu'il existe. À supprimer après les tests :

```
gcloud composer environments delete wb-bootcamp-env --location REGION
```

Le bucket créé automatiquement par Composer n'est pas supprimé avec l'environnement : à supprimer manuellement s'il n'est plus utile.

## Dépannage

| Erreur | Cause / solution |
| --- | --- |
| `Please enable all APIs ... iamcredentials.googleapis.com` | Activer l'API `iamcredentials` (étape 2) |
| `Composer environment service account is required...` | Ajouter `--service-account` à la commande `create` |
| `--service-account: command not found` | Commande coupée sur deux lignes : tout mettre sur une seule ligne |
| `DefaultCredentialsError` | Local : refaire `gcloud auth application-default login` et vérifier le volume ; sur Composer : vérifier le compte de service |
| `This environment variable name is reserved` | Choisir un autre nom (préfixe `WB_`) ou une Variable Airflow |
