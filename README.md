# EU Energy Data Pipeline

Ce projet vise à construire un pipeline de données simple et reproductible
pour collecter, transformer et stocker des données publiques de transparence
énergétique de l’Union européenne.

```bash
eu-energy-data-pipeline/
│
├── ingestion/
│   └── README.md
│
├── processing/
│   └── README.md
│
├── database/
│   └── README.md
│
├── docker/
│   └── README.md
│
├── k8s/
│   └── README.md
│
├── .gitignore
├── README.md
```

## Objectifs
- Ingestion de données énergétiques via des APIs publiques officielles
- Traitement et normalisation des données avec Spark
- Stockage des données dans une base relationnelle
- Exécution des différentes étapes via des conteneurs Docker
- Orchestration simple avec Kubernetes

## Périmètre
Ce projet est volontairement limité à une architecture de niveau débutant
en ingénierie des données, afin de mettre en avant les fondamentaux :
qualité des données, structuration du pipeline et reproductibilité.

## Technologies utilisées
- Python
- PySpark
- PostgreSQL
- Docker
- Kubernetes
- SQL

## Sources de données
Les données utilisées proviennent de la plateforme officielle
ENTSO-E Transparency Platform (Union européenne).
