# Projet -ETL E-commerce Professionnel avec Python & Pandas

_Auteur : Davidson Rigaud_

## Description du projet
Ce projet est un pipeline ETL (Extract, Transform, Load) complet réalisé en Python avec la bibliothèque pandas. Il a pour objectif de pratiquer et de démontrer les bases de l’ETL à partir de fichiers CSV issus d’un schéma e-commerce (inspiré du dataset Olist).

## Objectifs pédagogiques
- Lire et explorer des données réelles (CSV)
- Nettoyer, enrichir et transformer les données
- Gérer les valeurs manquantes et les outliers
- Réaliser des jointures robustes
- Calculer des métriques métiers (revenu, délais, top catégories...)
- Exporter les résultats en CSV et dans une base SQLite

## Structure du projet
- `tp_etl.py` : script principal ETL (extraction, exploration, transformation, chargement)
- `rapport_tp_etl.md` : rapport détaillé sur la démarche, les choix et les résultats
- `outputs/` : dossier de sortie pour les fichiers CSV et la base SQLite générés
- `tp_etl/tp_etl/sqlite_exports/` : dossier contenant les fichiers sources CSV

## Données utilisées
- `customers.csv`, `sellers.csv`, `products.csv`, `orders.csv`, `order_items.csv`, `order_pymts.csv`, `order_reviews.csv`, `geoloc.csv`, `translation.csv`

## Lancer le projet
1. Installer les dépendances :
   ```bash
   pip install -r tp_etl/tp_etl/requirements.txt
   ```
2. Exécuter le script ETL :
   ```bash
   python tp_etl.py --data-dir tp_etl/tp_etl/sqlite_exports --out outputs --sqlite outputs/etl.db
   ```
3. Consulter les résultats dans le dossier `outputs/` et le rapport `rapport_tp_etl.md`.

## Points forts
- Stratégies d’imputation adaptées à chaque variable (médiane si outliers, moyenne sinon, mode pour les catégorielles)
- Détection automatique des outliers
- Logging détaillé pour la traçabilité
- Code abondamment commenté pour la pédagogie

## Auteur
Davidson Rigaud

## Licence
Projet pédagogique, libre d’utilisation à des fins d’apprentissage.
