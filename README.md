#  Projet ETL E-commerce Professionnel
### Pipeline de données complet avec Python & Pandas

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-ETL-green?logo=pandas&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-Database-lightblue?logo=sqlite&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen)

---

##  Description

Ce projet est un **pipeline ETL (Extract, Transform, Load)** complet réalisé en Python avec la bibliothèque `pandas`.  
Il démontre les bonnes pratiques de traitement de données à partir de fichiers CSV issus d'un schéma e-commerce inspiré du célèbre **dataset Olist (Brésil)**.

---

##  Objectifs pédagogiques

| Étape | Description |
|-------|-------------|
|  **Extract** | Lire et explorer des données réelles (CSV) |
|  **Transform** | Nettoyer, enrichir et transformer les données |
|  **Qualité** | Gérer les valeurs manquantes et les outliers |
|  **Jointures** | Réaliser des jointures robustes entre tables |
|  **Métriques** | Calculer revenus, délais, top catégories... |
|  **Load** | Exporter en CSV et dans une base SQLite |

---

## 🗂️ Structure du projet

```
Projet-ETL_E-commerce/
│
├── tp_etl.py                          # Script principal ETL
├── rapport_tp_etl.md                  # Rapport détaillé de la démarche
├── outputs/                           # Résultats générés (CSV + SQLite)
└── tp_etl/tp_etl/sqlite_exports/      # Fichiers sources CSV
    ├── customers.csv
    ├── sellers.csv
    ├── products.csv
    ├── orders.csv
    ├── order_items.csv
    ├── order_pymts.csv
    ├── order_reviews.csv
    ├── geoloc.csv
    └── translation.csv
```

---

##  Lancer le projet

### 1. Cloner le repository
```bash
git clone https://github.com/davidsonrigaud65-prog/Projet-ETL_E-commerce.git
cd Projet-ETL_E-commerce
```

### 2. Installer les dépendances
```bash
pip install -r tp_etl/tp_etl/requirements.txt
```

### 3. Exécuter le pipeline ETL
```bash
python tp_etl.py --data-dir tp_etl/tp_etl/sqlite_exports --out outputs --sqlite outputs/etl.db
```

### 4. Consulter les résultats
- 📁 Dossier `outputs/` → fichiers CSV générés
- 📄 `rapport_tp_etl.md` → analyse détaillée des résultats
- 🗄️ `outputs/etl.db` → base SQLite (généré localement)

---

##  Points forts

-  **Imputation intelligente** — médiane si outliers, moyenne sinon, mode pour les catégorielles
-  **Détection automatique des outliers** via méthode IQR
-  **Logging détaillé** pour la traçabilité complète du pipeline
-  **Code abondamment commenté** pour la lisibilité et la pédagogie

---

## 👤 Auteur

**Davidson Rigaud**  
📊 Statisticien | Data Analyst en formation  
🔗 [GitHub](https://github.com/davidsonrigaud65-prog)

---

>  *Projet réalisé dans le cadre d'une formation Data Science — Phase 3 : Analyse de données, OLAP & Power BI*
