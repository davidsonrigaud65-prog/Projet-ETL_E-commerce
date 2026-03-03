# Importation des bibliotheques necessaires
import pandas as pd  # Manipulation de données
import os           # Gestion des fichiers et dossiers
import sys          # Accès aux fonctions système
import argparse     # Gestion des arguments en ligne de commande
import sqlite3      # Export SQLite
from typing import Dict, Tuple



def safe_read_csv(path: str) -> pd.DataFrame:
    """
    Lecture robuste d'un CSV avec fallback d'encodage.
    Permet d'éviter les erreurs d'encodage lors de la lecture.
    """
    try:
        return pd.read_csv(path, low_memory=False, encoding='utf-8')
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding='latin-1')


def log(msg: str, level: str = "INFO"):
    # Fonction utilitaire pour afficher des logs formatés
    prefix = f"[{level}]"
    print(f"{prefix:10} {msg}")


def explorer_donnees(dfs: Dict[str, pd.DataFrame]) -> str:
    """
    Exploration détaillée des données :
    - Affiche dimensions, types, valeurs manquantes
    - Statistiques descriptives (describe)
    - Info sur les colonnes (info)
    - Stratégies de gestion des valeurs manquantes
    - Rapport commenté
    """
    report = []
    report.append("\n" + "=" * 70)
    report.append("T2: RAPPORT - EXPLORATION DES DONNEES")
    report.append("=" * 70)

    for nom, df in sorted(dfs.items()):
        if df is None or df.empty:
            continue
        report.append(f"\n{nom.upper()} :")
        report.append(f"  Dimensions : {df.shape[0]:8d} lignes x {df.shape[1]:3d} colonnes")
        # Types de colonnes
        types_summary = df.dtypes.value_counts().to_dict()
        report.append(f"  Types de colonnes : {types_summary}")
        # Valeurs manquantes
        null_cols = df.isnull().sum()
        null_cols = null_cols[null_cols > 0]
        if len(null_cols) > 0:
            report.append(f"  Colonnes avec NaN : {dict(null_cols)}")
            # Pour chaque colonne avec NaN,on précise la stratégie d'imputation selon le type
            for col in null_cols.index:
                n_nan = null_cols[col]
                col_type = df[col].dtype
                total = len(df)
                pct = n_nan / total * 100 if total > 0 else 0
                strat = ""
                # Détection d'outliers pour les numériques
                if pd.api.types.is_numeric_dtype(col_type):
                    col_nonan = df[col].dropna()
                    q1 = col_nonan.quantile(0.25)
                    q3 = col_nonan.quantile(0.75)
                    iqr = q3 - q1
                    outliers = ((col_nonan < (q1 - 1.5 * iqr)) | (col_nonan > (q3 + 1.5 * iqr))).sum()
                    outlier_pct = outliers / len(col_nonan) * 100 if len(col_nonan) > 0 else 0
                    report.append(f"    > Outliers détectés pour {col}: {outliers} ({outlier_pct:.1f}%)")
                    if outlier_pct > 5:
                        strat = "Remplissage par la médiane (robuste aux outliers)"
                    else:
                        strat = "Remplissage par la moyenne "
                elif pd.api.types.is_categorical_dtype(col_type) or pd.api.types.is_object_dtype(col_type):
                    strat = "Remplissage par la valeur la plus fréquente (mode)"
                else:
                    strat = "Remplissage spécifique selon le contexte"
                report.append(f"    - {col} ({col_type}) : {n_nan} NaN ({pct:.1f}%) -> STRATEGIE : {strat}")
        else:
            report.append("  Colonnes avec NaN : aucune")
        # Statistiques descriptives
        report.append("  Statistiques descriptives :")
        try:
            desc = df.describe(include='all').transpose()
            report.append(desc.to_string())
        except Exception as e:
            report.append(f"    Erreur describe : {e}")
        # Info sur les colonnes
        report.append("  Info colonnes :")
        try:
            import io
            buf = io.StringIO()
            df.info(buf)
            info_str = buf.getvalue()
            report.append(info_str)
        except Exception as e:
            report.append(f"    Erreur info : {e}")
        # Commentaire pédagogique
        report.append("  COMMENTAIRE : Vérifiez les types, les NaN et la distribution des valeurs avant toute transformation.")

    report_text = "\n".join(report)
    print(report_text)
    return report_text


def extract_sources(data_dir: str) -> Dict[str, pd.DataFrame]:
    """
    Extraction : charge tous les CSV depuis data_dir.
    Chaque fichier est lu dans un DataFrame.
    """
    log("Etape 1: EXTRACTION", "START")
    fichiers = [
        'customers', 'sellers', 'products', 'orders', 'order_items',
        'order_pymts', 'order_reviews', 'geoloc', 'translation'
    ]
    dfs: Dict[str, pd.DataFrame] = {}
    for f in fichiers:
        chemin = os.path.join(data_dir, f"{f}.csv")
        if os.path.exists(chemin):
            try:
                dfs[f] = safe_read_csv(chemin)
                log(f"LOAD {f:15} -> {len(dfs[f]):8d} rows x {dfs[f].shape[1]:2d} cols", "OK")
            except Exception as exc:
                log(f"ERROR reading {f}: {exc}", "ERR")
        else:
            log(f"FILE NOT FOUND: {chemin}", "WARN")
    log(f"Extraction: {len(dfs)} files loaded", "OK")
    return dfs


def transform_data(dfs: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, pd.DataFrame], str]:
    """
    Transformation : nettoyage, enrichissement, création des métriques.
    - Conversion des dates
    - Enrichissement géographique
    - Gestion des valeurs manquantes :
        * Dates : conversion avec errors='coerce' (NaT si valeur manquante)
        * Prix/freight : conversion avec fillna(0)
        * Colonnes critiques : dropna ou remplissage selon le cas
    - Construction de la table de faits et des métriques
    """
    log("Etape 2: TRANSFORMATION", "START")
    required = ['orders', 'order_items', 'products', 'customers', 'sellers']
    missing = [k for k in required if k not in dfs]
    if missing:
        raise KeyError(f"DataFrames manquants: {missing}")
    # Copies pour éviter de modifier les originaux
    orders = dfs['orders'].copy()
    items = dfs['order_items'].copy()
    customers = dfs['customers'].copy()
    sellers = dfs['sellers'].copy()
    products = dfs['products'].copy()
    reviews = dfs.get('order_reviews', pd.DataFrame()).copy()
    translations = dfs.get('translation', pd.DataFrame()).copy()
    # --- Conversion des dates ---
    for df in [orders, items, reviews]:
        if not df.empty:
            for col in df.columns:
                if 'date' in col.lower() or 'timestamp' in col.lower():
                    df[col] = pd.to_datetime(df[col], errors='coerce')  # NaT si valeur manquante
    log("Date columns parsed", "OK")
    # --- Enrichissement géographique (bonus) ---
    if 'geoloc' in dfs and not dfs['geoloc'].empty:
        geoloc = dfs['geoloc'].copy()
        if 'customer_zip_code_prefix' in customers.columns and 'geolocation_zip_code_prefix' in geoloc.columns:
            customers = customers.merge(geoloc,
                left_on='customer_zip_code_prefix',
                right_on='geolocation_zip_code_prefix',
                how='left')
            log("Customers enriched with geoloc", "OK")
    # --- Construction de la table de faits ---
    if 'order_id' not in items.columns or 'order_id' not in orders.columns:
        raise KeyError("order_id manquant")
    fact = items.merge(orders, on='order_id', how='left', suffixes=('_item', '_order'))
    # Ajout des infos produits
    if 'product_id' in fact.columns and 'product_id' in products.columns:
        prod_cols = ['product_id', 'product_category_name']
        prod_subset = products[[c for c in prod_cols if c in products.columns]]
        fact = fact.merge(prod_subset, on='product_id', how='left')
    # Traduction des catégories (bonus)
    if 'product_category_name' in fact.columns and not translations.empty:
        if 'product_category_name' in translations.columns:
            trans_cols = ['product_category_name', 'product_category_name_english']
            trans_subset = translations[[c for c in trans_cols if c in translations.columns]].drop_duplicates()
            fact = fact.merge(trans_subset, on='product_category_name', how='left')
            fact['category'] = fact['product_category_name_english'].fillna(fact['product_category_name'])
        else:
            fact['category'] = fact.get('product_category_name', '')
    else:
        fact['category'] = 'UNKNOWN'
    # --- Calcul du revenu ---
    # Gestion des valeurs manquantes : fillna(0) pour les montants
    fact['price'] = pd.to_numeric(fact.get('price', 0), errors='coerce').fillna(0)
    fact['freight_value'] = pd.to_numeric(fact.get('freight_value', 0), errors='coerce').fillna(0)
    fact['total_price'] = fact['price'] + fact['freight_value']
    log(f"Fact table built: {len(fact)} rows", "OK")
    # --- T3: Revenu mensuel ---
    monthly_revenue = pd.DataFrame(columns=['year_month', 'revenue'])
    if 'order_purchase_timestamp' in fact.columns:
        fact_clean = fact.dropna(subset=['order_purchase_timestamp'])
        if not fact_clean.empty:
            fact_clean['year_month'] = fact_clean['order_purchase_timestamp'].dt.to_period('M').astype(str)
            monthly_revenue = fact_clean.groupby('year_month', as_index=False)['total_price'].sum()
            monthly_revenue.columns = ['year_month', 'revenue']
    log(f"T3: monthly_revenue -> {len(monthly_revenue)} rows", "OK")
    # --- T4: Top 10 catégories ---
    top_categories = pd.DataFrame(columns=['product_category', 'revenue'])
    if 'category' in fact.columns:
        top_categories = fact.groupby('category', as_index=False)['total_price'].sum()
        top_categories.columns = ['product_category', 'revenue']
        top_categories = top_categories.sort_values('revenue', ascending=False).head(10)
    log(f"T4: top_categories -> {len(top_categories)} rows", "OK")
    # --- T5: Métriques de livraison ---
    delivery_metrics = pd.DataFrame(columns=['year_month', 'avg_delivery_days'])
    if 'order_delivered_customer_date' in orders.columns and 'order_purchase_timestamp' in orders.columns:
        del_df = orders.dropna(subset=['order_delivered_customer_date']).copy()
        if not del_df.empty:
            del_df['order_delivered_customer_date'] = pd.to_datetime(del_df['order_delivered_customer_date'], errors='coerce')
            del_df['order_purchase_timestamp'] = pd.to_datetime(del_df['order_purchase_timestamp'], errors='coerce')
            del_df['delivery_days'] = (del_df['order_delivered_customer_date'] - del_df['order_purchase_timestamp']).dt.days
            del_df = del_df[del_df['delivery_days'] >= 0]
            if not del_df.empty:
                del_df['year_month'] = del_df['order_purchase_timestamp'].dt.to_period('M').astype(str)
                delivery_metrics = del_df.groupby('year_month', as_index=False)['delivery_days'].mean()
                delivery_metrics.columns = ['year_month', 'avg_delivery_days']
    log(f"T5: delivery_metrics -> {len(delivery_metrics)} rows", "OK")
    # --- Bonus: Reviews mensuels ---
    reviews_monthly = pd.DataFrame(columns=['year_month', 'avg_review_score'])
    if 'review_score' in reviews.columns and 'review_creation_date' in reviews.columns:
        rev_clean = reviews.dropna(subset=['review_score', 'review_creation_date'])
        if not rev_clean.empty:
            rev_clean['review_creation_date'] = pd.to_datetime(rev_clean['review_creation_date'], errors='coerce')
            rev_clean['year_month'] = rev_clean['review_creation_date'].dt.to_period('M').astype(str)
            reviews_monthly = rev_clean.groupby('year_month', as_index=False)['review_score'].mean()
            reviews_monthly.columns = ['year_month', 'avg_review_score']
    log(f"BONUS: reviews_monthly -> {len(reviews_monthly)} rows", "OK")
    log("Transform complete", "OK")
    report = f"\nTransform Summary:\n  fact_order_items: {len(fact)} rows\n  monthly_revenue: {len(monthly_revenue)} rows\n  top_categories: {len(top_categories)} rows\n  delivery_metrics: {len(delivery_metrics)} rows\n  reviews_monthly: {len(reviews_monthly)} rows"
    return {
        'dim_customers': customers,
        'dim_sellers': sellers,
        'dim_products': products,
        'fact_order_items': fact,
        'monthly_revenue': monthly_revenue,
        'top_categories': top_categories,
        'delivery_metrics': delivery_metrics,
        'reviews_monthly': reviews_monthly
    }, report


def load_outputs(results: Dict[str, pd.DataFrame], output_dir: str, sqlite_db: str = None):
    """T6: Load - exporte en CSV et SQLite."""
    log("Etape 3: LOAD", "START")

    # Create output dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        log(f"Created directory: {output_dir}", "OK")

    # Petites tables pour CSV (legeres, < 50K lignes)
    csv_tables = ['monthly_revenue', 'top_categories', 'delivery_metrics', 'reviews_monthly']

    # --- Export CSV: lightweight tables only ---
    for table_name in csv_tables:
        if table_name in results:
            df = results[table_name]
            if isinstance(df, pd.DataFrame) and not df.empty:
                csv_path = os.path.join(output_dir, f"{table_name}.csv")
                try:
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    log(f"CSV: {table_name} -> {len(df)} rows", "OK")
                except Exception as e:
                    log(f"CSV ERROR {table_name}: {e}", "ERR")

    # --- Export SQLite (T6): ALL tables (optimal for large ones) ---
    if sqlite_db:
        try:
            os.makedirs(os.path.dirname(sqlite_db) or '.', exist_ok=True)
            conn = sqlite3.connect(sqlite_db)
            
            for table_name, df in results.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Sanitize table name
                    table_name_sql = table_name.lower().replace('-', '_')
                    df.to_sql(table_name_sql, conn, if_exists='replace', index=False)
                    log(f"SQL: {table_name_sql} -> {len(df)} rows", "OK")
            
            conn.close()
            log(f"Database saved: {sqlite_db}", "OK")
        except Exception as e:
            log(f"SQLite error: {e}", "ERR")

    log("Load complete", "OK")


def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline")
    parser.add_argument('--data-dir', default='tp_etl/tp_etl/sqlite_exports', help='Input data directory')
    parser.add_argument('--out', default='outputs', help='Output directory')
    parser.add_argument('--sqlite', default=None, help='SQLite database path (optional)')
    args = parser.parse_args()

    try:
        # Validate data dir
        if not os.path.exists(args.data_dir):
            log(f"Data dir not found: {args.data_dir}", "ERR")
            sys.exit(1)

        # Extract
        dfs = extract_sources(args.data_dir)
        if not dfs:
            log("No files extracted", "ERR")
            sys.exit(1)

        # Explore & Report (T2)
        report_t2 = explorer_donnees(dfs)

        # Transform
        results, report_t_transform = transform_data(dfs)
        print(report_t_transform)

        # Load
        load_outputs(results, args.out, args.sqlite)

        log("PIPELINE SUCCESS", "OK")

    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERR")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
