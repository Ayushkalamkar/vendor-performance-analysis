import psycopg2
import pandas as pd
import logging
from sqlalchemy import create_engine

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ----------------------------
# PostgreSQL engine (update credentials)
# ----------------------------
engine = create_engine(
    "postgresql+psycopg2://postgres:ashitosh@localhost:5432/Vendor_performance"
)

# ----------------------------
# Function: Create Vendor Summary
# ----------------------------
def create_vendor_summary(engine):
    return pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT
            "VendorNumber",
            SUM("Freight") AS "FreightCost"
        FROM "vendor_invoice"
        GROUP BY "VendorNumber"
    ),

    PurchaseSummary AS (
        SELECT
            p."VendorNumber",
            p."VendorName",
            p."Brand",
            p."Description",
            p."PurchasePrice",
            pp."Price" AS "ActualPrice",
            pp."Volume",
            SUM(p."Quantity") AS "TotalPurchaseQuantity",
            SUM(p."Dollars") AS "TotalPurchaseDollars"
        FROM "purchases" p
        JOIN "purchase_prices" pp
            ON p."Brand" = pp."Brand"
        WHERE p."PurchasePrice" > 0
        GROUP BY
            p."VendorNumber",
            p."VendorName",
            p."Brand",
            p."Description",
            p."PurchasePrice",
            pp."Price",
            pp."Volume"
    ),

    SalesSummary AS (
        SELECT
            "VendorNo",
            "Brand",
            SUM("SalesQuantity") AS "TotalSalesQuantity",
            SUM("SalesDollars") AS "TotalSalesDollars",
            SUM("SalesPrice") AS "TotalSalesPrice",
            SUM("ExciseTax") AS "TotalExciseTax"
        FROM "sales"
        GROUP BY "VendorNo", "Brand"
    )

    SELECT
        ps."VendorNumber",
        ps."VendorName",
        ps."Brand",
        ps."Description",
        ps."PurchasePrice",
        ps."ActualPrice",
        ps."Volume",
        ps."TotalPurchaseQuantity",
        ps."TotalPurchaseDollars",
        COALESCE(ss."TotalSalesQuantity", 0) AS "TotalSalesQuantity",
        COALESCE(ss."TotalSalesDollars", 0) AS "TotalSalesDollars",
        COALESCE(ss."TotalSalesPrice", 0) AS "TotalSalesPrice",
        COALESCE(ss."TotalExciseTax", 0) AS "TotalExciseTax",
        COALESCE(fs."FreightCost", 0) AS "FreightCost"
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps."VendorNumber" = ss."VendorNo"
        AND ps."Brand" = ss."Brand"
    LEFT JOIN FreightSummary fs
        ON ps."VendorNumber" = fs."VendorNumber"
    ORDER BY ps."TotalPurchaseDollars" DESC
    """, engine)

# ----------------------------
# Function: Clean Data
# ----------------------------
def clean_data(df):
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    df['TotalPurchaseQuantity'] = pd.to_numeric(df['TotalPurchaseQuantity'], errors='coerce').fillna(0)
    df['TotalPurchaseDollars'] = pd.to_numeric(df['TotalPurchaseDollars'], errors='coerce').fillna(0)
    df['TotalSalesQuantity'] = pd.to_numeric(df['TotalSalesQuantity'], errors='coerce').fillna(0)
    df['TotalSalesDollars'] = pd.to_numeric(df['TotalSalesDollars'], errors='coerce').fillna(0)
    df['TotalSalesPrice'] = pd.to_numeric(df['TotalSalesPrice'], errors='coerce').fillna(0)
    df['TotalExciseTax'] = pd.to_numeric(df['TotalExciseTax'], errors='coerce').fillna(0)
    df['FreightCost'] = pd.to_numeric(df['FreightCost'], errors='coerce').fillna(0)

    df['VendorName'] = df['VendorName'].astype(str).str.strip()
    df['Description'] = df['Description'].astype(str).str.strip()

    # Same calculations as your original code
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = df.apply(
        lambda row: (row['GrossProfit'] / row['TotalSalesDollars'] * 100) if row['TotalSalesDollars'] != 0 else 0,
        axis=1
    )
    df['StockTurnover'] = df.apply(
        lambda row: (row['TotalSalesQuantity'] / row['TotalPurchaseQuantity']) if row['TotalPurchaseQuantity'] != 0 else 0,
        axis=1
    )
    df['SalesToPurchaseRatio'] = df.apply(
        lambda row: (row['TotalSalesDollars'] / row['TotalPurchaseDollars']) if row['TotalPurchaseDollars'] != 0 else 0,
        axis=1
    )

    return df

# ----------------------------
# Function: Ingest Data
# ----------------------------
def ingest_db(df, table_name, engine):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data successfully ingested into table '{table_name}'")
    except Exception as e:
        logging.error(f"Error ingesting data into DB: {e}")
        raise

# ----------------------------
# Main
# ----------------------------
if __name__ == '__main__':
    logging.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summary(engine)
    logging.info(summary_df.head())

    logging.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data...')
    ingest_db(clean_df, 'vendor_sales_summary', engine)
    logging.info('Completed')