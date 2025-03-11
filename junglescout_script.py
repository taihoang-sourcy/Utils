import os
import json
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from junglescout import ClientSync
from junglescout.models.parameters import Marketplace, ApiType, FilterOptions, Sort
from dotenv import load_dotenv

load_dotenv()


category = "Women Tennis Dresses"
record_date = "2025-02-06"
start_date = datetime.strptime(record_date, "%Y-%m-%d") - timedelta(days=30)
start_date = start_date.strftime("%Y-%m-%d")

js_client = ClientSync(
    api_key_name=os.getenv("JUNGLESCOUT_API_KEY_NAME"),
    api_key=os.getenv("JUNGLESCOUT_API_KEY"),
    marketplace=Marketplace.US,
    api_type=ApiType.JS,
)
engine = create_engine(
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@"
    f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
)


def get_asins(category):
    print(f"Retrieving ASINs for category '{category}'")
    try:
        query = text(
            """
            SELECT ab.asin
            FROM raw.amazon_bestsellers ab
            JOIN raw.amazon_products ap ON ab.asin = ap.asin
            WHERE ab.category = :category
        """
        )

        # Create SQLAlchemy engine

        with engine.connect() as conn:
            asins = conn.execute(query, {"category": category}).scalars().all()

        print(f"Retrieved {len(asins)} ASINs for category '{category}'")
        return asins

    except Exception as e:
        print(f"Error querying Postgres: {str(e)}")
        raise


def get_proccessed_asins():
    # Read all asins from /data/junglescout/{asin}.json
    asins = []
    for asin in os.listdir("data/junglescout"):
        asins.append(asin.split(".")[0])
    return asins


def ingest_sales_volume(asin, sale_volume):
    print(f"Updating sales volume for {asin}: {sale_volume}")
    try:
        query = text(
            """
            UPDATE raw.amazon_products
            SET est_mly_units_sold = :sales_volume
            WHERE asin = :asin
        """
        )

        with engine.connect() as conn:
            conn.execute(query, {"asin": asin, "sales_volume": sale_volume})

        print(f"Ingested sales volume for {asin}")

    except Exception as e:
        print(f"Error ingesting sales volume: {str(e)}")
        raise


def aggregate_sales_volume(json_data):
    try:
        json_data = json_data.get("data")[0]
        sales_data = json_data.get("attributes", {}).get("data", [])

        total_sales_volume = sum(item["estimated_units_sold"] for item in sales_data)

        return total_sales_volume

    except Exception as e:
        print(f"Error processing sales data: {e}")
        return None


def fetch_and_store_sales_data(asin, start_date, end_date):
    print(f"Retrieving product data for {asin} from {start_date} to {end_date}")
    try:
        response = js_client.sales_estimates(
            asin, start_date, end_date, sort_option=None
        )
        data = response.model_dump()
        # Store JSON response to data/junglescout folder, named by ASIN
        output_file = f"data/junglescout/{asin}.json"
        with open(output_file, "w") as f:
            f.write(json.dumps(data))

        print(f"Aggregating sales volume for {asin}")
        sale_volume = aggregate_sales_volume(data)
        ingest_sales_volume(asin, sale_volume)

    except Exception as e:
        print(f"Error retrieving products: {str(e)}")


asins = get_asins(category)
proccessed_asins = get_proccessed_asins()

for asin in asins:
    if asin in proccessed_asins:
        continue
    fetch_and_store_sales_data(asin, start_date, record_date)
