import json
import os
from sqlalchemy import create_engine, text  
from dotenv import load_dotenv
load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@"
    f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
)
asin = "B09XHYQ2RQ"
category = "Women Tennis Dresses"

def aggregate_sales_volume(json_data):
    try:
        json_data = json_data.get("data")[0]
        sales_data = json_data.get("attributes", {}).get("data", [])

        total_sales_volume = sum(item["estimated_units_sold"] for item in sales_data)

        return total_sales_volume

    except Exception as e:
        print(f"Error processing sales data: {e}")
        return None

def count_daily_sales(json_data):
    try:
        json_data = json_data.get("data")[0]
        sales_data = json_data.get("attributes", {}).get("data", [])
        sales_data = [item for item in sales_data if item["estimated_units_sold"] > 0]

        return len(sales_data)

    except Exception as e:
        print(f"Error processing sales data: {e}")
        return None


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
            conn.commit()

        print(f"Ingested sales volume for {asin}")

    except Exception as e:
        print(f"Error ingesting sales volume: {str(e)}")
        raise

def ingest_average_daily_sales(asin, avg_daily_sales):
    print(f"Updating daily sales for {asin}: {avg_daily_sales}")
    try:
        query = text(
            """
            UPDATE raw.amazon_products
            SET est_avg_dly_units_sold = :avg_daily_sales
            WHERE asin = :asin
        """
        )

        with engine.connect() as conn:
            conn.execute(query, {"asin": asin, "avg_daily_sales": avg_daily_sales})
            conn.commit()

        print(f"Ingested daily sales for {asin}")

    except Exception as e:
        print(f"Error ingesting daily sales: {str(e)}")
        raise


def get_processed_asins(category):
    print(f"Retrieving ASINs for category '{category}'")
    try:
        query = text(
            """
            select ap.asin
            from raw.amazon_bestsellers ab 
            join raw.amazon_products ap 
                on ab.asin = ap.asin
            where ab.category = :category
            and ap.est_mly_units_sold is not null;
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



# Read json data from file
def read_asin(asin):
    try:
        with open(f"data/junglescout/{asin}.json") as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error reading JSON data for {asin}: {e}")
        return None


def analyse_asin(asin):
    try:
        print(f"Analysing ASIN: {asin}")
        with open(f"data/junglescout/{asin}.json") as f:
            data = json.load(f)
        json_data = data.get("data")[0]
        sales_data = json_data.get("attributes", {}).get("data", [])

        daily_count = count_daily_sales(data)
        print(f"Daily count: {daily_count}")
        sale_volume = aggregate_sales_volume(data)
        print(f"Sale volume: {sale_volume}")
        average_daily_sales = sale_volume / daily_count
        print(f"Average daily sales: {average_daily_sales}")
    except Exception as e:
        print(f"Error reading JSON data for {asin}: {e}")
        return None

# def get_proccessed_asins():
#     # Read all asins from /data/junglescout/{asin}.json
#     asins = []
#     for asin in os.listdir("data/junglescout"):
#         asins.append(asin.split(".")[0])
#     print(len(asins))
#     return asins


# asins = get_processed_asins(category)
# print(len(asins))
# for asin in asins:
#     data = read_asin(asin)
#     if data:
#         daily_count = count_daily_sales(data)
#         sale_volume = aggregate_sales_volume(data)
#         average_daily_sales = sale_volume / daily_count
#         ingest_average_daily_sales(asin, average_daily_sales)
#         # ingest_sales_volume(asin, sale_volume)


analyse_asin("B0CYLFKWF8")