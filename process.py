import duckdb
import argparse
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from neo4j import GraphDatabase
from dotenv import load_dotenv
load_dotenv()

def setup_folders():
    sources = ["amazon", "tiktok", "shopee", "supply"]
    for source in sources:
        Path(f"data/{source}").mkdir(parents=True, exist_ok=True)


def get_postgres_data(source):
    """Query Postgres database and save results to CSV."""
    try:
        # PostgreSQL connection parameters from environment variables
        pg_params = {
            "host": os.getenv("PG_HOST", "localhost"),
            "database": (
                os.getenv("PG_DATABASE")
                if source != "supply"
                else os.getenv("PG_DATABASE_APP")
            ),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "port": os.getenv("PG_PORT", "5432"),
        }

        # Source-specific queries
        queries = {
            # "amazon": """
            #     SELECT distinct asin as id
            #     FROM amz.products
            #     ORDER BY id;
            # """,
            "amazon": """
                SELECT distinct p.asin as id
                FROM amz.products p
                JOIN amz.bestsellers_products bp
                    ON p.asin = bp.product_asin
                -- where bp.bestseller_id = 3096
                ORDER BY id;
            """,
            # "tiktok": "",
            "shopee": """
                SELECT distinct item_id as id
                FROM tmapi_shopee.normalized_shopee_product_details
                ORDER BY id;
            """,
            "supply": """
                SELECT distinct product_id as id
                FROM public.products
                ORDER BY id;
            """,
        }

        # Create SQLAlchemy engine
        engine = create_engine(
            f"postgresql://{pg_params['user']}:{pg_params['password']}@"
            f"{pg_params['host']}:{pg_params['port']}/{pg_params['database']}"
        )

        # Execute query and save to CSV
        query = queries[source]
        df = pd.read_sql_query(query, engine)
        output_file = f"data/{source}/full.csv"
        df.to_csv(output_file, index=False)
        print(f"Exported {len(df)} records from Postgres to {output_file}")

        return output_file

    except Exception as e:
        print(f"Error querying Postgres: {str(e)}")
        raise


def get_neo4j_data(source):
    """Query Neo4j database and save results to CSV."""
    try:
        # Neo4j connection parameters from environment variables
        neo4j_uri = os.getenv("NEO4J_URI")
        neo4j_user = os.getenv("NEO4J_USER")
        neo4j_password = os.getenv("NEO4J_PASSWORD")

        # Source-specific queries
        queries = {
            "amazon": "MATCH (n:Bestseller) RETURN n.asin as id order by id",
            # "tiktok": "",
            "shopee": "MATCH (n:ShopeeProduct) RETURN n.item_id as id order by id",
            "supply": "MATCH (n:Product) RETURN n.product_id as id order by id",
        }

        # Connect to Neo4j
        with GraphDatabase.driver(
            neo4j_uri, auth=(neo4j_user, neo4j_password)
        ) as driver:
            with driver.session() as session:
                result = session.run(queries[source])
                data = [record["id"] for record in result]

                # Create DataFrame and save to CSV
                df = pd.DataFrame(data, columns=["id"])
                output_file = f"data/{source}/loaded.csv"
                df.to_csv(output_file, index=False)
                print(f"Exported {len(df)} records from Neo4j to {output_file}")

                return output_file

    except Exception as e:
        print(f"Error querying Neo4j: {str(e)}")
        raise


def process_data(source):
    """Process data for a specific source."""
    # Define file paths
    data_folder = f"data/{source}"
    # full = os.path.join(data_folder, "full.csv")
    full = get_postgres_data(source)
    # loaded = os.path.join(data_folder, "loaded.csv")
    loaded = get_neo4j_data(source)
    unprocessed = os.path.join(data_folder, "unprocessed.csv")

    # Validate input files exist
    if not os.path.exists(full):
        raise FileNotFoundError(f"Full products file not found for {source}: {full}")
    if not os.path.exists(loaded):
        raise FileNotFoundError(
            f"Loaded products file not found for {source}: {loaded}"
        )

    # Open a DuckDB connection
    con = duckdb.connect()

    # Query to find unprocessed items
    query = f"""
    SELECT o.id as id
    FROM read_csv_auto('{full}') as o
    LEFT JOIN read_csv_auto('{loaded}') as l
    ON o.id = l.id
    WHERE l.id is null
    -- ORDER BY RANDOM()
    """

    try:
        # Execute the query and save results
        result = con.execute(query).fetchdf()
        result.to_csv(unprocessed, index=False)
        print(f"Processed {source} data:")
        print(f"Found {len(result)} unprocessed items")
        print(f"Results saved to: {unprocessed}")
    except Exception as e:
        print(f"Error processing {source} data: {str(e)}")
    finally:
        con.close()


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Process different data sources")
    parser.add_argument(
        "source",
        choices=["amazon", "tiktok", "shopee", "supply", "all"],
        help="Data source to process",
    )

    # Parse arguments
    args = parser.parse_args()

    # Process selected source or all sources
    if args.source == "all":
        sources = ["amazon", "tiktok", "shopee", "supply"]
        for source in sources:
            print(f"\nProcessing {source}...")
            try:
                process_data(source)
            except Exception as e:
                print(f"Error with {source}: {str(e)}")
    else:
        process_data(args.source)


if __name__ == "__main__":
    main()
