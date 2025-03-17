import requests
import pandas as pd
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
import json
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()


class SellerAppCategoryCrawler:
    """
    Crawler for SellerApp categories to extract hierarchical category data
    """

    def __init__(self, geo: str = "us"):
        """
        Initialize crawler with authentication credentials

        Args:
            client_id: The client ID for SellerApp API
            token: Authentication token for SellerApp API
            geo: Geographic location (default: "us")
        """
        client_id = os.getenv("SELLERAPP_CLIENT_ID")
        token = os.getenv("SELLERAPP_TOKEN")
        self.geo = geo
        self.headers = {"client-id": client_id, "token": token}
        self.base_url = "https://api.sellerapp.com/sellmetricsv2/category_tree"
        self.all_categories = []

    def fetch_categories(self, category_id: str) -> List[Dict[str, Any]]:
        """
        Fetch categories for a given parent category ID

        Args:
            category_id: The parent category ID to fetch children for

        Returns:
            List of category dictionaries
        """
        params = {"key": category_id, "key_type": "id", "geo": self.geo}

        try:
            response = requests.get(self.base_url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching categories for ID {category_id}: {e}")
            return []

    def process_category_path(self, path: str) -> List[str]:
        """
        Convert a slash-delimited path string to an array of path components

        Args:
            path: Category path string (e.g., "/Health & Household/Household Supplies/Toothpicks")

        Returns:
            List of path components
        """
        # Remove the leading slash and split by slash
        components = path.strip("/").split("/")
        return components

    def crawl_categories(self, start_category_id: str):
        """
        Recursively crawl all categories starting from the given category ID

        Args:
            start_category_id: The category ID to start crawling from
        """
        categories = self.fetch_categories(start_category_id)

        for category in categories:
            # Process and store the category
            category_data = {
                "category_id": category["category_id"],
                "category": category["name"],
                "category_path": self.process_category_path(category["category_path"]),
            }
            self.all_categories.append(category_data)

            # Recursively crawl child categories if they exist
            if category["has_child"]:
                self.crawl_categories(category["category_id"])

    def get_categories_dataframe(self, start_category_id: str) -> pd.DataFrame:
        """
        Crawl all categories and return them as a DataFrame

        Args:
            start_category_id: The category ID to start crawling from

        Returns:
            DataFrame with all categories
        """
        self.all_categories = []  # Reset collected categories
        self.crawl_categories(start_category_id)
        return pd.DataFrame(self.all_categories)

    def ingest_categories(self, df: pd.DataFrame):
        """
        Ingest categories into the database

        Args:
            df: DataFrame containing category data to be ingested
        """
        # Get database connection parameters from environment variables
        db_host = os.getenv("PG_HOST")
        db_name = os.getenv("PG_DATABASE")
        db_user = os.getenv("PG_USER")
        db_password = os.getenv("PG_PASSWORD")

        try:
            # Connect to the database
            conn = psycopg2.connect(
                host=db_host, database=db_name, user=db_user, password=db_password
            )
            cursor = conn.cursor()

            # Prepare data for insertion
            data_to_insert = []
            for _, row in df.iterrows():
                # Convert category_path list to JSON string
                category_path_json = json.dumps(row["category_path"])

                # Generate Amazon URL based on category_id

                # Create tuple with all required fields
                data_to_insert.append(
                    (
                        row["category_id"],
                        self.geo,  # Using the geo attribute as market
                        row["category"],
                        category_path_json,
                        "",
                    )
                )

            # save to json file
            with open("sellerapp_categories.json", "w") as f:
                json.dump(data_to_insert, f)
            # return

            # SQL query for insertion
            query = """
                INSERT INTO amz.bestsellers (category_id, market, category, category_path, url)
                VALUES %s
                ON CONFLICT (category_id, market) DO UPDATE 
                SET category = EXCLUDED.category,
                    category_path = EXCLUDED.category_path,
                    updated_at = NOW()
            """

            # Execute batch insert
            execute_values(cursor, query, data_to_insert)
            conn.commit()

            print(
                f"Successfully ingested {len(data_to_insert)} categories into the database"
            )

        except Exception as e:
            print(f"Error ingesting categories into database: {e}")
            if "conn" in locals() and conn:
                conn.rollback()
        finally:
            if "cursor" in locals() and cursor:
                cursor.close()
            if "conn" in locals() and conn:
                conn.close()


if __name__ == "__main__":
    # Example usage

    START_CATEGORY_IDS = [
        "553958"
    ]
    geo = "us"

    crawler = SellerAppCategoryCrawler(geo)
    for start_category_id in START_CATEGORY_IDS:
        df = crawler.get_categories_dataframe(start_category_id)
        crawler.ingest_categories(df)

        # Display the results
        print(f"Total categories found: {len(df)}")
        # print(df.head())

    # Save to CSV (optional)
    # df.to_csv("sellerapp_categories.csv", index=False)
