import os
import time
from pathlib import Path

import pandas as pd
import psycopg2
from neo4j import GraphDatabase


PG_RETRY_SECONDS = 2
NEO4J_RETRY_SECONDS = 2


def wait_for_postgres():
    """Wait until PostgreSQL accepts connections."""
    while True:
        try:
            conn = psycopg2.connect(
                host=os.environ.get("POSTGRES_HOST", "postgres"),
                dbname=os.environ.get("POSTGRES_DB", "shop"),
                user=os.environ.get("POSTGRES_USER", "app"),
                password=os.environ.get("POSTGRES_PASSWORD", "app"),
            )
            conn.close()
            print("Postgres is ready.")
            break
        except Exception as e:
            print(f"Waiting for Postgres... ({e})")
            time.sleep(PG_RETRY_SECONDS)


def wait_for_neo4j():
    """Wait until Neo4j accepts Bolt connections."""
    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "password")

    while True:
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            with driver.session() as session:
                session.run("RETURN 1")
            driver.close()
            print("Neo4j is ready.")
            break
        except Exception as e:
            print(f"Waiting for Neo4j... ({e})")
            time.sleep(NEO4J_RETRY_SECONDS)


def run_cypher(session, query, parameters=None):
    """Run a single Cypher query."""
    session.run(query, parameters or {})


def run_cypher_file(session, path: Path):
    """Run all Cypher statements contained in a .cypher file."""
    text = path.read_text(encoding="utf-8")
    statements = [s.strip() for s in text.split(";") if s.strip()]
    for stmt in statements:
        session.run(stmt)


def chunk(df, size=1000):
    """Yield DataFrame chunks of at most `size` rows."""
    for start in range(0, len(df), size):
        yield df.iloc[start : start + size]


def etl():
    """
    Main ETL function that migrates data from PostgreSQL to Neo4j.

    This function performs the complete Extract, Transform, Load process:
    1. Waits for both databases to be ready
    2. Sets up Neo4j schema using queries.cypher file
    3. Extracts data from PostgreSQL tables
    4. Transforms relational data into graph format
    5. Loads data into Neo4j with appropriate relationships

    The process creates the following graph structure:
    - Category nodes with name properties
    - Product nodes linked to categories via IN_CATEGORY relationships
    - Customer nodes with name and join_date properties
    - Order nodes linked to customers via PLACED relationships
    - Order-Product relationships via CONTAINS with quantity properties
    - Dynamic event relationships between customers and products
    """
    # Ensure dependencies are ready (useful when running in docker-compose)
    wait_for_postgres()
    wait_for_neo4j()

    # Get path to your Cypher schema file
    queries_path = Path(__file__).with_name("queries.cypher")
    print(f"Using Cypher schema from: {queries_path}")

    # --- Connect to Postgres
    pg_conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "shop"),
        user=os.environ.get("POSTGRES_USER", "app"),
        password=os.environ.get("POSTGRES_PASSWORD", "app"),
    )

    # --- Connect to Neo4j
    driver = GraphDatabase.driver(
        os.environ.get("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(
            os.environ.get("NEO4J_USER", "neo4j"),
            os.environ.get("NEO4J_PASSWORD", "password"),
        ),
    )

    try:
        with driver.session() as session:
            # 1) Apply schema
            print("Applying Neo4j schema...")
            run_cypher_file(session, queries_path)

            # 2) Extract from Postgres
            print("Reading tables from Postgres...")
            customers = pd.read_sql("SELECT * FROM customers", pg_conn)
            categories = pd.read_sql("SELECT * FROM categories", pg_conn)
            products = pd.read_sql("SELECT * FROM products", pg_conn)
            orders = pd.read_sql("SELECT * FROM orders", pg_conn)
            order_items = pd.read_sql("SELECT * FROM order_items", pg_conn)
            events = pd.read_sql("SELECT * FROM events", pg_conn)

            # 3) Load categories
            print("Loading categories...")
            for df in chunk(categories, 100):
                session.run(
                    """
                    UNWIND $rows AS row
                    MERGE (c:Category {id: row.id})
                    SET c.name = row.name
                    """,
                    {"rows": df.to_dict("records")},
                )

            # 4) Load products + IN_CATEGORY
            print("Loading products...")
            for df in chunk(products, 100):
                session.run(
                    """
                    UNWIND $rows AS row
                    MERGE (p:Product {id: row.id})
                    SET p.name = row.name,
                        p.price = row.price
                    WITH p, row
                    MATCH (c:Category {id: row.category_id})
                    MERGE (p)-[:IN_CATEGORY]->(c)
                    """,
                    {"rows": df.to_dict("records")},
                )

            # 5) Load customers
            print("Loading customers...")
            for df in chunk(customers, 100):
                session.run(
                    """
                    UNWIND $rows AS row
                    MERGE (c:Customer {id: row.id})
                    SET c.name = row.name,
                        c.join_date = row.join_date
                    """,
                    {"rows": df.to_dict("records")},
                )

            # 6) Load orders + PLACED
            print("Loading orders...")
            for df in chunk(orders, 100):
                session.run(
                    """
                    UNWIND $rows AS row
                    MERGE (o:Order {id: row.id})
                    SET o.ts = row.ts
                    WITH o, row
                    MATCH (c:Customer {id: row.customer_id})
                    MERGE (c)-[:PLACED]->(o)
                    """,
                    {"rows": df.to_dict("records")},
                )

            # 7) Load order_items as CONTAINS
            print("Loading order items...")
            for df in chunk(order_items, 100):
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (o:Order {id: row.order_id})
                    MATCH (p:Product {id: row.product_id})
                    MERGE (o)-[r:CONTAINS]->(p)
                    SET r.quantity = row.quantity
                    """,
                    {"rows": df.to_dict("records")},
                )

            # 8) Load events as VIEW / CLICK / ADD_TO_CART relationships

            print("Loading VIEW events...")
            view_df = events[events["event_type"] == "view"]
            for df in chunk(view_df, 100):
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (c:Customer {id: row.customer_id})
                    MATCH (p:Product {id: row.product_id})
                    MERGE (c)-[r:VIEW]->(p)
                    SET r.ts = row.ts
                    """,
                    {"rows": df.to_dict("records")},
                )

            print("Loading CLICK events...")
            click_df = events[events["event_type"] == "click"]
            for df in chunk(click_df, 100):
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (c:Customer {id: row.customer_id})
                    MATCH (p:Product {id: row.product_id})
                    MERGE (c)-[r:CLICK]->(p)
                    SET r.ts = row.ts
                    """,
                    {"rows": df.to_dict("records")},
                )

            print("Loading ADD_TO_CART events...")
            atc_df = events[events["event_type"] == "add_to_cart"]
            for df in chunk(atc_df, 100):
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (c:Customer {id: row.customer_id})
                    MATCH (p:Product {id: row.product_id})
                    MERGE (c)-[r:ADD_TO_CART]->(p)
                    SET r.ts = row.ts
                    """,
                    {"rows": df.to_dict("records")},
                )

        print("ETL done.")
    finally:
        pg_conn.close()
        driver.close()


if __name__ == "__main__":
    etl()