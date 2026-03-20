"""
Initialize FundOperations database with bronze, silver, gold schemas.
Creates database and schemas only if they don't already exist.
"""

from sqlalchemy import create_engine, text, inspect, event
from sqlalchemy.pool import StaticPool
from config import DB_CONN_STR_SQLSERVER


def initialize_database():
    """Create FundOperations database and schemas if they don't exist."""
    
    # Connect to master database with AUTOCOMMIT to allow CREATE DATABASE
    engine_master = create_engine(
        DB_CONN_STR_SQLSERVER.replace("FundOperations", "master"),
        isolation_level="AUTOCOMMIT"
    )
    
    with engine_master.connect() as conn:
        # Check if FundOperations database exists
        result = conn.execute(
            text("SELECT 1 FROM sys.databases WHERE name = 'FundOperations'")
        )
        
        if not result.fetchone():
            print("Creating FundOperations database...")
            conn.execute(text("CREATE DATABASE FundOperations"))
        else:
            print("Found FundOperations database.")
    
    engine_master.dispose()  # Close master connection
    
    # Connect to FundOperations database to create schemas
    engine = create_engine(DB_CONN_STR_SQLSERVER)
    inspector = inspect(engine)
    existing_schemas = inspector.get_schema_names()
    
    with engine.connect() as conn:
        # Note: CREATE SCHEMA doesn't need AUTOCOMMIT, but it's safer
        for schema in ["bronze", "silver", "gold"]:
            if schema not in existing_schemas:
                print(f"Creating schema: {schema}")
                conn.execute(text(f"CREATE SCHEMA {schema}"))
                conn.commit()
            else:
                print(f"Schema {schema} already exists.")
    
    print("Database initialization complete.")


if __name__ == "__main__":
    initialize_database()