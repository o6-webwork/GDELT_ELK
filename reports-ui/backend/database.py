import os
import datetime
import pytz # For timezone-aware datetimes
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text, ForeignKey, Float
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func # For server-side default timestamps if needed

# Get DATABASE_URL from environment variable set by Docker Compose
# Provide a default for local development if .env is not loaded by the runner
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://gdeltuser:yourSUPERsecurePassword123!@localhost:5432/gdelt_monitoring")
# IMPORTANT: Replace the default connection string above if you run this script locally
# and your .env file isn't automatically picked up by that execution context.
# For execution INSIDE a Docker container that gets DATABASE_URL from docker-compose, os.getenv is perfect.

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- SQLAlchemy Models ---

class MonitoredTask(Base):
    __tablename__ = "monitored_tasks"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    query_string = Column(String, index=True, nullable=False)
    interval_minutes = Column(Integer, default=15, nullable=False)
    # Store datetimes in UTC in the database for consistency
    last_checked_at = Column(DateTime(timezone=True), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(pytz.utc), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    user_identifier = Column(String, index=True, nullable=True)

    # --- Columns for User-Selectable Alert Parameters ---
    # These will store the user's override. If NULL, the system uses defaults from PARAM_SETS.

    # General parameters
    custom_baseline_window_pd_str = Column(String, nullable=True, comment="Pandas offset string e.g., '7d', '24h', '1d'. Overrides PARAM_SETS.BASELINE_WINDOW_FOR_ROLLING.")
    custom_min_periods_baseline = Column(Integer, nullable=True, comment="Overrides PARAM_SETS.MIN_PERIODS_BASELINE.")
    custom_min_count_for_alert = Column(Integer, nullable=True, comment="Overrides PARAM_SETS.MIN_COUNT_FOR_ALERT.")
    
    # Spike Detection specific (Z-score)
    custom_spike_threshold = Column(Float, nullable=True, comment="Overrides PARAM_SETS.SPIKE_THRESHOLD.")
    
    # Build Detection specific (replaces separate short/extended builds)
    custom_build_window_periods_count = Column(Integer, nullable=True, comment="Number of task's own intervals. Overrides PARAM_SETS.BUILD_WINDOW_PERIODS_COUNT.")
    custom_build_threshold = Column(Float, nullable=True, comment="Overrides PARAM_SETS.BUILD_THRESHOLD.")

    def __repr__(self):
        return f"<MonitoredTask(id={self.id}, query='{self.query_string[:30]}...', active={self.is_active})>"

class AlertHistory(Base):
    __tablename__ = "alert_history"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    task_id = Column(Integer, ForeignKey("monitored_tasks.id"), nullable=False, index=True)
    # Store datetimes in UTC
    alert_timestamp = Column(DateTime(timezone=True), nullable=False) # Timestamp from the alert_info (event time)
    recorded_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(pytz.utc), nullable=False) # When this record was created
    
    alert_type = Column(String, nullable=True)
    reason = Column(Text, nullable=True)
    
    # Store key metrics related to the alert
    count = Column(Integer, nullable=True)
    baseline_mean = Column(Float, nullable=True)
    baseline_std = Column(Float, nullable=True)
    z_score = Column(Float, nullable=True)
    build_ratio = Column(Float, nullable=True)
    extended_build_ratio = Column(Float, nullable=True)
    current_interval_used = Column(String, nullable=True)


    # For storing the timeseries_data that led to the alert, JSONB is good in PostgreSQL
    # For SQLite or other DBs, you might use Text and store JSON as a string.
    # This can get large, so consider if you really need to store it for every alert.
    # from sqlalchemy.dialects.postgresql import JSONB
    # timeseries_snapshot = Column(JSONB, nullable=True) # If using PostgreSQL JSONB type

    is_acknowledged = Column(Boolean, default=False, nullable=False)

    def __repr__(self):
        return f"<AlertHistory(id={self.id}, task_id={self.task_id}, type='{self.alert_type}')>"

# Function to create tables in the database
def create_db_tables():
    # This is okay for development. For production, use Alembic migrations.
    print(f"Attempting to connect to database at: {'postgresql://****:****@'+DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}") # Mask credentials for printing
    print("Creating database tables if they don't exist...")
    try:
        Base.metadata.create_all(bind=engine)
        print("Tables should now exist or were already present.")
    except Exception as e:
        print(f"Error creating tables: {e}")
        print("Please ensure the PostgreSQL database is running and accessible,")
        print("and that the DATABASE_URL is correctly configured.")
        print(f"Using DATABASE_URL: {'postgresql://****:****@'+DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}")


# --- Dependency for FastAPI to get DB session ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

if __name__ == "__main__":
    # This allows you to run `python database.py` to create tables initially
    create_db_tables()  