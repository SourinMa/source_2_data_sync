# db_setup.py

from sqlalchemy import create_engine, Column, String, Boolean, Date, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.engine import URL
from datetime import date, datetime

# 🔥 PostgreSQL connection using URL.create()
# Docker container: postgres-db-3 on port 5434
DATABASE_URL = URL.create(
    drivername="postgresql+psycopg2",
    username="sourin",
    password="admin",  
    host="localhost",
    port=5434,
    database="config-db"
)

engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Config(Base):
    __tablename__ = "config"   

    id = Column(Integer, primary_key=True)
    base_url = Column(String)
    api_key = Column(String)
    country = Column(String)
    data_type = Column(String)
    from_date = Column(Date)
    to_date = Column(Date)
    operator = Column(String)
    hs_code = Column(String)
    active = Column(Boolean, default=True)

    # ✅ New columns
    number_of_rows = Column(Integer, default=0)
    run_id = Column(String, unique=True, nullable=True)
    created_date = Column(DateTime, default=datetime.utcnow)
    updated_date = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def seed():
    session = Session()

    entry = Config(
        base_url="http://127.0.0.1:8000",
        api_key="API123",
        country="India",
        data_type="export",
        from_date=date(2025, 6, 1),
        to_date=date(2025, 6, 30),
        operator="and",
        hs_code="HS_Code-85",
        active=True,

        # ✅ New column values
        number_of_rows=None,
        run_id=None,
    )

    session.add(entry)
    session.commit()
    session.close()

    print("✅ PostgreSQL DB setup completed.")


if __name__ == "__main__":
    # Create table if not exists
    Base.metadata.create_all(bind=engine)

    # Insert sample data
    seed()