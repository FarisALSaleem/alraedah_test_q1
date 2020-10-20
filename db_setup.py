from sqlalchemy import Column, ForeignKey, Integer, String, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()


class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)


class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    last_trade_price = Column(DECIMAL)
    last_trade_volume = Column(DECIMAL)
    last_trade_change_value = Column(DECIMAL)
    last_trade_percentage_change = Column(DECIMAL)
    cumulative_number_of_trades = Column(Integer)
    cumulative_volume_traded = Column(Integer)
    best_bid_price = Column(DECIMAL)
    best_bid_volume = Column(Integer)
    category_id = Column(Integer, ForeignKey('category.id'))
    category = relationship(Category)


# connect to db
db_url = 'postgresql://alraedah_user:plain_text@localhost/tadwul'
engine = create_engine(db_url)

# create tables
Base.metadata.create_all(engine)
