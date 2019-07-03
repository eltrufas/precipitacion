import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from datetime import datetime
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Rayo(Base):
    __tablename__ = 'rayos'
    id = Column(Integer, primary_key=True)
    longitud = Column(Float())
    latitud = Column(Float())
    corriente_pico = Column(Float())
    multiplicidad = Column(Float())
    fecha = Column(DateTime(), index=True)

url = 'postgresql://postgres:postgres@localhost:5432/postgres'
engine = create_engine(url)
 
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

with open('NLDN_flash_Tiles5-6_2009/NLDN_flash_Tiles5-6_2009', 'r') as fp:
    for i, line in enumerate(fp):
        things = line.split()
        dt = datetime.strptime(things[0] + ' ' +  things[1], '%m/%d/%y %H:%M:%S')
        lon = float(things[2])
        lat = float(things[3])
        cp = float(things[4])
        mp = float(things[5])

        rayo = Rayo(longitud=lon, latitud=lat, corriente_pico=cp, multiplicidad=mp, fecha=dt)
        session.add(rayo)
        if i % 1000 == 0:
            print("guardando", i)
            session.commit()

