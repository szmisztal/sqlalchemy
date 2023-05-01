from sqlalchemy import create_engine, Table, Column, ForeignKey, Integer, String, Float, MetaData, text
import csv

engine = create_engine('sqlite:///database.db')
conn = engine.connect()
meta = MetaData()

clean_stations = Table(
    'clean_stations', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String),
)

clean_measure = Table(
    'clean_measure', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String, ForeignKey('clean_stations.station')),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer),
)

meta.create_all(engine)

with open("clean_measure.csv", "r") as file:
    reader = csv.reader(file)
    next(reader)
    for data in reader:
        query = clean_measure.insert().values(station=data[0], date=data[1], precip=data[2], tobs=data[3])
        conn.execute(query)

with open("clean_stations.csv", "r") as file:
    reader = csv.reader(file)
    next(reader)
    for data in reader:
        query = clean_stations.insert().values(station=data[0], latitude=data[1], longitude=data[2], elevation=data[3], name=data[4], country=data[5], state=data[6])
        conn.execute(query)

conn.commit()
print('done')
result = conn.execute(text("SELECT * FROM clean_stations")).fetchall()
result_2 = conn.execute(text("SELECT * FROM clean_measure")).fetchall()
result_3 = conn.execute(text("SELECT * FROM clean_stations")).fetchone()
result_4 = conn.execute(text("SELECT * FROM clean_measure")).fetchone()
print(result)
print(result_2)
print(result_3)
print(result_4)
conn.close()
