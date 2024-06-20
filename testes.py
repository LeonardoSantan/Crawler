# import pandas
# from sqlalchemy import create_engine, text
# from sqlalchemy.orm import sessionmaker
import time
# url = 'postgresql+psycopg2://postgres:admin@localhost:5432/postgres'
# engine = create_engine(url)


# Session = sessionmaker(bind=engine)

date_str = time.strftime("%d/%m/%Y %H:%M")
print(type(date_str))

