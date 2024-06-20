from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import time
url = 'postgresql+psycopg2://postgres:x@167.234.231.201:6543/postgres'
engine = create_engine(url)

Session = sessionmaker(bind=engine)


def test_connection():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * from fii;"))
            print(f"Connection test succeeded, result{result}")
    except Exception as e:
        print(f"Connection test failed: {e}")


def insert_data_fii(nomes_fii, valores_fii, dividendos_valores, links):
    session = Session()
    date_str = time.strftime("%Y-%m-%d %H:%M:%S")
    print(date_str)
    try:
        
        valores_fii_truncated = valores_fii[3:]
        valores_fii_modificado = valores_fii_truncated.replace(
            '.', '').replace(',', '.')
        dividendos_valores_modificado = dividendos_valores.replace(
            '.', '').replace(',', '.')

        query = text(
            f"INSERT INTO FII(nomes_fii, valores_fii, dividendos_valores, links, date_consulta) values(:nomes_fii, :valores_fii, :dividendos_valores, :links, :date_consulta)")
        session.execute(query, {
            'nomes_fii': nomes_fii,
            'valores_fii': valores_fii_modificado,
            'dividendos_valores': dividendos_valores_modificado,
            'links': links,
            'date_consulta': date_str
        })
        session.commit()
        
    except Exception as e:
        print(f"Connection test failed: {e}")
        session.rollback()

    finally:
        session.close()
        print(f"INSERT INTO FII values({nomes_fii}, {valores_fii}, {dividendos_valores}, {links}, {date_str})")
