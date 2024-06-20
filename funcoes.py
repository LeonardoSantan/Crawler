
from bs4 import BeautifulSoup
import pandas as pd
import lxml
import requests
import datetime
import time
import pytz
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from connection import insert_data_fii
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}


dividendos_valores = []
nomes_fii = []
valores_fii = []
data_hora_atual = []
links = []


df2 = pd.read_csv(
    "fundosListados.csv",
    engine="python",
    encoding="latin-1",
    header=None,
    skiprows=1,
    skip_blank_lines=True,
)


nome = df2.iloc[0: len(df2), 3].values
link = "https://www.fundsexplorer.com.br/funds"


def consulta_individual(i):
    link_name = f"{link}/{nome[i]}11"
    dividendos = requests.get(link_name, headers=headers).text
    teste_dividendos = BeautifulSoup(dividendos, "lxml")
    if teste_dividendos.find("div", class_="headerTicker__content__price").find("p").text and teste_dividendos.find("div", class_="headerTicker__content__price").find("p").text == 'R$ 0,00':
        return
    else:
        consulta_teste = teste_dividendos.find(
            "div", class_="headerTicker__content__price").find("p")
    if consulta_teste != 'N/A':
        value_fii = consulta_teste.text
    else:
        return nome[i], "0.00", "0.00", link_name
    if teste_dividendos.find("div", class_="indicators historic").find("b").text == 'N/A' or teste_dividendos.find("div", class_="indicators historic").find("b").text == 'NaN':
        div = "0.00"
    else:
        div = teste_dividendos.find(
            "div", class_="indicators historic").find("b").text
    return nome[i], value_fii, div, link_name


def consulta_geral():
    while True:    
        start = time.time()
        with ThreadPoolExecutor(max_workers=8) as executor:

            resultados = executor.map(consulta_individual, range(len(df2)))
        for resultado in resultados:
            if (resultado):
                insert_data_fii(resultado[0], resultado[1],
                                resultado[2], resultado[3])
            else:
                continue
        end = time.time()
        print(end - start)
    time.sleep(10)