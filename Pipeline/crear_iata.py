import pandas as pd
from models import Iata
from sqlmodel import create_engine, Session
from functions import *

IATA_PATH: str = r"C:\Users\juans\Documents\cv\Proyectos\Ingenieria-de-Datos\TSA\Data-Engineering-Pipeline2\Pipeline\Archivos\iatas.xlsx"
df = pd.read_excel(IATA_PATH)


def replacer_interrog(country: str):
    return country.replace("?", "N")


paises = [
    "Hong Kong",  # AEROPUERTO DE HONG KONG
    "Emiratos Árabes Unidos",  # AL AIN
    "Líbano",  # BEIRUT
    "Líbano",  # BROUMANA
    "Croacia",  # CAVTAT
    "Croacia",  # CRES
    None,  # CRUCEROS
    "Curazao",  # CURACAO
    "Hong Kong",  # HONG KONG
    "Hong Kong",  # HONG KONG
    "Hong Kong",  # HONG KONG CHEUNG CHAU
    "Hong Kong",  # HONG KONG SUR
    "Hong Kong",  # HONG KONG TSING YI
    "Croacia",  # HVAR
    "Croacia",  # ISTRIA
    "Líbano",  # JOUNIEH
    "Líbano",  # KFARDEBIANE
    "Croacia",  # KOLOCEP
    "Croacia",  # KORENICA
    "Hong Kong",  # KOWLOON
    "Croacia",  # KVARNER BAY
    "Islas Caimán",  # LITTLE CAYMAN
    "Macao",  # MACAU
    "Croacia",  # OMIS
    "Croacia",  # OPATIJA
    "Croacia",  # OREBIC
    "Croacia",  # PLITVICE PARQUE NACIONAL
    "Croacia",  # POREC
    "Croacia",  # PULA
    "Emiratos Árabes Unidos",  # RAS AL KHAYMAH
    "Croacia",  # RIJEKA
    "Croacia",  # ROVINJ
    "República Dominicana",  # SAMANA
    "Sint Maarten",  # SAN MAARTEN-OYSTER PO
    "Hong Kong",  # SHATIN
    "Croacia",  # SIBENIK
    "Croacia",  # SOLIN
    "Croacia",  # SPLIT
    "Croacia",  # SPLIT-MIDDLE DALMATIA
    "Croacia",  # STARI GRAD (HVAR)
    "Hong Kong",  # TINSHUIWAI
    "Líbano",  # TRIPOLI (LB)
    "Croacia",  # TROGIR
    "Hong Kong",  # TSUEN WAN
    "Hong Kong",  # TUEN MUN
    "Croacia",  # VODICE
    "Croacia",  # VRSAR
    "Estados Unidos",  # WEST YELLOWSTONE
    "Croacia",  # ZADAR-NORTH DALMATIA
    "REINO UNIDO",  # nan
]


with Session(ENGINE) as session:
    empty = df[df.Idpaises.isnull()]
    paises_mayus = [p.upper() if p is not None else None for p in paises]
    empty.loc[:, "Idpaises"] = paises_mayus

    data = pd.merge(df, empty, how="outer")
    data.loc[data["Codigociudad"] == "EXT", "Nombreciudad"] = "Exeter"
    data.dropna(inplace=True)

    data["Idpaises"] = data["Idpaises"].astype(str).apply(replacer_interrog)

    for _, row in data.iterrows():
        iata = row["Codigociudad"]
        country = row["Idpaises"]

        # evitar insertar 'nan' como texto
        if pd.notna(iata) and pd.notna(country):
            session.add(Iata(codigo_iata=iata, pais=country))

    session.commit()
