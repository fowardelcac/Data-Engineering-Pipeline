from sqlmodel import create_engine


class Paths:
    ENGINE = create_engine("mysql+pymysql://root:Trips2025*@localhost/prevision")
    PREVISION: str = (
        r"C:\Users\jsaldano\Documents\Procesar\Pipeline\Archivos\PREVISION.xlsx"
    )
    ERRORES: str = (
        r"C:\Users\jsaldano\Documents\Procesar\Pipeline\Archivos\Posibles Errores.xlsx"
    )
    IATA_PATH: str = (
        r"C:\Users\jsaldano\Documents\Procesar\Pipeline\Archivos\iatas.xlsx"
    )
