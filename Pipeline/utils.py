from sqlmodel import select, create_engine


ENGINE = create_engine("mysql+pymysql://root:2001@localhost/prevision")

PREVISION: str = (
    r"C:\Users\juans\Documents\cv\Proyectos\Ingenieria-de-Datos\TSA\Data-Engineering-Pipeline2\Pipeline\Archivos\PREVISION.xlsx"
)
ERRORES: str = (
    r"C:\Users\juans\Documents\cv\Proyectos\Ingenieria-de-Datos\TSA\Data-Engineering-Pipeline2\Pipeline\Archivos\Posibles Errores"
)
IATA_PATH: str = r"C:\Users\juans\Documents\cv\Proyectos\Ingenieria-de-Datos\TSA\Data-Engineering-Pipeline2\Pipeline\Archivos\iatas.xlsx"
