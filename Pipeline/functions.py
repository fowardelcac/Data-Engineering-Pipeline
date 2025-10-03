import pandas as pd
from sqlmodel import select, Session
import logging
from datetime import datetime
import hashlib
from Pipeline.utils import Paths
from Pipeline.models import Iata


class ProcessData:
    @staticmethod
    def hash_row(row):
        # columnas que definen la transacción
        claves = [
            "file",
            "moneda",
            "fecha_in",
            "fecha_out",
            "fecha_sal",
            "proveedor",
            "pasajero",
            "codigo_iata",
        ]
        row_str = "|".join(str(row[c]) for c in claves)
        return hashlib.sha256(row_str.encode()).hexdigest()

    @staticmethod
    def clean_str(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip().str.upper()
        return df

    @staticmethod
    def clean_nan(value):
        return None if pd.isna(value) else value

    @staticmethod
    def preproccess_traffic(df: pd.DataFrame) -> pd.DataFrame:
        # --- Limpiar strings primero ---
        df = ProcessData.clean_str(
            df, ["estado", "moneda", "proveedor", "pasajero", "codigo_iata"]
        )

        # --- Convertir strings vacíos a None ---
        text_cols = df.select_dtypes(include="object").columns
        df[text_cols] = df[text_cols].replace({"": None})

        # --- Redondear totales ---
        df.loc[:, "total"] = round(df["total"], 2)

        # --- Convertir fechas ---
        date_cols = ["fecha_pago_proveedor", "fecha_in", "fecha_out", "fecha_sal"]
        for col in date_cols:
            df.loc[:, col] = pd.to_datetime(df[col], errors="coerce").dt.date

        # --- Crear hash determinista ---
        df["hash"] = df.apply(ProcessData.hash_row, axis=1)

        # --- Guardar duplicados antes de eliminarlos (manteniendo 1) ---
        duplicated_rows = df[df.duplicated(subset=df.columns, keep="first")].copy()
        df.drop_duplicates(subset=df.columns, keep="first", inplace=True)

        # --- Guardar filas con 'file' nulo ---
        missing_file_rows = df[df["file"].isna()].copy()
        df.dropna(subset=["file"], inplace=True)

        # --- Guardar eliminadas en un Excel ---
        removed_rows = pd.concat([duplicated_rows, missing_file_rows]).drop_duplicates()
        removed_rows.to_excel(Paths.ERRORES, index=False)

        return df

    @staticmethod
    def preproccess_prev(df: pd.DataFrame) -> pd.DataFrame:
        columns_to_check = [
            "codigo_transferencia",
            "tipo_movimiento",
            "fecha_pago",
            "descripcion",
            "moneda_pago",
            "monto",
            "tipo_de_cambio",
            "comision",
            "impuesto",
            "estado_pago",
            "tipo_de_saldo",
            "banco",
        ]

        df_filtered = df[df[columns_to_check].notna().any(axis=1)].copy()

        df_filtered = ProcessData.clean_str(
            df_filtered,
            [
                "codigo_transferencia",
                "tipo_movimiento",
                "descripcion",
                "moneda_pago",
                "estado_pago",
                "tipo_de_saldo",
                "banco",
            ],
        )

        df_filtered["codigo_transferencia"] = df_filtered[
            "codigo_transferencia"
        ].str.replace(".0", "")

        df_filtered["fecha_pago"] = pd.to_datetime(df_filtered["fecha_pago"]).dt.date

        text_cols = df_filtered.select_dtypes(include="object").columns
        df_filtered[text_cols] = df_filtered[text_cols].replace({"": None})
        return df_filtered


#############################################################################################
# RELACIONADO CON LA BDD


def verify_existence(session, model, field_name, value):
    if value is None:
        return None

    stmt = select(model).where(getattr(model, field_name) == value)
    obj = session.exec(stmt).first()
    if obj:
        return obj

    try:
        obj = model(**{field_name: value})
        session.add(obj)  # marca el objeto como "pendiente" de insertar
        session.flush()  # ejecuta el INSERT real en la base de datos, y ahora el objeto tiene un id asignado
        # vuelve a consultar el objeto desde la base de datos para asegurarse de que todos los campos estén actualizados
        session.refresh(obj)
        return obj
    except Exception as e:
        session.rollback()
        return None


#############################################################################################
# RELACIONADO CON LOGS
def setup_logging():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # También muestra en consola
        ],
    )

    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("INICIANDO PROCESO DE IMPORTACIÓN DE RESERVAS")
    logger.info("=" * 60)
    return logger


class ProcessTracker:
    """Clase para trackear el proceso y generar reportes"""

    def __init__(self):
        self.stats = {
            "nuevos": 0,
            "actualizados": 0,
            "sin_cambios": 0,
            "errores": 0,
            "total_procesadas": 0,
        }
        self.updated_records = []
        self.error_records = []
        self.new_records = []
        self.start_time = datetime.now()

    def add_new(self, file_code, row_data):
        """Registra un nuevo registro"""
        self.stats["nuevos"] += 1
        self.new_records.append(
            {
                "file": file_code,
                "accion": "NUEVO",
                "timestamp": datetime.now(),
                "proveedor": row_data.get("proveedor", ""),
                "pasajero": row_data.get("pasajero", ""),
                "monto": row_data.get("monto_a_pagar", 0),
                "detalle": "Registro creado exitosamente",
            }
        )

    def add_update(self, file_code, row_data, changed_fields):
        """Registra una actualización"""
        self.stats["actualizados"] += 1
        self.updated_records.append(
            {
                "file": file_code,
                "accion": "ACTUALIZADO",
                "timestamp": datetime.now(),
                "proveedor": row_data.get("proveedor", ""),
                "pasajero": row_data.get("pasajero", ""),
                "monto": row_data.get("monto_a_pagar", 0),
                "campos_modificados": ", ".join(changed_fields),
                "detalle": f'Campos actualizados: {", ".join(changed_fields)}',
            }
        )
        print(self.updated_records[-1])

    def add_no_change(self):
        """Registra un registro sin cambios"""
        self.stats["sin_cambios"] += 1

    def add_error(self, file_code, row_data, error_msg):
        """Registra un error"""
        self.stats["errores"] += 1
        self.error_records.append(
            {
                "file": file_code,
                "accion": "ERROR",
                "timestamp": datetime.now(),
                "proveedor": (
                    row_data.get("proveedor", "") if row_data is not None else ""
                ),
                "pasajero": (
                    row_data.get("pasajero", "") if row_data is not None else ""
                ),
                "monto": (
                    row_data.get("monto_a_pagar", 0) if row_data is not None else 0
                ),
                "error": error_msg,
                "detalle": f"Error durante procesamiento: {error_msg}",
            }
        )

    def increment_processed(self):
        """Incrementa el contador de filas procesadas"""
        self.stats["total_procesadas"] += 1

    def get_summary(self):
        """Retorna un resumen del proceso"""
        end_time = datetime.now()
        duration = end_time - self.start_time

        return {
            "inicio": self.start_time,
            "fin": end_time,
            "duracion": str(duration),
            "stats": self.stats,
            "tasa_exito": round(
                (self.stats["nuevos"] + self.stats["actualizados"])
                / max(self.stats["total_procesadas"], 1)
                * 100,
                2,
            ),
        }


def init_iata():

    df = pd.read_excel(Paths.IATA_PATH)

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

    with Session(Paths.ENGINE) as session:
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
