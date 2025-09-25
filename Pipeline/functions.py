import pandas as pd # pyright: ignore[reportMissingModuleSource]
from sqlmodel import select, create_engine # type: ignore
import logging
import os
from datetime import datetime


ENGINE = create_engine("mysql+pymysql://root:Trips2025*@localhost/prevision")


def preproccess_traffic(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df.drop_duplicates(subset=df.columns, inplace=True)
    df.dropna(subset=["file", "proveedor", "pasajero"], inplace=True)
    df.loc[:, "estado"] = df["estado"].str.strip().str.upper()
    df.loc[:, "moneda"] = df["moneda"].str.strip().str.upper()
    df.loc[:, "monto_a_pagar"] = round(df["monto_a_pagar"], 2)
    df.loc[:, "fecha_de_pago_proveedor"] = df["fecha_de_pago_proveedor"].dt.date
    df.loc[:, "fecha_servicio"] = df["fecha_servicio"].dt.date
    df.loc[:, "proveedor"] = df["proveedor"].str.strip().str.upper()
    df.loc[:, "pasajero"] = df["pasajero"].str.strip().str.upper()
    df.loc[:, "fecha_out"] = df["fecha_out"].dt.date
    df.loc[:, "ciudad"] = df["ciudad"].str.strip().str.upper().replace({"": None})

    return df


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
    log_filename = f"reservas_process_{timestamp}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # También muestra en consola
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("="*60)
    logger.info("INICIANDO PROCESO DE IMPORTACIÓN DE RESERVAS")
    logger.info("="*60)
    return logger, log_filename


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

    def export_to_excel(self, folder=None, filename_prefix="reporte_reservas"):
        """Exporta los reportes a Excel"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if folder is None:
            folder = os.getcwd()  # Carpeta actual por defecto
        os.makedirs(folder, exist_ok=True)  # Crear carpeta si no existe

        # Crear DataFrame con todos los registros procesados
        all_records = []
        all_records.extend(self.new_records)
        all_records.extend(self.updated_records)
        all_records.extend(self.error_records)

        if all_records:
            df_reporte = pd.DataFrame(all_records)

            # Crear archivo Excel con múltiples hojas
            filename = f"{filename_prefix}_{timestamp}.xlsx"
            filepath = os.path.join(folder, filename)
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                # Hoja con todos los registros
                df_reporte.to_excel(writer, sheet_name="Resumen_General", index=False)

                # Hoja solo con actualizaciones
                if self.updated_records:
                    df_updated = pd.DataFrame(self.updated_records)
                    df_updated.to_excel(writer, sheet_name="Actualizados", index=False)

                # Hoja solo con errores
                if self.error_records:
                    df_errors = pd.DataFrame(self.error_records)
                    df_errors.to_excel(writer, sheet_name="Errores", index=False)

                # Hoja solo con nuevos
                if self.new_records:
                    df_new = pd.DataFrame(self.new_records)
                    df_new.to_excel(writer, sheet_name="Nuevos", index=False)

                # Hoja con estadísticas
                summary = self.get_summary()
                df_stats = pd.DataFrame(
                    [
                        ["Fecha de inicio", summary["inicio"]],
                        ["Fecha de fin", summary["fin"]],
                        ["Duración", summary["duracion"]],
                        ["Total procesadas", summary["stats"]["total_procesadas"]],
                        ["Nuevos registros", summary["stats"]["nuevos"]],
                        ["Registros actualizados", summary["stats"]["actualizados"]],
                        ["Sin cambios", summary["stats"]["sin_cambios"]],
                        ["Errores", summary["stats"]["errores"]],
                        ["Tasa de éxito (%)", summary["tasa_exito"]],
                    ],
                    columns=["Métrica", "Valor"],
                )
                df_stats.to_excel(writer, sheet_name="Estadisticas", index=False)

            return filepath

        return None