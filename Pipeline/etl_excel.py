import pandas as pd # type: ignore
from models import Saldo, Cuenta, Reserva
from sqlmodel import Session, select # type: ignore
import logging
from functions import *

# ---------- TRACKER ----------
class ProcessTracker:
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

    def add_new(self, row_data):
        self.stats["nuevos"] += 1
        self.new_records.append(
            {
                "accion": "NUEVO",
                "timestamp": datetime.now(),
                "id_reserva": row_data.get("id_reserva"),
                "banco": row_data.get("banco"),
                "monto": row_data.get("monto"),
                "detalle": "Saldo creado exitosamente",
            }
        )

    def add_update(self, row_data, changed_fields):
        self.stats["actualizados"] += 1
        self.updated_records.append(
            {
                "accion": "ACTUALIZADO",
                "timestamp": datetime.now(),
                "id_reserva": row_data.get("id_reserva"),
                "banco": row_data.get("banco"),
                "monto": row_data.get("monto"),
                "campos_modificados": ", ".join(changed_fields),
            }
        )

    def add_no_change(self):
        self.stats["sin_cambios"] += 1

    def add_error(self, row_data, error_msg):
        self.stats["errores"] += 1
        self.error_records.append(
            {
                "accion": "ERROR",
                "timestamp": datetime.now(),
                "id_reserva": (
                    row_data.get("id_reserva") if row_data is not None else None
                ),
                "banco": row_data.get("banco") if row_data is not None else None,
                "error": error_msg,
            }
        )

    def increment_processed(self):
        self.stats["total_procesadas"] += 1

    def get_summary(self):
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

    def export_to_excel(self, folder=None, filename_prefix="reporte_saldos"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if folder is None:
            folder = os.getcwd()
        os.makedirs(folder, exist_ok=True)

        all_records = []
        all_records.extend(self.new_records)
        all_records.extend(self.updated_records)
        all_records.extend(self.error_records)

        if all_records:
            df_reporte = pd.DataFrame(all_records)
            filename = f"{filename_prefix}_{timestamp}.xlsx"
            filepath = os.path.join(folder, filename)

            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                df_reporte.to_excel(writer, sheet_name="Resumen_General", index=False)
                if self.updated_records:
                    pd.DataFrame(self.updated_records).to_excel(
                        writer, sheet_name="Actualizados", index=False
                    )
                if self.error_records:
                    pd.DataFrame(self.error_records).to_excel(
                        writer, sheet_name="Errores", index=False
                    )
                if self.new_records:
                    pd.DataFrame(self.new_records).to_excel(
                        writer, sheet_name="Nuevos", index=False
                    )

                summary = self.get_summary()
                df_stats = pd.DataFrame(
                    [
                        ["Fecha de inicio", summary["inicio"]],
                        ["Fecha de fin", summary["fin"]],
                        ["Duración", summary["duracion"]],
                        ["Total procesadas", summary["stats"]["total_procesadas"]],
                        ["Nuevos", summary["stats"]["nuevos"]],
                        ["Actualizados", summary["stats"]["actualizados"]],
                        ["Sin cambios", summary["stats"]["sin_cambios"]],
                        ["Errores", summary["stats"]["errores"]],
                        ["Tasa de éxito (%)", summary["tasa_exito"]],
                    ],
                    columns=["Métrica", "Valor"],
                )
                df_stats.to_excel(writer, sheet_name="Estadisticas", index=False)

            return filepath
        return None


def clean_nan(value):
    return None if pd.isna(value) else value


def preproccess(df: pd.DataFrame) -> pd.DataFrame:
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

    # Filtra filas donde al menos una columna NO es NaN
    df_filtered = df[df[columns_to_check].notna().any(axis=1)].copy()

    # Aplicar transformaciones al DataFrame filtrado
    df_filtered["codigo_transferencia"] = (
        df_filtered["codigo_transferencia"]
        .astype(str)
        .str.strip()
        .str.replace(".0", "")
    )
    df_filtered["tipo_movimiento"] = (
        df_filtered["tipo_movimiento"].fillna("").str.strip().str.upper()
    )
    df_filtered["fecha_pago"] = pd.to_datetime(
        df_filtered["fecha_pago"], errors="coerce"
    ).dt.date

    df_filtered["descripcion"] = (
        df_filtered["descripcion"].fillna("").str.strip().replace({"": None})
    )
    df_filtered["moneda_pago"] = (
        df_filtered["moneda_pago"].fillna("").str.strip().str.upper().str[0]
    )
    df_filtered["estado_pago"] = (
        df_filtered["estado_pago"].fillna("").str.strip().str.upper()
    )
    df_filtered["tipo_de_saldo"] = (
        df_filtered["tipo_de_saldo"].fillna("").str.strip().str.upper()
    )
    df_filtered["banco"] = df_filtered["banco"].fillna("").str.strip().str.upper()

    # Columnas de texto
    text_cols = df_filtered.select_dtypes(include="object").columns
    df_filtered[text_cols] = df_filtered[text_cols].replace({"": None})

    return df_filtered


def process_row(
    session: Session,
    row: pd.Series,
    tracker: ProcessTracker,
    logger: logging.Logger,
    index: int,
) -> None:
    try:
        tracker.increment_processed()
        banco: Cuenta = verify_existence(session, Cuenta, "banco", row["banco"])
        verify_saldo: Saldo = session.exec(
            select(Saldo).where(Saldo.id_reserva == row["id_reserva"])
        ).first()

        create_dic = {
            "codigo_transferencia": row.get("codigo_transferencia"),
            "tipo_movimiento": row.get("tipo_movimiento"),
            "fecha_pago": row.get("fecha_pago"),
            "descripcion": row.get("descripcion"),
            "moneda_pago": row.get("moneda_pago"),
            "monto": clean_nan(row.get("monto")),
            "tipo_de_cambio": clean_nan(row.get("tipo_de_cambio")),
            "comision": clean_nan(row.get("comision")),
            "impuesto": clean_nan(row.get("impuesto")),
            "estado_pago": row.get("estado_pago"),
            "tipo_de_saldo": row.get("tipo_de_saldo"),
            "id_reserva": None,
            "id_cuenta": banco.id_cuenta if banco else None,
        }

        if not verify_saldo:
            reserva = session.exec(
                select(Reserva).where(Reserva.id_reserva == row["id_reserva"])
            ).first()
            if reserva:
                create_dic["id_reserva"] = reserva.id_reserva
                nueva = Saldo(**create_dic)
                session.add(nueva)
                tracker.add_new(row)
                logger.info(
                    f"✨ NUEVO: id_reserva={row['id_reserva']} - Banco: {row.get('banco')}"
                )
            else:
                msg = f"Reserva {row['id_reserva']} no existe en la tabla Reserva"
                tracker.add_error(row, msg)
                logger.warning(f"⚠️ {msg}")
        else:
            changed_fields = []
            items: list = [
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
                "id_cuenta",
            ]
            for key in items:
                value = create_dic.get(key)
                if getattr(verify_saldo, key) != value:
                    setattr(verify_saldo, key, value)
                    changed_fields.append(key)

            if changed_fields:
                session.add(verify_saldo)
                tracker.add_update(row, changed_fields)
                logger.info(
                    f"📝 ACTUALIZADO: id_reserva={row['id_reserva']} - Campos: {', '.join(changed_fields)}"
                )
            else:
                tracker.add_no_change()
                if index % 500 == 0:
                    logger.debug(f"⚪ SIN CAMBIOS: id_reserva={row['id_reserva']}")
    except Exception as e:
        msg = f"Error procesando fila {index}: {str(e)}"
        tracker.add_error(row, msg)
        logger.error(f"❌ {msg}")


def main():
    logger, log_filename = setup_logging()
    tracker = ProcessTracker()
    try:
        excel_path = r"C:\Users\jsaldano\Documents\Procesar\Pipeline\Archivos\PREVISION.xlsx"
        logger.info(f"📁 Leyendo archivo: {excel_path}")
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Archivo no encontrado: {excel_path}")
        df = pd.read_excel(excel_path)
        logger.info(f"📊 Archivo leído exitosamente: {len(df)} filas encontradas")
        logger.info("🔄 Iniciando preprocesamiento...")
        df_original_count = len(df)
        df = preproccess(df)
        logger.info(
            f"✅ Preprocesamiento completado: {len(df)} filas válidas (eliminadas: {df_original_count - len(df)})"
        )
        with Session(ENGINE) as session:
            logger.info("🚀 Iniciando procesamiento de saldos...")
            for index, row in df.iterrows():
                process_row(session, row, tracker, logger, index)
                if (index + 1) % 100 == 0:
                    stats = tracker.stats
                    logger.info(
                        f"📈 Progreso: {index+1}/{len(df)} | Nuevos: {stats['nuevos']} | Actualizados: {stats['actualizados']} | Errores: {stats['errores']}"
                    )
            logger.info("💾 Realizando commit final...")
            session.commit()
            logger.info("✅ Commit exitoso")
    except Exception as e:
        logger.error(f"❌ ERROR CRÍTICO: {str(e)}")
        if "session" in locals():
            session.rollback()
            logger.info("🔄 Rollback realizado")
        raise
    finally:
        logger.info("📋 Generando reportes finales...")
        summary = tracker.get_summary()
        logger.info("=" * 60)
        logger.info("📊 RESUMEN FINAL DEL PROCESO")
        logger.info("=" * 60)
        logger.info(f"⏰ Duración total: {summary['duracion']}")
        logger.info(f"📁 Total procesadas: {summary['stats']['total_procesadas']}")
        logger.info(f"✨ Nuevos registros: {summary['stats']['nuevos']}")
        logger.info(f"📝 Registros actualizados: {summary['stats']['actualizados']}")
        logger.info(f"⚪ Sin cambios: {summary['stats']['sin_cambios']}")
        logger.info(f"❌ Errores: {summary['stats']['errores']}")
        logger.info(f"📊 Tasa de éxito: {summary['tasa_exito']}%")

        folder = r"C:\Users\jsaldano\Documents\Test\Procesar\Pipeline\Archivos\Posibles Errores"
        excel_filename = tracker.export_to_excel(folder)
        if excel_filename:
            logger.info(f"📄 Reporte Excel generado: {excel_filename}")
        logger.info(f"📋 Log guardado en: {log_filename}")
        logger.info("=" * 60)
        logger.info("✅ PROCESO COMPLETADO")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
