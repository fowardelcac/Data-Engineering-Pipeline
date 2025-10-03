import pandas as pd
from Pipeline.models import Saldo, Cuenta, Reserva
from sqlmodel import Session, select
from Pipeline.functions import (
    setup_logging,
    verify_existence,
    ProcessData,
    ProcessTracker,
)
from Pipeline.utils import Paths
import os, logging


def process_row(
    session: Session,
    row: pd.Series,
    tracker: ProcessTracker,
    logger: logging.Logger,
    row_index: int,
) -> None:
    # file_code = id_saldo
    file_code = row.get("id_saldo", f"ROW_{row_index}")
    try:
        tracker.increment_processed()
        banco: Cuenta = verify_existence(session, Cuenta, "banco", row["banco"])
        verify_saldo: Saldo = session.exec(
            select(Saldo).where(Saldo.id_reserva == row["id_reserva"])
        ).first()
        create_dic: dict = {
            "codigo_transferencia": row.get("codigo_transferencia"),
            "tipo_movimiento": row.get("tipo_movimiento"),
            "fecha_pago": row.get("fecha_pago"),
            "descripcion": row.get("descripcion"),
            "moneda_pago": row.get("moneda_pago"),
            "monto": ProcessData.clean_nan(row.get("monto")),
            "tipo_de_cambio": ProcessData.clean_nan(row.get("tipo_de_cambio")),
            "comision": ProcessData.clean_nan(row.get("comision")),
            "impuesto": ProcessData.clean_nan(row.get("impuesto")),
            "estado_pago": row.get("estado_pago"),
            "tipo_de_saldo": row.get("tipo_de_saldo"),
            "id_reserva": None,
            "id_cuenta": banco.id_cuenta if banco else None,
        }

        if not verify_saldo:
            reserva: Reserva = session.exec(
                select(Reserva).where(Reserva.id_reserva == row["id_reserva"])
            ).first()
            if reserva:
                create_dic["id_reserva"] = reserva.id_reserva
                nueva = Saldo(**create_dic)
                session.add(nueva)
                tracker.add_new(file_code, row)
                logger.info(
                    f"✨ NUEVO: id_reserva={row['id_reserva']} - Banco: {row.get('banco')}"
                )
            else:
                msg = f"Reserva {row['id_reserva']} no existe en la tabla Reserva"
                tracker.add_error(file_code, row, msg)
                logger.warning(f"⚠️ {msg}")
        else:
            changed_fields: list = []
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
                current = getattr(verify_saldo, key)

                if value != current:
                    setattr(verify_saldo, key, value)
                    changed_fields.append(key)

            if changed_fields:
                session.add(verify_saldo)
                tracker.add_update(file_code, row, changed_fields)
                logger.info(
                    f"📝 ACTUALIZADO: id_reserva={row['id_reserva']} - Campos: {', '.join(changed_fields)}"
                )
            else:
                tracker.add_no_change()
                if row_index % 500 == 0:
                    logger.debug(f"⚪ SIN CAMBIOS: id_reserva={row['id_reserva']}")
    except Exception as e:
        msg = f"Error procesando fila {row_index}: {str(e)}"
        tracker.add_error(file_code, row, msg)
        logger.error(f"❌ {msg}")


def main_excel():
    logger = setup_logging()
    tracker = ProcessTracker()
    try:
        logger.info(f"📁 Leyendo archivo: {Paths.PREVISION}")
        if not os.path.exists(Paths.PREVISION):
            raise FileNotFoundError(f"Archivo no encontrado: {Paths.PREVISION}")

        df = pd.read_excel(Paths.PREVISION, engine="openpyxl")
        logger.info(f"📊 Archivo leído exitosamente: {len(df)} filas encontradas")
        logger.info("🔄 Iniciando preprocesamiento...")
        df_original_count = len(df)
        df = ProcessData.preproccess_prev(df)
        logger.info(
            f"✅ Preprocesamiento completado: {len(df)} filas válidas (eliminadas: {df_original_count - len(df)})"
        )
        with Session(Paths.ENGINE) as session:
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
        logger.info("=" * 60)
        logger.info("✅ PROCESO COMPLETADO")
        logger.info("=" * 60)


if __name__ == "__main__":
    main_excel()
