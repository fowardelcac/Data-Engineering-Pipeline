import pandas as pd  # type: ignore
from models import Proveedor, Pasajero, Reserva
from sqlmodel import Session, select, and_  # type: ignore
from functions import *


def bulk_prov(df: pd.DataFrame, session: Session, logger: logging.Logger) -> dict:
    """Carga provededores con logging"""
    logger.info("🏢 Iniciando carga de proveedores...")
    proveedores_map = {}
    unique_proveedores = df["proveedor"].dropna().unique()
    logger.info(f"   Proveedores únicos encontrados: {len(unique_proveedores)}")
    for i, nombre in enumerate(unique_proveedores, 1):
        try:
            obj = verify_existence(session, Proveedor, "nombre_proveedor", nombre)
            proveedores_map[nombre] = obj.id_proveedor
            if i % 10 == 0:
                logger.info(f"   Procesados {i}/{len(unique_proveedores)} proveedores")
        except Exception as e:
            logger.error(f"   Error procesando proveedor '{nombre}': {str(e)}")

    logger.info(f"✅ Carga de proveedores completada: {len(proveedores_map)} cargados")
    return proveedores_map


def bulk_pass(df: pd.DataFrame, session: Session, logger: logging.Logger) -> dict:
    logger.info("👥 Iniciando carga de pasajeros...")
    pasajeros_map = {}
    unique_pasajeros = df["pasajero"].dropna().unique()
    logger.info(f"   Pasajeros únicos encontrados: {len(unique_pasajeros)}")

    for i, nombre in enumerate(unique_pasajeros, 1):
        try:
            obj = verify_existence(session, Pasajero, "nombre_pasajero", nombre)
            pasajeros_map[nombre] = obj.id_pasajero
            if i % 50 == 0:
                logger.info(f"   Procesados {i}/{len(unique_pasajeros)} pasajeros")
        except Exception as e:
            logger.error(f"   Error procesando pasajero '{nombre}': {str(e)}")

    logger.info(f"✅ Carga de pasajeros completada: {len(pasajeros_map)} cargados")
    return pasajeros_map


# se lee el df y procesa cada fila individualemnte, esta funcion recibe la fila desde iterrows()
def process_row(
    session: Session,
    row: pd.Series,
    proveedores_map: dict,
    pasajeros_map: dict,
    tracker: ProcessTracker,
    logger: logging.Logger,
    row_index: int,
) -> None:
    """Procesa una fila con tracking detallado"""
    file_code = row.get("file", f"ROW_{row_index}")
    try:
        tracker.increment_processed()
        create_dic: dict = {
            "file": row["file"],
            "estado": row["estado"] if pd.notna(row["estado"]) else None,
            "moneda": row["moneda"] if pd.notna(row["moneda"]) else None,
            "total": row["monto_a_pagar"] if pd.notna(row["monto_a_pagar"]) else None,
            "fecha_pago_proveedor": (
                row["fecha_de_pago_proveedor"]
                if pd.notna(row["fecha_de_pago_proveedor"])
                else None
            ),
            "fecha_in": (
                row["fecha_servicio"] if pd.notna(row["fecha_servicio"]) else None
            ),
            "fecha_out": row["fecha_out"] if pd.notna(row["fecha_out"]) else None,
            "id_proveedor": proveedores_map.get(row["proveedor"]),
            "id_pasajero": pasajeros_map.get(row["pasajero"]),
            "codigo_iata": row["ciudad"] if pd.notna(row["ciudad"]) else None,
        }

        conditions = [
            Reserva.file == create_dic.get("file"),
            Reserva.id_pasajero == create_dic.get("id_pasajero"),
            Reserva.id_proveedor == create_dic.get("id_proveedor"),
            Reserva.total == create_dic.get("total"),
        ]

        result = session.exec(select(Reserva).where(and_(*conditions))).first()
        if result:
            # Registro existente - verificar actualizaciones
            changed_fields = []
            items: list = [
                "estado",
                "moneda",
                "fecha_pago_proveedor",
                "fecha_in",
                "fecha_out",
                "codigo_iata",
            ]
            for key in items:
                value = create_dic.get(key)
                if getattr(result, key) != value:
                    setattr(result, key, value)
                    changed_fields.append(key)
            if changed_fields:
                session.add(result)
                tracker.add_update(file_code, row, changed_fields)
                logger.info(
                    f"📝 ACTUALIZADO: {file_code} - Campos: {', '.join(changed_fields)}"
                )
            else:
                tracker.add_no_change(file_code)
                if row_index % 500 == 0:  # Log menos frecuente para sin cambios
                    logger.debug(f"⚪ SIN CAMBIOS: {file_code}")
        else:
            nueva = Reserva(**create_dic)
            session.add(nueva)
            tracker.add_new(file_code, row)
            logger.info(
                f"✨ NUEVO: {file_code} - Proveedor: {row.get('proveedor', 'N/A')}"
            )
    except Exception as e:
        error_msg = f"Error procesando fila {row_index}: {str(e)}"
        tracker.add_error(file_code, row, error_msg)
        logger.error(f"❌ ERROR: {file_code} - {error_msg}")


def main():
    """Función principal con logging completo"""
    # Setup inicial
    logger, log_filename = setup_logging()
    tracker = ProcessTracker()
    try:
        # Leer archivo
        excel_path = (
            r"C:\Users\jsaldano\Documents\Procesar\Pipeline\Archivos\SaldoAutoriza.xlsx"
        )
        logger.info(f"📁 Leyendo archivo: {excel_path}")
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Archivo no encontrado: {excel_path}")

        df = pd.read_excel(excel_path)
        logger.info(f"📊 Archivo leído exitosamente: {len(df)} filas encontradas")

        # Preprocesamiento
        logger.info("🔄 Iniciando preprocesamiento...")
        df_original_count = len(df)
        df = preproccess_traffic(df)
        logger.info(
            f"✅ Preprocesamiento completado: {len(df)} filas válidas (eliminadas: {df_original_count - len(df)})"
        )

        with Session(ENGINE) as session:
            # Cargar mapeos
            proveedores_map = bulk_prov(df, session, logger)
            pasajeros_map = bulk_pass(df, session, logger)
            # Procesar filas
            logger.info("🚀 Iniciando procesamiento de reservas...")
            for index, row in df.iterrows():
                process_row(
                    session, row, proveedores_map, pasajeros_map, tracker, logger, index
                )
                # Progress logging
                if (index + 1) % 100 == 0:
                    stats = tracker.stats
                    logger.info(
                        f"📈 Progreso: {index + 1}/{len(df)} | Nuevos: {stats['nuevos']} | Actualizados: {stats['actualizados']} | Errores: {stats['errores']}"
                    )
            # Commit final
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
        # Generar reportes finales
        logger.info("📋 Generando reportes finales...")

        # Resumen en consola
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

        # Exportar a Excel
        folder = (
            r"C:\Users\jsaldano\Documents\Procesar\Pipeline\Archivos\Posibles Errores"
        )
        excel_filename = tracker.export_to_excel(folder)
        if excel_filename:
            logger.info(f"📄 Reporte Excel generado: {excel_filename}")

        logger.info(f"📋 Log guardado en: {log_filename}")
        logger.info("=" * 60)
        logger.info("✅ PROCESO COMPLETADO")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
