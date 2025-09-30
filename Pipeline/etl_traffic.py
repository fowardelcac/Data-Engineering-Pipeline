import pandas as pd
from Pipeline.models import Proveedor, Pasajero, Reserva, Iata
from sqlmodel import Session, select
from Pipeline.functions import *
from Pipeline.scrape_traffic import main_scraper


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
    id_orden = row.get("id_orden")
    try:
        tracker.increment_processed()

        codigo_iata = row.get("codigo_iata")
        codigo_iata_valido = None
        
        if codigo_iata and pd.notna(codigo_iata):
            exists = session.exec(select(Iata).where(Iata.codigo_iata == codigo_iata)).first()
            if exists:
                codigo_iata_valido = codigo_iata
            else:
                logger.warning(f"⚠️ Código IATA '{codigo_iata}' no encontrado. Fila {row_index} será omitida.")
                tracker.add_error(file_code, row, f"Código IATA '{codigo_iata}' no existe en la BD")
                return  # ❌ No insertar si el IATA es inválido
            
        create_dic: dict = {
            "id_reserva": id_orden,
            "file": row.get("file"),
            "estado": row.get("estado"),
            "moneda": row.get("moneda"),
            "total": row.get("monto_a_pagar"),
            "fecha_pago_proveedor": row.get("fecha_de_pago_proveedor"),
            "fecha_in": row.get("fecha_servicio"),
            "fecha_out": row.get("fecha_out"),
            "fecha_sal": row.get("fecha_sal"),
            "id_proveedor": proveedores_map.get(row["proveedor"]),
            "id_pasajero": pasajeros_map.get(row["pasajero"]),
            "codigo_iata": codigo_iata_valido,
        }

        result = session.exec(
            select(Reserva).where(Reserva.id_reserva == id_orden)).first()

        if result:
            # Registro existente - verificar actualizaciones
            changed_fields = []
            items: list = [
                "estado",
                "moneda",
                "total",
                "fecha_pago_proveedor",
                "fecha_in",
                "fecha_out",
                "fecha_sal",
                "codigo_iata",
            ]
            for key in items:
                value = create_dic.get(key)
                current = getattr(result, key)

                if values_differ(current, value):
                    logger.debug(f"   CAMPO '{key}': '{current}' → '{value}'")
                    setattr(result, key, value)
                    changed_fields.append(key)

            if changed_fields:
                session.add(result)
                tracker.add_update(file_code, row, changed_fields)
                logger.info(
                    f"📝 ACTUALIZADO: {file_code} (ID: {id_orden}) - Campos: {', '.join(changed_fields)}"
                )
            else:
                tracker.add_no_change()
                if row_index % 500 == 0:  # Log menos frecuente para sin cambios
                    logger.debug(f"⚪ SIN CAMBIOS: {file_code} (ID: {id_orden})")
        else:
            nueva = Reserva(**create_dic)
            session.add(nueva)
            tracker.add_new(file_code, row)
            logger.info(
                f"✨ NUEVO: {file_code} (ID: {id_orden}) - Proveedor: {row.get('proveedor', 'N/A')}"
            )
    except Exception as e:
        error_msg = f"Error procesando fila {row_index}: {str(e)}"
        tracker.add_error(file_code, row, error_msg)
        logger.error(f"❌ ERROR: {file_code} - {error_msg}")


def main_traffic():
    """Función principal con logging completo"""
    # Setup inicial
    logger = setup_logging()
    tracker = ProcessTracker()
    try:
        logger.info("Iniciando comunicacion con traffic...")
        data: pd.DataFrame = main_scraper()
        logger.info(
            f"📊 Archivo descargado exitosamente: {len(data)} filas encontradas"
        )

        # Preprocesamiento
        logger.info("🔄 Iniciando preprocesamiento...")
        df_original_count = len(data)
        df: pd.DataFrame = preproccess_traffic(data)
        df.to_excel("excel.xlsx")

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
        excel_filename = tracker.export_to_excel(ERRORES)
        if excel_filename:
            logger.info(f"📄 Reporte Excel generado: {excel_filename}")

        logger.info("=" * 60)
        logger.info("✅ PROCESO COMPLETADO")
        logger.info("=" * 60)


if __name__ == "__main__":
    main_traffic()
