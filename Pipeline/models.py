from sqlmodel import SQLModel, Field
from datetime import date


class Proveedor(SQLModel, table=True):
    __tablename__ = "proveedores"
    id_proveedor: int | None = Field(default=None, primary_key=True)
    nombre_proveedor: str = Field(max_length=255)


class Pasajero(SQLModel, table=True):
    __tablename__ = "pasajeros"
    id_pasajero: int | None = Field(default=None, primary_key=True)
    nombre_pasajero: str = Field(max_length=255)


class Iata(SQLModel, table=True):
    __tablename__ = "iatas"
    codigo_iata: str = Field(primary_key=True, max_length=3)
    pais: str = Field(max_length=50)


class Cuenta(SQLModel, table=True):
    __tablename__ = "cuentas"
    id_cuenta: int | None = Field(default=None, primary_key=True)
    banco: str = Field(max_length=50)


class Reserva(SQLModel, table=True):
    __tablename__ = "reservas"
    id_reserva: int | None = Field(default=None, primary_key=True)
    file: str = Field(max_length=6)
    estado: str = Field(max_length=2)
    moneda: str | None = Field(max_length=1)
    total: float
    fecha_pago_proveedor: date | None
    fecha_in: date | None
    fecha_out: date | None
    id_proveedor: int = Field(foreign_key="proveedores.id_proveedor")
    id_pasajero: int = Field(foreign_key="pasajeros.id_pasajero")
    codigo_iata: str = Field(max_length=3, foreign_key="iatas.codigo_iata")


class Saldo(SQLModel, table=True):
    __tablename__ = "saldos"
    id_saldo: int | None = Field(default=None, primary_key=True)
    codigo_transferencia: str | None = Field(max_length=30)
    tipo_movimiento: str = Field(max_length=1)
    fecha_pago: date | None
    descripcion: str | None = Field(max_length=150)
    moneda_pago: str = Field(max_length=1)
    monto: float
    tipo_de_cambio: float
    comision: float | None
    impuesto: float | None
    estado_pago: str | None
    tipo_de_saldo: str | None
    id_reserva: int = Field(foreign_key="reservas.id_reserva")
    id_cuenta: int = Field(foreign_key="cuentas.id_cuenta")
