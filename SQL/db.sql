CREATE DATABASE prevision;
USE prevision;

CREATE TABLE proveedores (
    id_proveedor INT AUTO_INCREMENT PRIMARY KEY,
    nombre_proveedor VARCHAR(255)
);

CREATE TABLE pasajeros (
    id_pasajero INT AUTO_INCREMENT PRIMARY KEY,
    nombre_pasajero VARCHAR(255)
);

CREATE TABLE iatas (
    codigo_iata VARCHAR(3) PRIMARY KEY,
    pais VARCHAR(50)
);

CREATE TABLE cuentas (
    id_cuenta INT AUTO_INCREMENT PRIMARY KEY,
    banco VARCHAR(50)
);

CREATE TABLE reservas (
    id_reserva INT AUTO_INCREMENT PRIMARY KEY,
    file CHAR(6),
    estado VARCHAR(2),
    moneda ENUM('P', 'D', 'L', 'B'),
    total DECIMAL(15, 2),
    fecha_pago_proveedor DATE,
    fecha_in DATE,
    fecha_out DATE,
    id_proveedor INT,
    id_pasajero INT,
    codigo_iata VARCHAR(3),
    FOREIGN KEY (id_proveedor) REFERENCES proveedores(id_proveedor),
    FOREIGN KEY (id_pasajero) REFERENCES pasajeros(id_pasajero),
    FOREIGN KEY (codigo_iata) REFERENCES iatas(codigo_iata)
);

 
CREATE TABLE saldos (
    id_saldo INT AUTO_INCREMENT PRIMARY KEY,
    codigo_transferencia VARCHAR(30),
    tipo_movimiento ENUM('I', 'E'),
    fecha_pago DATE,
    descripcion VARCHAR(150),
    moneda_pago ENUM('P', 'D', 'L', 'B'),
    monto DECIMAL(15, 2),
    tipo_de_cambio DECIMAL(15, 2),
    comision DECIMAL(15, 2),
    impuesto DECIMAL(15, 2),
    estado_pago ENUM('CANCELADO', 'PAGADO', 'PENDIENTE', 'UTILIZADO'),
    tipo_de_saldo VARCHAR(30),
    id_reserva INT,
    id_cuenta INT,
    FOREIGN KEY (id_reserva) REFERENCES reservas(id_reserva),
    FOREIGN KEY (id_cuenta) REFERENCES cuentas(id_cuenta)
);


INSERT INTO cuentas (banco) VALUES 
('EFECTIVO'),
('PAYONEER'),
('WE TRAVEL'),
('REBA ARS'),
('REBA USD'),
('GALICIA ARS'),
('GALICIA USD'),
('TC'),
('NC'),
('NACION ARS');

CREATE view gabi_prevision AS
SELECT
    r.id_reserva,
    r.file,
    r.estado,
    r.moneda,
    r.total,
    r.fecha_pago_proveedor,
    r.fecha_in,
    r.fecha_out,
    p.nombre_proveedor,
    pa.nombre_pasajero,
    i.pais,
    CASE
        WHEN i.pais = 'ARGENTINA' THEN 'NACIONAL'
        ELSE 'EXTERIOR'
    END AS origen,
    s.codigo_transferencia,
    s.tipo_movimiento,
    s.fecha_pago,
    s.descripcion,
    s.moneda_pago,
    s.monto,
    s.tipo_de_cambio,
    s.comision,
    s.impuesto,
    s.estado_pago,
    s.tipo_de_saldo,
    c.banco
FROM reservas r
LEFT JOIN proveedores p ON p.id_proveedor = r.id_proveedor
LEFT JOIN pasajeros pa ON pa.id_pasajero = r.id_pasajero
LEFT JOIN iatas i ON i.codigo_iata = r.codigo_iata
LEFT JOIN saldos s ON s.id_reserva = r.id_reserva
LEFT JOIN cuentas c ON c.id_cuenta = s.id_cuenta
WHERE r.fecha_pago_proveedor >= CURDATE()
ORDER BY r.fecha_pago_proveedor ASC;

CREATE VIEW s4 AS
SELECT
  s.file,
  id_saldo,
  id_reserva,
  tipo_movimiento,
  fecha_pago,
  moneda_pago,
  monto,
  tipo_de_cambio,
  comision,
  impuesto,
  estado_pago,

  -- Ingreso en USD
  ROUND(
    CASE
      WHEN UPPER(moneda_pago) = 'P'
        THEN monto / NULLIF(tipo_de_cambio, 0)
      ELSE monto
    END, 2
  ) AS ingreso_usd,

  -- Gasto en USD (comisiones + impuestos)
  ROUND(
  CASE
    WHEN UPPER(s.moneda_pago) = 'P'
      THEN (COALESCE(s.comision,0) + COALESCE(s.impuesto,0)) / NULLIF(s.tipo_de_cambio, 0)
    ELSE (COALESCE(s.comision,0) + COALESCE(s.impuesto,0))
  END, 2
) AS gasto_usd

FROM saldos s
LEFT JOIN reservas r ON r.id_reserva = s.id_reserva;


