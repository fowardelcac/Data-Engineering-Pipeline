DROP DATABASE PREVISION;
CREATE DATABASE PREVISION;
USE PREVISION;

CREATE TABLE proveedores (
    id_proveedor INT AUTO_INCREMENT PRIMARY KEY,
    nombre_proveedor VARCHAR(255) NOT NULL
);

CREATE TABLE pasajeros (
    id_pasajero INT AUTO_INCREMENT PRIMARY KEY,
    nombre_pasajero VARCHAR(255) NOT NULL
);

CREATE TABLE iatas (
    codigo_iata VARCHAR(3) PRIMARY KEY,
    pais VARCHAR(50) NOT NULL
);

CREATE TABLE cuentas (
    id_cuenta INT AUTO_INCREMENT PRIMARY KEY,
    banco VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE reservas (
    id_reserva INT AUTO_INCREMENT PRIMARY KEY,
    file CHAR(6) NOT NULL,
    estado VARCHAR(2) NOT NULL,
    moneda ENUM('P', 'D', 'L', 'B'),
    total DECIMAL(15, 2) NOT NULL,
    fecha_pago_proveedor DATE,
    fecha_in DATE,
    fecha_out DATE,
    fecha_sal DATE,
    id_proveedor int,
    id_pasajero int,
    codigo_iata VARCHAR(3) NOT NULL,
    hash CHAR(64) NOT NULL UNIQUE NOT NULL,
    FOREIGN KEY (id_proveedor) REFERENCES proveedores(id_proveedor),
    FOREIGN KEY (id_pasajero) REFERENCES pasajeros(id_pasajero),
    FOREIGN KEY (codigo_iata) REFERENCES iatas(codigo_iata)
);
 
CREATE TABLE saldos (
    id_saldo INT AUTO_INCREMENT PRIMARY KEY,
    codigo_transferencia VARCHAR(30),
    tipo_movimiento ENUM('I', 'E') NOT NULL,
    fecha_pago DATE,
    descripcion VARCHAR(150),
    moneda_pago ENUM('P', 'D', 'L', 'B') NOT NULL,
    monto DECIMAL(15, 2) NOT NULL,
    tipo_de_cambio DECIMAL(15, 2) NOT NULL,
    comision DECIMAL(15, 2),
    impuesto DECIMAL(15, 2),
    estado_pago ENUM('CANCELADO', 'PAGADO', 'PENDIENTE', 'UTILIZADO'),
    tipo_de_saldo VARCHAR(30),
    id_reserva int,
    id_cuenta int,
    FOREIGN KEY (id_reserva) REFERENCES reservas(id_reserva),
    FOREIGN KEY (id_cuenta) REFERENCES cuentas(id_cuenta)
);

-- ~duplicate
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

CREATE VIEW gabi_prevision AS
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
WHERE r.fecha_pago_proveedor >= CURDATE();

