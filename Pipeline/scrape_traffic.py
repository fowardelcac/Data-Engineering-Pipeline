from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import requests
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta


def main_scraper() -> pd.DataFrame:
    # Rutas
    URL_LOGIN = "https://traffic.welcomelatinamerica.com/iTraffic_TSA/Account/Login?ReturnUrl=%2fiTraffic_TSA%2f"
    URL_DATA = "https://traffic.welcomelatinamerica.com/iTraffic_TSA/Services/Z_Reportes/SaldoAutoriza_List/List"

    # Datos de login
    USERNAME = "Juancruz"
    PASSWORD = "southamerica123"

    FECHA_HOY = datetime.datetime.now()
    FECHA_TOP = FECHA_HOY + relativedelta(months=20)
    print("FECHAS")
    print(FECHA_HOY, FECHA_TOP)

    # 1️⃣ Inicializar Selenium
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")  # maximiza ventana
    options.add_argument("--headless")  # modo headless
    options.add_argument("--disable-gpu")  # recomendado en headless
    options.add_argument("--window-size=1920,1080")  # tamaño de la ventana

    driver = webdriver.Chrome(options=options)

    # 2️⃣ Abrir la página de login
    driver.get(URL_LOGIN)
    time.sleep(2)  # Esperar a que cargue

    # 3️⃣ Rellenar formulario
    driver.find_element(
        By.ID, "Softur_Serene_Membership_LoginPanel0_Username"
    ).send_keys(USERNAME)
    driver.find_element(
        By.ID, "Softur_Serene_Membership_LoginPanel0_Password"
    ).send_keys(PASSWORD)

    driver.find_element(
        By.ID, "Softur_Serene_Membership_LoginPanel0_LoginButton"
    ).click()
    time.sleep(3)

    # Cookies obtenidas de Selenium
    selenium_cookies = driver.get_cookies()
    cookies = {c["name"]: c["value"] for c in selenium_cookies}

    # Cabeceras
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://traffic.welcomelatinamerica.com",
        "Referer": URL_DATA,
        "User-Agent": "Mozilla/5.0",
    }

    session = requests.Session()
    all_data = []
    take = 500  # filas por request
    skip = 0

    while True:
        payload = {
            "Take": take,
            "Skip": skip,
            "EqualityFilter": {},
            "cod_oper": None,
            "cod_vdor": None,
            "tiposaldo": "",
            "tipocc": None,
            "moneda": "",
            "estadoRva": "",
            "fec_Compdesde": FECHA_HOY.strftime("%Y/%m/%d"),
            "fec_CompHasta": FECHA_TOP.strftime("%Y/%m/%d"),
        }

        response = session.post(
            URL_DATA, headers=headers, cookies=cookies, json=payload
        )

        if response.status_code != 200:
            print("Error:", response.status_code)
            break

        data = response.json().get("Entities", [])
        if not data:
            break  # No quedan más filas

        all_data.extend(data)
        skip += take
        print(f"Traído {len(data)} filas, total acumulado: {len(all_data)}")
    driver.quit()
    cols: list = [
        "rva",
        "estadoope",
        "monedalocal",
        "Fec_in",
        "Fec_out",
        "Descrip",
        "saldo",
        "nombre",
        "ciudad",
        "fec_sal",
        "fec_vencop",
    ]
    data = pd.DataFrame(all_data, columns=cols)

    columns = {
        "rva": "file",
        "estadoope": "estado",
        "monedalocal": "moneda",
        "Fec_in": "fecha_in",
        "Fec_out": "fecha_out",
        "fec_sal": "fecha_sal",
        "fec_vencop": "fecha_pago_proveedor",
        "Descrip": "pasajero",
        "saldo": "total",
        "nombre": "proveedor",
        "ciudad": "codigo_iata",
    }
    data.rename(columns=columns, inplace=True)
    return data
