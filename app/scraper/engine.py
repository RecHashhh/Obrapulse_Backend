import os
import re
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

VALOR_MIN_OBRAS = float(os.getenv("VALOR_MIN_OBRAS", "5000000"))
VALOR_MIN_CONSULTORIA = float(os.getenv("VALOR_MIN_CONSULTORIA", "1000000"))
PROCEDIMIENTOS_VALIDOS = ["Licitación", "Lista Corta"]
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

COLUMNAS_TABLA = [
    "Nro.", "Partida Pres.", "CPC", "T. Compra", "T. Régimen", "Fondo BID",
    "Tipo de Presupuesto", "Tipo de Producto", "Cat. Electrónico", "Procedimiento",
    "Descripción", "Cant.", "U. Medida", "Costo U.", "V. Total", "Extra", "Período"
]

COLUMNAS_PARTIDAS = [
    "Nro", "Partida_Pres", "CPC", "T_Compra", "T_Regimen", "Fondo_BID",
    "Tipo_Presupuesto", "Tipo_Producto", "Cat_Electronico", "Procedimiento",
    "Descripcion", "Cantidad", "Unidad_Medida", "Costo_Unitario", "V_Total",
    "Extra", "Periodo",
]


def identificar_organismo(texto):
    if pd.isna(texto):
        return "Directo"
    texto = str(texto).upper()
    if "BID" in texto or "OC-" in texto or "EC-L" in texto:
        return "BID"
    if "BANCO MUNDIAL" in texto:
        return "Banco Mundial"
    if "CAF" in texto:
        return "CAF"
    if "AFD" in texto:
        return "AFD"
    return "Directo"


def es_infraestructura(texto):
    if pd.isna(texto):
        return False
    texto = str(texto).lower()
    keywords = [
        "obra", "construcción", "construccion", "mantenimiento", "rehabilitación",
        "rehabilitacion", "vial", "edificio", "infraestructura", "puente", "carretera",
        "alcantarillado", "agua potable", "hidráulica", "hidraulica", "urbanización",
        "urbanizacion", "hospital", "escuela"
    ]
    return any(k in texto for k in keywords)


def crear_driver():
    chrome_options = webdriver.ChromeOptions()
    if HEADLESS:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options,
    )
    return driver


def ejecutar_scraping(df_empresas: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe un DataFrame con columnas: Nombre Comercial, Url, Provincia, Ciudad.
    Retorna un DataFrame con todas las partidas scrapeadas.
    """
    driver = crear_driver()
    detalle_procesos = []
    detalle_partidas = []

    try:
        for _, row in df_empresas.iterrows():
            url = row["Url"]
            nombre_comercial = row["Nombre Comercial"]
            provincia_bdd = row.get("Provincia", None)
            ciudad_bdd = row.get("Ciudad", None)

            print(f"\nProcesando: {nombre_comercial}")

            try:
                driver.get(url)
                wait = WebDriverWait(driver, 60)
                wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, "table")))
                tables = driver.find_elements(By.TAG_NAME, "table")

                html_completo = driver.page_source
                soup_full = BeautifulSoup(html_completo, "html.parser")
                entidad_tag = soup_full.find(string=re.compile("Entidad:", re.IGNORECASE))
                entidad = (
                    entidad_tag.find_next("label").text.strip()
                    if entidad_tag
                    else "No encontrado"
                )

                try:
                    table8 = wait.until(
                        EC.presence_of_element_located((By.XPATH, "(//table)[8]"))
                    )
                    table7 = wait.until(
                        EC.presence_of_element_located((By.XPATH, "(//table)[7]"))
                    )

                    soup_proc = BeautifulSoup(
                        table8.get_attribute("outerHTML") + table7.get_attribute("outerHTML"),
                        "html.parser",
                    )

                    rows = soup_proc.find_all(
                        "tr",
                        class_=lambda x: x and ("filaElemento1" in x or "filaElemento2" in x),
                    )

                    data = []
                    seen = set()
                    for r in rows:
                        cells = r.find_all("td")
                        values = [c.get_text(strip=True) for c in cells]
                        if values and values[0] not in seen:
                            seen.add(values[0])
                            while len(values) < len(COLUMNAS_TABLA):
                                values.append("")
                            data.append(values[: len(COLUMNAS_TABLA)])

                    df_proc = pd.DataFrame(data, columns=COLUMNAS_TABLA)

                    if not df_proc.empty:
                        df_proc["Nro."] = pd.to_numeric(df_proc["Nro."], errors="coerce")
                        df_proc = df_proc.sort_values(by="Nro.").reset_index(drop=True)
                        df_proc["V_Total_Numeric"] = df_proc["V. Total"].str.replace(
                            r"[^\d.]", "", regex=True
                        )
                        df_proc["V_Total_Numeric"] = pd.to_numeric(
                            df_proc["V_Total_Numeric"], errors="coerce"
                        ).fillna(0)
                        df_proc["Organismo"] = df_proc["Fondo BID"].apply(identificar_organismo)
                        df_proc["Tipo_Financiamiento"] = df_proc["Organismo"].apply(
                            lambda x: "Multilateral" if x != "Directo" else "Directo"
                        )
                        df_proc["Es_Infraestructura"] = df_proc["Descripción"].apply(
                            es_infraestructura
                        )
                        df_proc["Tipo_Tabla"] = "Procesos"
                        df_proc["Entidad"] = entidad
                        df_proc["url"] = url
                        df_proc["Nombre_Comercial"] = nombre_comercial
                        df_proc["Provincia"] = provincia_bdd
                        df_proc["Ciudad"] = ciudad_bdd
                        df_proc["Fecha_Carga"] = datetime.now().strftime("%Y-%m-%d")
                        detalle_procesos.append(df_proc)
                except Exception:
                    print("⚠️ No se pudo extraer Procesos")

                tabla_partidas_html = None
                for table in tables:
                    html = table.get_attribute("outerHTML")
                    if "Partida Pres." in html and "V. Total" in html and "Procedimiento" in html:
                        tabla_partidas_html = html
                        break

                if tabla_partidas_html:
                    soup_part = BeautifulSoup(tabla_partidas_html, "html.parser")
                    rows_part = soup_part.find_all(
                        "tr",
                        class_=lambda x: x and ("filaElemento1" in x or "filaElemento2" in x),
                    )

                    data_part = []
                    for r in rows_part:
                        cells = r.find_all("td")
                        values = [c.get_text(strip=True) for c in cells]
                        if values:
                            data_part.append(values)

                    df_part = pd.DataFrame(data_part, columns=COLUMNAS_PARTIDAS)
                    if not df_part.empty:
                        df_part["V_Total_Numeric"] = df_part["V_Total"].str.replace(
                            r"[^\d.]", "", regex=True
                        )
                        df_part["V_Total_Numeric"] = pd.to_numeric(
                            df_part["V_Total_Numeric"], errors="coerce"
                        ).fillna(0)
                        df_part["Tipo_Tabla"] = "Partidas"
                        df_part["Entidad"] = entidad
                        df_part["url"] = url
                        df_part["Nombre_Comercial"] = nombre_comercial
                        df_part["Provincia"] = provincia_bdd
                        df_part["Ciudad"] = ciudad_bdd
                        df_part["Fecha_Carga"] = datetime.now().strftime("%Y-%m-%d")
                        detalle_partidas.append(df_part)

            except Exception as e:
                print(f"❌ Error procesando {url}: {e}")
    finally:
        driver.quit()

    df_partidas_final = (
        pd.concat(detalle_partidas, ignore_index=True) if detalle_partidas else pd.DataFrame()
    )

    print(f"\nScraping finalizado ✅ — Total partidas: {len(df_partidas_final)}")
    return df_partidas_final


def filtrar_obras(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        (df["T_Compra"].fillna("").str.contains("obra", case=False, na=False))
        & (df["Procedimiento"].isin(PROCEDIMIENTOS_VALIDOS))
        & (pd.to_numeric(df["V_Total_Numeric"], errors="coerce").fillna(0) >= VALOR_MIN_OBRAS)
    ].copy()


def filtrar_consultoria(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        (df["T_Compra"].fillna("").str.contains("consult", case=False, na=False))
        & (pd.to_numeric(df["V_Total_Numeric"], errors="coerce").fillna(0) >= VALOR_MIN_CONSULTORIA)
    ].copy()
