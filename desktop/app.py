import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import sys
import os
import requests

# Agrega la carpeta raíz (Test) al sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from Pipeline.etl_traffic import main_traffic
from Pipeline.etl_excel import main_excel


class ConsoleRedirect:
    """Redirige stdout/stderr a un widget Text de tkinter"""

    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, msg):
        self.text_widget.insert(tk.END, msg)
        self.text_widget.see(tk.END)  # autoscroll al final

    def flush(self):
        pass  # necesario para compatibilidad con sys.stdout


def joker() -> str:
    """Obtiene un chiste aleatorio desde la API"""
    try:
        resp = requests.get(
            "https://official-joke-api.appspot.com/random_joke", timeout=5
        )
        if resp.status_code == 200:
            joke = resp.json()
            return f"{joke['setup']} - {joke['punchline']}"
    except requests.RequestException:
        pass
    return "No se pudo obtener un chiste. Espera un momento..."


def run_traffic():
    joke_text = joker()
    joke_label.config(text=joke_text)
    main_traffic()
    status_label.config(text="✅ ETL-Traffic finalizado")


def run_excel():
    joke_text = joker()
    joke_label.config(text=joke_text)  # Muestra el chiste en la pestaña
    main_excel()
    status_label.config(text="✅ Actualización finalizada")


# --- Ventana principal ---
root = tk.Tk()
root.title("Pipeline - TSA trips")
root.geometry("800x600")

notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True)

# --- Pestaña 1: Botones ---
frame_buttons = ttk.Frame(notebook)

btn1 = tk.Button(frame_buttons, text="ETL-Traffic", command=run_traffic)
btn1.pack(pady=10)

btn2 = tk.Button(frame_buttons, text="Actualizar Excel", command=run_excel)
btn2.pack(pady=10)

status_label = tk.Label(frame_buttons, text="Esperando acción...")
status_label.pack(pady=10)

joke_label = tk.Label(
    frame_buttons,
    text="",
    wraplength=600,
    justify="center",
    font=("Arial", 12),
    fg="blue",
)
joke_label.pack(pady=20)

notebook.add(frame_buttons, text="Acciones")

# --- Pestaña 2: Consola ---
frame_logs = ttk.Frame(notebook)
log_text = scrolledtext.ScrolledText(frame_logs, wrap=tk.WORD, state="normal")
log_text.pack(fill="both", expand=True)
notebook.add(frame_logs, text="Logs")

# Redirigimos stdout/stderr a la pestaña de logs
sys.stdout = ConsoleRedirect(log_text)
sys.stderr = ConsoleRedirect(log_text)

root.mainloop()
