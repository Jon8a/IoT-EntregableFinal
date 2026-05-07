"""
╔══════════════════════════════════════════════════════════════╗
║         MÓDULO 1 — CAPTURA DE DATOS                         ║
║         Sistema de Monitorización Climática Urbana           ║
║                                                              ║
║  Fuente:  Open-Meteo API (sin API key)                       ║
║  Ciudades: Madrid, Barcelona, Bilbao, Sevilla                ║
║  Modos:   - Carga histórica (batch)                          ║
║           - Captura en tiempo real (streaming simulado)      ║
╚══════════════════════════════════════════════════════════════╝
"""

import requests
import json
import time
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict

# ─────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
#  CONFIGURACIÓN CIUDADES
#  Cada ciudad es un diccionario con nombre, latitud y longitud.
#  Open-Meteo no usa nombres — solo coordenadas GPS.
# ─────────────────────────────────────────────────────────────
CIUDADES = [
    {"nombre": "Madrid",    "lat": 40.42, "lon": -3.70},
    {"nombre": "Barcelona", "lat": 41.39, "lon":  2.15},
    {"nombre": "Vitoria",   "lat": 42.85, "lon": -2.67},
    {"nombre": "Sevilla",   "lat": 37.39, "lon": -5.99},
]

# Variables meteorológicas que queremos capturar
VARIABLES_HISTORICO  = "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,surface_pressure"
VARIABLES_REALTIME   = "temperature_2m,precipitation,wind_speed_10m,relative_humidity_2m,surface_pressure"

# Endpoints de la API
URL_HISTORICO = "https://archive-api.open-meteo.com/v1/archive"
URL_REALTIME  = "https://api.open-meteo.com/v1/forecast"

# Cuántos segundos esperar entre peticiones en modo streaming
INTERVALO_STREAMING_SEG = 60  # cada 1 minuto


# ─────────────────────────────────────────────────────────────
#  DATACLASS — Registro climático
#
#  Un dataclass es básicamente una clase que solo sirve para
#  guardar datos. Python genera automáticamente __init__,
#  __repr__ y __eq__. Lo usamos para tener un tipo claro
#  para cada lectura del sensor, fácil de convertir a dict/JSON.
# ─────────────────────────────────────────────────────────────
@dataclass
class RegistroClimatico:
    ciudad:      str
    timestamp:   str       # ISO 8601 — ej: "2024-03-15T14:00"
    temperatura: float     # °C
    precipitacion: float   # mm
    viento:      float     # km/h
    humedad:     float     # %
    presion:     float     # hPa
    fuente:      str       # "historico" o "realtime"

    def a_dict(self) -> dict:
        """Convierte el registro a diccionario (útil para JSON/MQTT)."""
        return asdict(self)

    def a_json(self) -> str:
        """Convierte el registro a JSON string."""
        return json.dumps(self.a_dict(), ensure_ascii=False)


# ─────────────────────────────────────────────────────────────
#  FUNCIÓN AUXILIAR — Petición segura a la API
#
#  Toda llamada HTTP puede fallar (red caída, servidor ocupado,
#  límite de peticiones, etc). Esta función centraliza el manejo
#  de errores con reintentos para que el resto del código
#  no tenga que preocuparse de ello.
# ─────────────────────────────────────────────────────────────
def _get_api(url: str, params: dict, reintentos: int = 3) -> dict | None:
    """
    Hace GET a la API con reintentos automáticos.
    Devuelve el JSON parseado o None si falla tras todos los reintentos.
    """
    for intento in range(1, reintentos + 1):
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()  # lanza excepción si status >= 400
            return response.json()

        except requests.exceptions.Timeout:
            log.warning(f"  Timeout en intento {intento}/{reintentos} → {url}")

        except requests.exceptions.HTTPError as e:
            log.error(f"  Error HTTP {e.response.status_code}: {e}")
            return None  # errores HTTP no tienen sentido reintentarlos

        except requests.exceptions.ConnectionError:
            log.warning(f"  Sin conexión en intento {intento}/{reintentos}")

        except Exception as e:
            log.error(f"  Error inesperado: {e}")
            return None

        if intento < reintentos:
            espera = 2 ** intento  # espera exponencial: 2s, 4s, 8s...
            log.info(f"  Reintentando en {espera}s...")
            time.sleep(espera)

    log.error(f"  Falló tras {reintentos} intentos.")
    return None


# ─────────────────────────────────────────────────────────────
#  MODO 1 — CARGA HISTÓRICA
#
#  Pide a la API todos los datos horarios de un rango de fechas.
#  Útil para poblar la base de datos desde el primer arranque.
#  Con 1 año × 4 ciudades obtenemos ~35.000 registros.
# ─────────────────────────────────────────────────────────────
def capturar_historico(
    fecha_inicio: str,
    fecha_fin:    str,
    ciudades:     list = CIUDADES
) -> list[RegistroClimatico]:
    """
    Descarga datos históricos horarios para todas las ciudades.

    Args:
        fecha_inicio: "YYYY-MM-DD"
        fecha_fin:    "YYYY-MM-DD"
        ciudades:     lista de dicts con nombre/lat/lon

    Returns:
        Lista de RegistroClimatico
    """
    log.info("=" * 60)
    log.info("  MODO: Carga histórica")
    log.info(f"  Periodo: {fecha_inicio} → {fecha_fin}")
    log.info(f"  Ciudades: {[c['nombre'] for c in ciudades]}")
    log.info("=" * 60)

    todos_los_registros = []

    for ciudad in ciudades:
        log.info(f"\n  Descargando {ciudad['nombre']}...")

        params = {
            "latitude":   ciudad["lat"],
            "longitude":  ciudad["lon"],
            "start_date": fecha_inicio,
            "end_date":   fecha_fin,
            "hourly":     VARIABLES_HISTORICO,
            "timezone":   "Europe/Madrid",
        }

        datos = _get_api(URL_HISTORICO, params)
        if datos is None:
            log.error(f"  No se pudieron obtener datos de {ciudad['nombre']}")
            continue

        # La API devuelve listas paralelas: un índice = una hora
        # data["hourly"]["time"][i] corresponde a data["hourly"]["temperature_2m"][i]
        horario = datos["hourly"]
        n = len(horario["time"])

        registros_ciudad = []
        for i in range(n):
            # Algunos valores pueden ser None (sensor sin dato en esa hora)
            # Los filtramos para no meter datos sucios en la BD
            temp    = horario["temperature_2m"][i]
            precip  = horario["precipitation"][i]
            viento  = horario["wind_speed_10m"][i]
            humedad = horario["relative_humidity_2m"][i]
            presion = horario["surface_pressure"][i]

            if None in (temp, precip, viento, humedad, presion):
                continue  # descartamos el registro incompleto

            registro = RegistroClimatico(
                ciudad        = ciudad["nombre"],
                timestamp     = horario["time"][i],
                temperatura   = temp,
                precipitacion = precip,
                viento        = viento,
                humedad       = humedad,
                presion       = presion,
                fuente        = "historico",
            )
            registros_ciudad.append(registro)

        log.info(f"  ✅ {ciudad['nombre']}: {len(registros_ciudad)} registros obtenidos")
        todos_los_registros.extend(registros_ciudad)

    log.info(f"\n  TOTAL histórico: {len(todos_los_registros)} registros")
    return todos_los_registros


# ─────────────────────────────────────────────────────────────
#  MODO 2 — CAPTURA EN TIEMPO REAL (streaming simulado)
#
#  Llama a la API periódicamente para obtener el dato actual.
#  Simula el comportamiento de un sensor IoT físico.
#  En el proyecto final, en lugar de imprimir, enviará
#  el dato por MQTT al broker (Módulo 2).
# ─────────────────────────────────────────────────────────────
def capturar_realtime(
    ciudades:  list = CIUDADES,
    intervalo: int  = INTERVALO_STREAMING_SEG,
    max_ciclos: int = None   # None = infinito; número = para N ciclos (útil en tests)
) -> None:
    """
    Captura datos en tiempo real de forma periódica.
    Cada 'intervalo' segundos obtiene el estado actual de cada ciudad.

    Args:
        ciudades:   lista de ciudades a monitorizar
        intervalo:  segundos entre cada ronda de capturas
        max_ciclos: None para loop infinito, o un entero para parar
    """
    log.info("=" * 60)
    log.info("  MODO: Tiempo real (streaming)")
    log.info(f"  Intervalo: cada {intervalo}s")
    log.info(f"  Ciudades: {[c['nombre'] for c in ciudades]}")
    log.info("  Ctrl+C para detener")
    log.info("=" * 60)

    ciclo = 0
    try:
        while True:
            ciclo += 1
            log.info(f"\n  ── Ciclo #{ciclo} · {datetime.now().strftime('%H:%M:%S')} ──")

            for ciudad in ciudades:
                params = {
                    "latitude":  ciudad["lat"],
                    "longitude": ciudad["lon"],
                    "current":   VARIABLES_REALTIME,
                    "timezone":  "Europe/Madrid",
                }

                datos = _get_api(URL_REALTIME, params)
                if datos is None:
                    log.error(f"  Error al obtener {ciudad['nombre']}")
                    continue

                current = datos["current"]

                registro = RegistroClimatico(
                    ciudad        = ciudad["nombre"],
                    timestamp     = current["time"],
                    temperatura   = current["temperature_2m"],
                    precipitacion = current["precipitation"],
                    viento        = current["wind_speed_10m"],
                    humedad       = current["relative_humidity_2m"],
                    presion       = current["surface_pressure"],
                    fuente        = "realtime",
                )

                # ── AQUÍ irá el envío MQTT en el Módulo 2 ──────────────
                # mqtt_client.publish("clima/datos", registro.a_json())
                # ───────────────────────────────────────────────────────

                # Por ahora: mostramos el dato por consola
                log.info(
                    f"  📡 {registro.ciudad:<12} "
                    f"{registro.temperatura:>5.1f}°C  "
                    f"💧{registro.precipitacion:.1f}mm  "
                    f"💨{registro.viento:.1f}km/h  "
                    f"💦{registro.humedad:.0f}%"
                )

            if max_ciclos and ciclo >= max_ciclos:
                log.info(f"\n  Completados {max_ciclos} ciclos. Fin.")
                break

            log.info(f"\n  Esperando {intervalo}s para el siguiente ciclo...")
            time.sleep(intervalo)

    except KeyboardInterrupt:
        log.info("\n  Captura detenida por el usuario.")


# ─────────────────────────────────────────────────────────────
#  FUNCIÓN DE DEMO — muestra un subconjunto de los datos
# ─────────────────────────────────────────────────────────────
def mostrar_muestra(registros: list[RegistroClimatico], n: int = 5):
    """Imprime los primeros n registros de forma legible."""
    print(f"\n{'─'*70}")
    print(f"  Muestra de datos ({min(n, len(registros))} de {len(registros)} registros)")
    print(f"{'─'*70}")
    print(f"  {'Ciudad':<12} {'Timestamp':<18} {'Temp':>6} {'Prec':>6} {'Viento':>8} {'Hum':>5} {'Pres':>8}")
    print(f"  {'─'*12} {'─'*18} {'─'*6} {'─'*6} {'─'*8} {'─'*5} {'─'*8}")
    for r in registros[:n]:
        print(
            f"  {r.ciudad:<12} {r.timestamp:<18} "
            f"{r.temperatura:>5.1f}° {r.precipitacion:>5.1f}mm "
            f"{r.viento:>6.1f}km/h {r.humedad:>4.0f}% "
            f"{r.presion:>7.1f}hPa"
        )
    print(f"{'─'*70}\n")


# ─────────────────────────────────────────────────────────────
#  MAIN — Punto de entrada
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    # Permitimos elegir el modo por argumento de línea de comandos:
    #   python captura.py historico   → carga histórica del último mes
    #   python captura.py realtime    → streaming en tiempo real
    #   python captura.py             → demo rápida (por defecto)

    modo = sys.argv[1] if len(sys.argv) > 1 else "demo"

    if modo == "historico":
        # Descarga el último año completo
        fin    = (datetime.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        inicio = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        registros = capturar_historico(inicio, fin)
        mostrar_muestra(registros, n=10)

        # Guardar en JSON (en el proyecto final irá a InfluxDB)
        with open("datos_historicos.json", "w", encoding="utf-8") as f:
            json.dump([r.a_dict() for r in registros], f, ensure_ascii=False, indent=2)
        log.info(f"  💾 Guardado en datos_historicos.json ({len(registros)} registros)")

    elif modo == "realtime":
        # Streaming continuo (Ctrl+C para parar)
        capturar_realtime(intervalo=60)

    else:
        # Demo rápida: 3 días históricos + 1 ciclo realtime
        log.info("  DEMO RÁPIDA")
        fin    = (datetime.today() - timedelta(days=5)).strftime("%Y-%m-%d")
        inicio = (datetime.today() - timedelta(days=8)).strftime("%Y-%m-%d")

        log.info("\n  [1/2] Carga histórica (últimos 3 días)...")
        registros = capturar_historico(inicio, fin)
        mostrar_muestra(registros, n=8)

        log.info("\n  [2/2] Un ciclo de captura en tiempo real...")
        capturar_realtime(max_ciclos=1, intervalo=0)
