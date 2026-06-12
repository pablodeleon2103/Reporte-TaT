"""
actualizar_reporte.py
=====================
Descarga el CSV desde nueva.percapita.mx, calcula todos los
indicadores del reporte mensual de TaT y actualiza index.html.

Corre en GitHub Actions — no necesita config.txt ni token.

Cambios v2 (jun 2026):
- Fix typo loanAceppped → loanAceppted (estaba contando 0)
- Deduplicación por ID Crédito (clave única real)
- Timeout 300s + 3 reintentos en descarga CSV
- TaT desglosado por sub-etapas (revisión, operación, rechazo)
- Motivos de rechazo, ratio Monto Autorizado/Solicitado, mapa por Estado Residencia
"""

import pandas as pd
import numpy as np
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO

# ══════════════════════════════════════════════════════
# CONFIGURACIÓN
# ══════════════════════════════════════════════════════

CSV_URL        = os.environ.get('CSV_URL', 'https://nueva.percapita.mx/api/creditos/reporte/export/csv')
DASHBOARD_HTML = Path(__file__).parent / 'index_base.html'
LOG_FILE       = Path(__file__).parent / 'actualizaciones.log'
TAT_THRESHOLD  = 200

FESTIVOS = {datetime(y,m,d) for y,m,d in [
    (2025,1,1),(2025,2,3),(2025,3,17),(2025,4,17),(2025,4,18),(2025,5,1),
    (2025,9,16),(2025,11,17),(2025,12,25),
    (2026,1,1),(2026,2,2),(2026,3,16),(2026,4,2),(2026,4,3),(2026,5,1),
    (2026,9,16),(2026,11,16),(2026,12,25),
    (2027,1,1),(2027,2,1),(2027,3,15),(2027,4,1),(2027,4,2),(2027,5,1),
]}

MESES_ES  = ['Enero','Febrero','Marzo','Abril','Mayo','Junio',
             'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']
MESES_CRT = ['Ene','Feb','Mar','Abr','May','Jun',
             'Jul','Ago','Sep','Oct','Nov','Dic']
DIAS_ES   = ['Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo']

PROD_LABEL  = {'TITULO':'Título','HIBRIDO':'Híbrido',
               'RENOVACIONTITULO':'Reno. Título','RENOVACIONHIBRIDO':'Reno. Híbrido'}
PROD_COLORS = {'TITULO':'#1a5e9e','HIBRIDO':'#1e7347',
               'RENOVACIONTITULO':'#c85a17','RENOVACIONHIBRIDO':'#c97c00'}
PROD_ORDER  = ['TITULO','HIBRIDO','RENOVACIONTITULO','RENOVACIONHIBRIDO']
# Nota: RENOVACIONMIXTO entra al dataset como cualquier renovación,
# pero no se desglosa como cubo separado (decisión Pablo, jun 2026).

STEP_LABEL = {
    'crearUsuario':'Crear usuario','datosPersona':'Datos personales',
    'telefonoPersona':'Teléfono','generaCURP':'Validación CURP',
    'datosDomicilio':'Datos domicilio','datosEmpleo':'Datos empleo',
    'datosReferenciaPersonal':'Referencias','evaluacionProceso':'Evaluación proceso',
    'medioEntrega':'Medio de entrega','procesoPruebaDeVida':'Prueba de vida',
    'loanAceppted':'Crédito aceptado','enEsperaDispersion':'En espera dispersión',
    'inicio':'Dispersión iniciada','loanRejected':'Rechazado sistema',
    'prestamoCanceladoUsuario':'Cancelado por cliente',
    'actualizarCuentaBancaria':'Actualizar cuenta',
}

# Labels legibles para motivos de rechazo
MOTIVO_LABEL = {
    'dictamen_rechazado':  'Dictamen rechazado',
    'credito_activo_zell': 'Crédito activo en Zell',
}

# ══════════════════════════════════════════════════════
# UTILIDADES
# ══════════════════════════════════════════════════════

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linea = f"[{ts}] {msg}"
    print(linea, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(linea + "\n")
    except Exception:
        pass


def biz_hours(start, end):
    if pd.isna(start) or pd.isna(end): return np.nan
    start = pd.Timestamp(start); end = pd.Timestamp(end)
    if end <= start: return 0.0
    total = 0.0; cur = start
    while cur.date() <= end.date():
        if cur.weekday() < 5 and datetime(cur.year,cur.month,cur.day) not in FESTIVOS:
            s  = cur.replace(hour=9,  minute=0, second=0, microsecond=0)
            e  = cur.replace(hour=18, minute=0, second=0, microsecond=0)
            ds = max(cur if cur.date()==start.date() else s, s)
            de = min(end if cur.date()==end.date() else e, e)
            if de > ds: total += (de-ds).total_seconds() / 3600
        cur = (cur+timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)
    return total


def r2(v): return round(float(v), 2) if not np.isnan(v) else 0.0
def r1(v): return round(float(v), 1) if not np.isnan(v) else 0.0

def tat_med(serie):
    v = serie.dropna(); v = v[v <= TAT_THRESHOLD]
    return r2(v.median()) if len(v) else 0.0

def tat_p90(serie):
    v = serie.dropna(); v = v[v <= TAT_THRESHOLD]
    return r2(v.quantile(.9)) if len(v) else 0.0

def mes_largo(periodo):
    t = periodo.to_timestamp()
    return f"{MESES_ES[t.month-1]} {t.year}"

def mes_corto(periodo):
    t = periodo.to_timestamp()
    return f"{MESES_CRT[t.month-1]} {str(t.year)[-2:]}"

def fecha_corte(periodo):
    t = periodo.to_timestamp()
    # Último día del mes
    import calendar
    ultimo = calendar.monthrange(t.year, t.month)[1]
    return f"{ultimo} {MESES_CRT[t.month-1]} {t.year}"

# ══════════════════════════════════════════════════════
# PASO 1 — DESCARGAR CSV (con reintentos)
# ══════════════════════════════════════════════════════

def descargar_csv(url):
    """Descarga el CSV con reintentos automáticos.
    Si el servidor de Percapita está lento, espera y reintenta.
    """
    import requests
    intentos_max = 3
    timeout_seg  = 300  # 5 minutos por intento

    for intento in range(1, intentos_max + 1):
        try:
            log(f"Descargando CSV (intento {intento}/{intentos_max}) desde {url} ...")
            resp = requests.get(url, timeout=timeout_seg)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text), encoding="utf-8")
            log(f"  ✓ {len(df):,} filas descargadas")
            return df
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError) as e:
            log(f"  ⚠️ Intento {intento} falló: {type(e).__name__}")
            if intento < intentos_max:
                espera = 30 * intento  # 30s, 60s
                log(f"     Esperando {espera}s antes de reintentar...")
                time.sleep(espera)
            else:
                log(f"  ❌ Todos los intentos fallaron")
                raise


# ══════════════════════════════════════════════════════
# PASO 2B — DESCARGAR BASE DE CONTROL (Google Sheets)
# ══════════════════════════════════════════════════════

SHEET_ID = os.environ.get('SHEET_ID', '11fk_9Vl8CBNVW1cDtZL5GEOjuV9VM7K8dugi5rTimJE')

def descargar_base_control():
    """Descarga la Base de Control desde Google Sheets como CSV."""
    import requests
    from io import StringIO
    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQRJ6r-1yEdDh-ZwYa6WiQuiYSyaq4mqEfWw1Zhez8ERhzIOYvK2teCKJMs8DVdD1O0JAPMTUU3bpaI/pub?gid=1318178910&single=true&output=csv"
    log("Descargando Base de Control desde Google Sheets...")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text), encoding='utf-8')

        # Limpiar nombres de columnas (quitar espacios)
        df.columns = df.columns.str.strip()

        # La primera columna contiene la fecha pero su nombre varía
        # (a veces es un número de teléfono u otro valor del encabezado)
        # La renombramos a FECHA siempre
        col_fecha = df.columns[0]
        df = df.rename(columns={col_fecha: 'FECHA'})

        # Parsear fechas — el formato es DD/MM/YYYY HH:MM:SS
        df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce', dayfirst=True)

        # Filtrar filas sin fecha válida (encabezados extra, filas vacías)
        df = df[df['FECHA'].notna()].copy()

        # Normalizar columnas clave para búsquedas robustas
        for col in ['CANAL', 'RESULTADO', 'MOTIVO DE CONTACTO', 'ASESOR']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        log(f"  ✓ Base de Control: {len(df):,} registros | "
            f"Canales: {df['CANAL'].nunique() if 'CANAL' in df.columns else '?'} | "
            f"Resultados: {df['RESULTADO'].nunique() if 'RESULTADO' in df.columns else '?'}")
        return df
    except Exception as e:
        log(f"  ⚠️ No se pudo descargar Base de Control: {e}")
        return None


def calcular_indicadores_control(bc, mes_actual, mes_anterior):
    """Calcula indicadores de la Base de Control para el reporte."""
    if bc is None or len(bc) == 0:
        return None

    # Filtrar por mes actual y anterior
    bc['mes'] = bc['FECHA'].dt.to_period('M')

    # Excluir registros etiquetados como "Renovación (sin historial)"
    # Son clientes nuevos que en realidad son renovaciones viejas — distorsionan los tiempos
    col_resultado = 'RESULTADO' if 'RESULTADO' in bc.columns else None
    if col_resultado:
        bc = bc[bc[col_resultado] != 'Renovación (sin historial)'].copy()
        log(f"  ✓ Base de Control filtrada — excluidos 'Renovación (sin historial)'")

    dm = bc[bc['mes'] == mes_actual].copy()
    dp = bc[bc['mes'] == mes_anterior].copy()

    if len(dm) == 0:
        log("  ⚠️ Sin datos de Base de Control para el mes actual")
        return None

    def dist(d, col):
        if col not in d.columns: return []
        total = len(d)
        return [{'label': str(k), 'n': int(v), 'pct': round(v/total*100,1)}
                for k,v in d[col].value_counts().head(10).items()]

    # Canales
    canales_act = dist(dm, 'CANAL')
    canales_ant = dist(dp, 'CANAL')

    # Motivos
    motivos_act = dist(dm, 'MOTIVO DE CONTACTO')

    # Resultados
    resultados_act = dist(dm, 'RESULTADO')

    # Por asesor
    asesores = []
    if 'ASESOR' in dm.columns:
        for asesor, g in dm.groupby('ASESOR'):
            if pd.isna(asesor): continue
            total_a = len(g)
            res = g['RESULTADO'].value_counts() if 'RESULTADO' in g.columns else pd.Series()
            apoyo = int(res.get('Apoyo a solicitar', 0)) + int(res.get('Apoyo a cerrar', 0))
            asesores.append({
                'asesor':    str(asesor),
                'total':     total_a,
                'apoyo':     apoyo,
                'pct_apoyo': round(apoyo/total_a*100,1) if total_a else 0,
            })
        asesores.sort(key=lambda x: x['total'], reverse=True)

    # ── Métricas de atacables gestionados por Dashboard ──
    dashboard_metricas = {'exitoso': 0, 'declinado': 0, 'en_proceso': 0, 'total': 0}
    if col_resultado and 'RESULTADO' in dm.columns:
        dashboard_metricas['exitoso']    = int((dm['RESULTADO'] == 'Dashboard Exitoso').sum())
        dashboard_metricas['declinado']  = int((dm['RESULTADO'] == 'Dashboard Declinado').sum())
        dashboard_metricas['en_proceso'] = int((dm['RESULTADO'] == 'Dashboard en proceso').sum())
        dashboard_metricas['total']      = (dashboard_metricas['exitoso'] +
                                             dashboard_metricas['declinado'] +
                                             dashboard_metricas['en_proceso'])

    # Totales comparativos
    return {
        'total_act':          len(dm),
        'total_ant':          len(dp),
        'canales_act':        canales_act,
        'canales_ant':        canales_ant,
        'motivos_act':        motivos_act,
        'resultados_act':     resultados_act,
        'asesores':           asesores,
        'dashboard_metricas': dashboard_metricas,
    }

# ══════════════════════════════════════════════════════
# PASO 2 — PREPARAR BASE (filtros v8)
# ══════════════════════════════════════════════════════

def preparar_base(df):
    # Mapeo extendido — ahora capturamos columnas nuevas:
    # id_credito (dedupe), fechas de etapas, monto solicitado, motivo rechazo,
    # estado/municipio (mapa geográfico).
    MAPEO = {
        'ID Crédito':       'id_credito',
        'Nombres':          'nombres',
        'Fecha Solicitud':  'fecha_solicitud',
        'Fecha Dictamen':   'fecha_dictamen',
        'Fecha Aceptación': 'fecha_aceptacion',
        'Fecha Dispersión': 'fecha_dispersion',
        'Fecha Rechazo':    'fecha_rechazo',
        'Tipo Crédito':     'tipo_crediticio',
        'Monto':            'monto_solicitado',
        'Monto Autorizado': 'monto_autorizado',
        'Estado Solicitud': 'estado_solicitud',
        'Motivo Rechazo':   'motivo_rechazo',
        'Step':             'step',
        'Estatus':          'estatus',
        'Ciudad Título':    'ciudad_titulo',
        'Estado Residencia':'estado_residencia',
        'Municipio':        'municipio',
        'Título':           'titulo',
    }
    df = df.rename(columns=MAPEO)

    # Parsear todas las fechas de ciclo de vida.
    # IMPORTANTE: el CSV mezcla dos formatos ISO:
    #   formato completo:   '2026-06-03T12:26:56'  (~98% de filas)
    #   formato incompleto: '2026-06-03T12:28'     (~2% de filas, sin segundos)
    # Sin format='mixed', pandas infiere UN solo formato y descarta el otro como NaT.
    # Eso causaba que el reporte ignorara ~98% de los registros silenciosamente.
    for col in ['fecha_solicitud','fecha_dictamen','fecha_aceptacion',
                'fecha_dispersion','fecha_rechazo']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')

    # Defensa contra fila de header duplicada en CSV malformado
    df = df[df['tipo_crediticio'] != 'tipo_crediticio'].copy()

    # Excluir productos que no analizamos en este reporte
    df = df[~df['tipo_crediticio'].isin(['PERSONAL','RENOVACIONPERSONAL'])].copy()

    # Excluir steps que son artefactos del sistema (no etapas reales)
    df = df[~df['step'].isin(['solicitudPrestamoTitulo','solicitudPrestamoPersonal'])].copy()

    # Ventana de análisis
    df = df[df['fecha_solicitud'] >= '2025-08-01'].copy()

    # ── DEDUPLICACIÓN POR CLAVE ÚNICA REAL ──
    # Antes: drop_duplicates() comparaba todas las columnas.
    # Con el CSV nuevo (27 columnas, algunas mutables entre snapshots)
    # eso dejaba pasar duplicados. Ahora deduplicamos por ID Crédito
    # quedándonos con el snapshot más reciente.
    if 'id_credito' in df.columns:
        antes = len(df)
        df = df.drop_duplicates(subset=['id_credito'], keep='last')
        log(f"  ✓ Dedupe por ID Crédito: {antes:,} → {len(df):,} registros")
    else:
        # Fallback si por alguna razón no viene ID Crédito
        df = df.drop_duplicates()
        log(f"  ⚠️ Sin ID Crédito — usando dedupe genérico")

    df['mes'] = df['fecha_solicitud'].dt.to_period('M')

    PASO_ORDEN = {
        'crearUsuario':1,'datosPersona':2,'telefonoPersona':3,'generaCURP':4,
        'datosDomicilio':5,'datosEmpleo':6,'datosReferenciaPersonal':7,
        'evaluacionProceso':9,'medioEntrega':10,'procesoPruebaDeVida':11,
        'loanRejected':12,'loanAceppted':13,'enEsperaDispersion':14,
        'prestamoCanceladoUsuario':14,'inicio':15,
    }
    df['paso_num'] = df['step'].map(PASO_ORDEN).fillna(0)

    # ── DEDUPE SECUNDARIO POR TÍTULO+TIPO ──
    # Captura casos donde el cliente hizo varios intentos y solo uno fue exitoso.
    # Prefiere el registro completado (creditoAperturado).
    dup_mask = df.duplicated(subset=['titulo','tipo_crediticio'], keep=False)
    df_unicos = df[~dup_mask].copy()
    elegidos = []
    for (t,tp), g in df[dup_mask].groupby(['titulo','tipo_crediticio']):
        comp = g[g['estatus']=='creditoAperturado']
        if   len(comp)==0: elegidos.append(g.sort_values(['paso_num','fecha_solicitud'],ascending=[False,False]).iloc[[0]])
        elif len(comp)==1: elegidos.append(comp.iloc[[0]])
        else:              elegidos.append(comp)
    if elegidos:
        df = pd.concat([df_unicos, pd.concat(elegidos,ignore_index=True)],ignore_index=True)

    log(f"  ✓ {len(df):,} registros tras filtros v8")
    return df.reset_index(drop=True)

# ══════════════════════════════════════════════════════
# PASO 3 — CALCULAR TaT (TOTAL + SUB-ETAPAS)
# ══════════════════════════════════════════════════════

def calcular_tat(df):
    """Calcula TaT total y desglosado por sub-etapas.

    - tat_total:     Solicitud → Dispersión   (créditos exitosos)
    - tat_revision:  Solicitud → Dictamen     (tiempo de evaluación crediticia)
    - tat_operacion: Dictamen  → Dispersión   (tiempo de ejecución tras decidir)
    - tat_rechazo:   Solicitud → Rechazo      (cuánto tardamos en rechazar)
    """
    log("  Calculando TaT hábil (total + sub-etapas)...")

    df['tat_total'] = df.apply(
        lambda r: biz_hours(r['fecha_solicitud'], r['fecha_dispersion']), axis=1)

    df['tat_revision'] = df.apply(
        lambda r: biz_hours(r['fecha_solicitud'], r['fecha_dictamen']), axis=1)

    df['tat_operacion'] = df.apply(
        lambda r: biz_hours(r['fecha_dictamen'], r['fecha_dispersion']), axis=1)

    df['tat_rechazo'] = df.apply(
        lambda r: biz_hours(r['fecha_solicitud'], r['fecha_rechazo']), axis=1)

    log("  ✓ TaT calculado (total, revisión, operación, rechazo)")
    return df

# ══════════════════════════════════════════════════════
# PASO 4 — CALCULAR TODOS LOS INDICADORES DEL REPORTE
# ══════════════════════════════════════════════════════

def bloque_horario(h):
    if 9  <= h < 11: return '09–11h'
    if 11 <= h < 13: return '11–13h'
    if 13 <= h < 15: return '13–15h'
    if 15 <= h < 17: return '15–17h'
    if 17 <= h < 18: return '17–18h'
    return 'Fuera horario'


def calcular_indicadores(df):
    meses = sorted(df['mes'].unique())

    # ── Lógica de mes inteligente ──
    # Días 1-7: el mes en curso tiene pocos datos — reportar mes anterior completo
    # Día 8+:   reportar mes en curso vs mes anterior
    try:
        import pytz as _pytz
        _hoy_check = datetime.now(_pytz.timezone('America/Mexico_City'))
    except ImportError:
        from zoneinfo import ZoneInfo as _ZI
        _hoy_check = datetime.now(_ZI('America/Mexico_City'))

    if _hoy_check.day <= 7:
        mes_actual   = meses[-2] if len(meses) >= 2 else meses[-1]
        mes_anterior = meses[-3] if len(meses) >= 3 else meses[-2] if len(meses) >= 2 else meses[-1]
        log(f"  📅 Día {_hoy_check.day} del mes — mostrando mes anterior completo ({mes_actual})")
    else:
        mes_actual   = meses[-1]
        mes_anterior = meses[-2] if len(meses) >= 2 else mes_actual
        log(f"  📅 Día {_hoy_check.day} del mes — mostrando mes en curso ({mes_actual})")

    dm  = df[df['mes'] == mes_actual].copy()
    dp  = df[df['mes'] == mes_anterior].copy()
    try:
        import pytz
        tz_mx = pytz.timezone('America/Mexico_City')
        hoy_mx = datetime.now(tz_mx)
    except ImportError:
        from zoneinfo import ZoneInfo
        hoy_mx = datetime.now(ZoneInfo('America/Mexico_City'))
    hoy = datetime.now()

    log(f"  Mes actual: {mes_actual} ({len(dm)} registros) | Anterior: {mes_anterior} ({len(dp)} registros)")

    # ── KPIs globales ──
    def kpi_aprobacion(d):
        return r1(d[d['estatus']=='creditoAperturado'].shape[0] / len(d) * 100) if len(d) else 0.0

    def kpi_mismo_dia(d):
        v = d['tat_total'].dropna(); v = v[v <= TAT_THRESHOLD]
        return r1((v <= 9).sum() / len(v) * 100) if len(v) else 0.0

    def kpi_flujo(d):
        return r1(d[d['estatus']=='creditoAperturado'].shape[0] / len(d) * 100) if len(d) else 0.0

    tat_act  = tat_med(dm['tat_total'])
    tat_prev = tat_med(dp['tat_total'])
    apr_act  = kpi_aprobacion(dm); apr_prev = kpi_aprobacion(dp)
    dia_act  = kpi_mismo_dia(dm);  dia_prev = kpi_mismo_dia(dp)
    flu_act  = kpi_flujo(dm);      flu_prev = kpi_flujo(dp)

    # ── KPIs NUEVOS: sub-etapas de TaT ──
    # Solo cuentan créditos dispersados (excluye rechazados y pendientes)
    disp_m = dm[dm['estatus']=='creditoAperturado']
    disp_p = dp[dp['estatus']=='creditoAperturado']
    tat_revision_act   = tat_med(disp_m['tat_revision'])
    tat_revision_prev  = tat_med(disp_p['tat_revision'])
    tat_operacion_act  = tat_med(disp_m['tat_operacion'])
    tat_operacion_prev = tat_med(disp_p['tat_operacion'])

    # ── KPI NUEVO: TaT de rechazos ──
    # Solo cuentan créditos rechazados (tienen fecha_rechazo)
    rech_m = dm[dm['estatus'].isin(['rechazado','declinado'])]
    rech_p = dp[dp['estatus'].isin(['rechazado','declinado'])]
    tat_rechazo_act  = tat_med(rech_m['tat_rechazo'])
    tat_rechazo_prev = tat_med(rech_p['tat_rechazo'])

    # ── KPI NUEVO: ratio Monto Autorizado / Monto Solicitado ──
    # Mediana del % aprobado del monto pedido (solo créditos dispersados)
    if 'monto_solicitado' in dm.columns:
        ratio_m = disp_m[(disp_m['monto_solicitado']>0) & disp_m['monto_autorizado'].notna()].copy()
        ratio_m['ratio'] = (ratio_m['monto_autorizado'] / ratio_m['monto_solicitado']) * 100
        ratio_monto_act = r1(ratio_m['ratio'].median()) if len(ratio_m) else 0.0
        ratio_p = disp_p[(disp_p.get('monto_solicitado', pd.Series()).fillna(0)>0) & disp_p['monto_autorizado'].notna()].copy()
        if len(ratio_p):
            ratio_p['ratio'] = (ratio_p['monto_autorizado'] / ratio_p['monto_solicitado']) * 100
            ratio_monto_prev = r1(ratio_p['ratio'].median())
        else:
            ratio_monto_prev = 0.0
    else:
        ratio_monto_act = ratio_monto_prev = 0.0

    dispersados = dm[dm['estatus']=='creditoAperturado']
    total_monto = round(float(dm['monto_autorizado'].sum()))

    # ── Productos ──
    productos = []
    for prod in PROD_ORDER:
        g  = dm[dm['tipo_crediticio']==prod]
        gp = dp[dp['tipo_crediticio']==prod]
        if len(g) == 0: continue
        n       = len(g)
        apr     = r1(g[g['estatus']=='creditoAperturado'].shape[0] / n * 100)
        rech    = r1(g[g['estatus'].isin(['rechazado','declinado'])].shape[0] / n * 100)
        pend    = r1(g[g['estado_solicitud']=='PENDIENTE'].shape[0] / n * 100)
        tm      = tat_med(g['tat_total'])
        tp      = tat_p90(g['tat_total'])
        tm_prev = tat_med(gp['tat_total'])
        monto   = round(float(g[g['estatus']=='creditoAperturado']['monto_autorizado'].sum()))
        productos.append({
            'label':       PROD_LABEL[prod],
            'n':           n,
            'aprobacion':  apr,
            'rechazado':   rech,
            'pendiente':   pend,
            'tat_med':     tm,
            'tat_p90':     tp,
            'tat_anterior':tm_prev,
            'monto':       monto,
            'color':       PROD_COLORS[prod],
        })

    # ── Distribución TaT ──
    RANGOS = [
        ('<1 hr',     0,    1,    '#00a878'),
        ('1–2.5 hrs', 1,    2.5,  '#1a5e9e'),
        ('2.5–4.5',   2.5,  4.5,  '#1e6abf'),
        ('4.5–9 hrs', 4.5,  9,    '#3d9be8'),
        ('9–18 hrs',  9,    18,   '#c97c00'),
        ('18–36 hrs', 18,   36,   '#c85a17'),
        ('>36 hrs',   36,   9999, '#b91c1c'),
    ]
    v_tat = dm[dm['estatus']=='creditoAperturado']['tat_total'].dropna()
    v_tat = v_tat[v_tat <= TAT_THRESHOLD]
    total_v = len(v_tat)
    dist_tat = []
    for rango, lo, hi, color in RANGOS:
        n_r = ((v_tat >= lo) & (v_tat < hi)).sum()
        dist_tat.append({'rango':rango,'n':int(n_r),'pct':r1(n_r/total_v*100) if total_v else 0,'color':color})

    # ── Embudo ──
    STEPS_EMBUDO = [
        'telefonoPersona','generaCURP','datosDomicilio','datosEmpleo',
        'datosReferenciaPersonal','evaluacionProceso','medioEntrega',
        'procesoPruebaDeVida','loanAceppted','enEsperaDispersion','inicio',
    ]
    total_emb = len(dm)
    embudo = []
    for step in STEPS_EMBUDO:
        n_s = (dm['step']==step).sum()
        if n_s == 0: continue
        embudo.append({'paso': STEP_LABEL.get(step,step), 'n':int(n_s), 'pct':r1(n_s/total_emb*100)})
    n_disp = int((dm['estatus']=='creditoAperturado').sum())
    n_rech = int((dm['estatus'].isin(['rechazado','declinado']) | (dm['step']=='loanRejected')).sum())
    n_canc = int((dm['step']=='prestamoCanceladoUsuario').sum())
    embudo.append({'paso':'Dispersado ✅','n':n_disp,'pct':r1(n_disp/total_emb*100),'ok':True})
    embudo.append({'paso':'Rechazado',    'n':n_rech,'pct':r1(n_rech/total_emb*100),'mal':True})
    embudo.append({'paso':'Cancelado',    'n':n_canc,'pct':r1(n_canc/total_emb*100),'mal':True})

    # ── Rechazos ──
    rechazados = dm[dm['estatus'].isin(['rechazado','declinado']) | (dm['step']=='loanRejected')]
    rech_total = len(rechazados)
    rech_tasa  = r1(rech_total / len(dm) * 100)
    rech_prod  = []
    for prod in PROD_ORDER:
        n_r = ((rechazados['tipo_crediticio']==prod)).sum()
        if n_r > 0:
            rech_prod.append({'producto':PROD_LABEL[prod],'n':int(n_r)})

    # ── NUEVO: Motivos de rechazo (top 5) ──
    motivos_rechazo = []
    if 'motivo_rechazo' in rechazados.columns:
        motivos_serie = rechazados['motivo_rechazo'].dropna()
        for motivo, n_m in motivos_serie.value_counts().head(5).items():
            # Limpiamos motivos que vienen con cola (ej: "dictamen_rechazado, EL CLIENTE YA NO LO QUIERE")
            motivo_clean = str(motivo).split(',')[0].strip()
            motivos_rechazo.append({
                'motivo': MOTIVO_LABEL.get(motivo_clean, motivo_clean),
                'n':      int(n_m),
                'pct':    r1(n_m / rech_total * 100) if rech_total else 0,
            })

    # ── Horario ──
    dm2 = dm[dm['estatus']=='creditoAperturado'].copy()
    dm2['blq'] = dm2['fecha_solicitud'].dt.hour.apply(bloque_horario)
    BLOQUES = ['09–11h','11–13h','13–15h','15–17h','17–18h','Fuera horario']
    horario = []
    for blq in BLOQUES:
        g_blq = dm2[dm2['blq']==blq]
        if len(g_blq) == 0:
            horario.append({'franja':blq,'tat_med':0,'mismo_dia':0})
            continue
        tm_blq = tat_med(g_blq['tat_total'])
        v_blq  = g_blq['tat_total'].dropna(); v_blq = v_blq[v_blq<=TAT_THRESHOLD]
        md_blq = r1((v_blq<=9).sum()/len(v_blq)*100) if len(v_blq) else 0
        horario.append({'franja':blq,'tat_med':tm_blq,'mismo_dia':md_blq})

    # ── Pendientes ──
    pend = dm[dm['estado_solicitud']=='PENDIENTE'].copy()
    pend['dias'] = (hoy - pend['fecha_solicitud']).dt.days.clip(lower=0)
    total_pend = len(pend)
    pend_tabla = []
    for step, lbl in STEP_LABEL.items():
        g_s = pend[pend['step']==step]
        if len(g_s) == 0: continue
        pend_tabla.append({
            'paso':     step,
            'label':    lbl,
            'n':        int(len(g_s)),
            'pct':      r1(len(g_s)/total_pend*100) if total_pend else 0,
            'med_dias': r1(g_s['dias'].median()),
            'max_dias': r1(g_s['dias'].max()),
        })
    pend_tabla.sort(key=lambda x: x['n'], reverse=True)

    # ── Ciudades (Ciudad Título — donde está la sucursal) ──
    ciudades = []
    for ciudad, g_c in dm.groupby('ciudad_titulo'):
        if pd.isna(ciudad) or len(g_c) < 5: continue
        apr_c = r1(g_c[g_c['estatus']=='creditoAperturado'].shape[0]/len(g_c)*100)
        ciudades.append({
            'ciudad':    str(ciudad),
            'n':         int(len(g_c)),
            'aprobacion':apr_c,
            'tat_med':   tat_med(g_c['tat_total']),
            'tat_p90':   tat_p90(g_c['tat_total']),
        })
    ciudades.sort(key=lambda x: x['n'], reverse=True)

    # ── NUEVO: Demanda por Estado de Residencia ──
    # De dónde vienen los clientes (donde viven, no donde solicitan)
    estados_residencia = []
    if 'estado_residencia' in dm.columns:
        for estado, g_e in dm.groupby('estado_residencia'):
            if pd.isna(estado) or len(g_e) < 5: continue
            apr_e = r1(g_e[g_e['estatus']=='creditoAperturado'].shape[0]/len(g_e)*100)
            estados_residencia.append({
                'estado':     str(estado),
                'n':          int(len(g_e)),
                'aprobacion': apr_e,
                'tat_med':    tat_med(g_e['tat_total']),
            })
        estados_residencia.sort(key=lambda x: x['n'], reverse=True)
        estados_residencia = estados_residencia[:15]  # top 15

    D = {
        'mes_actual':              mes_largo(mes_actual),
        'mes_anterior':            mes_largo(mes_anterior),
        'mes_actual_corto':        mes_corto(mes_actual),
        'mes_anterior_corto':      mes_corto(mes_anterior),
        'fecha_corte':             fecha_corte(mes_actual),
        'hora_actualizacion':      hoy_mx.strftime('%H:%M'),
        # KPIs existentes
        'tat_med':                 tat_act,
        'tat_anterior':            tat_prev,
        'aprobacion_pct':          apr_act,
        'aprobacion_anterior':     apr_prev,
        'mismo_dia_pct':           dia_act,
        'mismo_dia_anterior':      dia_prev,
        'flujo_completo_pct':      flu_act,
        'flujo_completo_anterior': flu_prev,
        'total_solicitudes':       len(dm),
        'total_dispersados':       n_disp,
        'total_monto':             total_monto,
        # KPIs NUEVOS
        'tat_revision_med':        tat_revision_act,
        'tat_revision_anterior':   tat_revision_prev,
        'tat_operacion_med':       tat_operacion_act,
        'tat_operacion_anterior':  tat_operacion_prev,
        'tat_rechazo_med':         tat_rechazo_act,
        'tat_rechazo_anterior':    tat_rechazo_prev,
        'ratio_monto_pct':         ratio_monto_act,
        'ratio_monto_anterior':    ratio_monto_prev,
        # Bloques existentes
        'productos':               productos,
        'dist_tat':                dist_tat,
        'embudo':                  embudo,
        'rechazos_total':          rech_total,
        'rechazos_tasa':           rech_tasa,
        'rechazos_por_producto':   rech_prod,
        'motivos_rechazo':         motivos_rechazo,   # NUEVO
        'horario':                 horario,
        'pendientes':              pend_tabla,
        'ciudades':                ciudades,
        'estados_residencia':      estados_residencia, # NUEVO
    }

    log(f"  ✓ {mes_largo(mes_actual)} | TaT total {tat_act}h | "
        f"Revisión {tat_revision_act}h | Operación {tat_operacion_act}h | "
        f"Rechazo {tat_rechazo_act}h | Aprobación {apr_act}% | "
        f"Ratio Monto {ratio_monto_act}% | {len(dm)} sol")
    return D

# ══════════════════════════════════════════════════════
# PASO 5 — INYECTAR DATOS EN index.html
# ══════════════════════════════════════════════════════

def inyectar_en_html(D, ruta_html):
    if not ruta_html.exists():
        raise FileNotFoundError(f"No encontré: {ruta_html}")

    with open(ruta_html, 'r', encoding='utf-8') as f:
        html = f.read()

    START = 'const D = {'; END = '\n};\n'
    ini = html.find(START)
    if ini == -1: raise ValueError("No encontré 'const D = {' en el HTML")
    fin = html.find(END, ini)
    if fin == -1: raise ValueError("No encontré el cierre '};' del bloque")
    fin += len(END)

    def js_val(v):
        if isinstance(v, bool):   return 'true' if v else 'false'
        if isinstance(v, str):    return f"'{v}'"
        if isinstance(v, list):   return json.dumps(v, ensure_ascii=False, indent=4)
        if isinstance(v, dict):   return json.dumps(v, ensure_ascii=False)
        return str(v)

    lineas = [f"  // Actualizado automáticamente — {D['fecha_corte']} {D['hora_actualizacion']}"]
    for k, v in D.items():
        lineas.append(f"  {k}: {js_val(v)},")
    nuevo_bloque = START + '\n' + '\n'.join(lineas) + '\n' + END

    html_nuevo = html[:ini] + nuevo_bloque + html[fin:]
    with open(ruta_html, 'w', encoding='utf-8') as f:
        f.write(html_nuevo)

    log(f"  ✓ index.html actualizado — {len(html_nuevo):,} chars")
    return html_nuevo

# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════

def main():
    log("=" * 55)
    log("INICIO — REPORTE TAT DIRECCIÓN")
    log("=" * 55)
    try:
        df = descargar_csv(CSV_URL)
        df = preparar_base(df)
        df = calcular_tat(df)
        D  = calcular_indicadores(df)

        # Agregar datos de Base de Control si está disponible
        bc = descargar_base_control()
        if bc is not None:
            meses_bc = sorted(df['mes'].unique())
            try:
                import pytz as _p
                _hd = datetime.now(_p.timezone('America/Mexico_City'))
            except ImportError:
                from zoneinfo import ZoneInfo as _Z
                _hd = datetime.now(_Z('America/Mexico_City'))
            mes_rep  = meses_bc[-2] if _hd.day <= 7 and len(meses_bc)>=2 else meses_bc[-1]
            mes_prev = meses_bc[-3] if _hd.day <= 7 and len(meses_bc)>=3 else meses_bc[-2] if len(meses_bc)>=2 else meses_bc[-1]
            ctrl = calcular_indicadores_control(bc, mes_rep, mes_prev)
            if ctrl:
                D['control'] = ctrl
                log(f"  ✓ Base de Control integrada: {ctrl['total_act']} contactos")
        inyectar_en_html(D, DASHBOARD_HTML)
        log("✅ REPORTE ACTUALIZADO EXITOSAMENTE")
        log("=" * 55)
    except Exception as e:
        log(f"❌ ERROR: {e}")
        log(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
