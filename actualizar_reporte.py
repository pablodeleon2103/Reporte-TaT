"""
actualizar_reporte.py
=====================
Descarga el CSV desde nueva.percapita.mx, calcula todos los
indicadores del reporte mensual de TaT y actualiza index.html.

Corre en GitHub Actions — no necesita config.txt ni token.
"""

import pandas as pd
import numpy as np
import json
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO

# ══════════════════════════════════════════════════════
# CONFIGURACIÓN
# ══════════════════════════════════════════════════════

CSV_URL        = os.environ.get('CSV_URL', 'https://nueva.percapita.mx/api/creditos/reporte/export/csv')
DASHBOARD_HTML = Path(__file__).parent / 'index.html'
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

STEP_LABEL = {
    'crearUsuario':'Crear usuario','datosPersona':'Datos personales',
    'telefonoPersona':'Teléfono','generaCURP':'Validación CURP',
    'datosDomicilio':'Datos domicilio','datosEmpleo':'Datos empleo',
    'datosReferenciaPersonal':'Referencias','evaluacionProceso':'Evaluación proceso',
    'medioEntrega':'Medio de entrega','procesoPruebaDeVida':'Prueba de vida',
    'loanAceppped':'Crédito aceptado','enEsperaDispersion':'En espera dispersión',
    'inicio':'Dispersión iniciada','loanRejected':'Rechazado sistema',
    'prestamoCanceladoUsuario':'Cancelado por cliente',
    'actualizarCuentaBancaria':'Actualizar cuenta',
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
# PASO 1 — DESCARGAR CSV
# ══════════════════════════════════════════════════════

def descargar_csv(url):
    import requests
    log(f"Descargando CSV desde {url} ...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), encoding="utf-8")
    log(f"  ✓ {len(df):,} filas descargadas")
    return df

# ══════════════════════════════════════════════════════
# PASO 2 — PREPARAR BASE (filtros v8)
# ══════════════════════════════════════════════════════

def preparar_base(df):
    MAPEO = {
        'Nombres':'nombres','Fecha Solicitud':'fecha_solicitud',
        'Fecha Dispersión':'fecha_dispersion','Tipo Crédito':'tipo_crediticio',
        'Monto Autorizado':'monto_autorizado','Estado Solicitud':'estado_solicitud',
        'Step':'step','Estatus':'estatus','Ciudad Título':'ciudad_titulo',
        'Título':'titulo',
    }
    df = df.rename(columns=MAPEO)
    for col in ['fecha_solicitud','fecha_dispersion']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df = df[df['tipo_crediticio'] != 'tipo_crediticio'].copy()
    df = df[~df['tipo_crediticio'].isin(['PERSONAL','RENOVACIONPERSONAL'])].copy()
    df = df[~df['step'].isin(['solicitudPrestamoTitulo','solicitudPrestamoPersonal'])].copy()
    df = df[df['fecha_solicitud'] >= '2025-08-01'].copy()
    df = df.drop_duplicates()
    df['mes'] = df['fecha_solicitud'].dt.to_period('M')

    PASO_ORDEN = {
        'crearUsuario':1,'datosPersona':2,'telefonoPersona':3,'generaCURP':4,
        'datosDomicilio':5,'datosEmpleo':6,'datosReferenciaPersonal':7,
        'evaluacionProceso':9,'medioEntrega':10,'procesoPruebaDeVida':11,
        'loanRejected':12,'loanAceppped':13,'enEsperaDispersion':14,
        'prestamoCanceladoUsuario':14,'inicio':15,
    }
    df['paso_num'] = df['step'].map(PASO_ORDEN).fillna(0)

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
# PASO 3 — CALCULAR TaT
# ══════════════════════════════════════════════════════

def calcular_tat(df):
    log("  Calculando TaT hábil...")
    df['tat_total'] = df.apply(
        lambda r: biz_hours(r['fecha_solicitud'], r['fecha_dispersion']), axis=1)
    log("  ✓ TaT calculado")
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
    meses        = sorted(df['mes'].unique())
    mes_actual   = meses[-1]
    mes_anterior = meses[-2] if len(meses) >= 2 else mes_actual
    dm  = df[df['mes'] == mes_actual].copy()
    dp  = df[df['mes'] == mes_anterior].copy()
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
        'procesoPruebaDeVida','loanAceppped','enEsperaDispersion','inicio',
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

    # ── Ciudades ──
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

    D = {
        'mes_actual':              mes_largo(mes_actual),
        'mes_anterior':            mes_largo(mes_anterior),
        'mes_actual_corto':        mes_corto(mes_actual),
        'mes_anterior_corto':      mes_corto(mes_anterior),
        'fecha_corte':             fecha_corte(mes_actual),
        'hora_actualizacion':      hoy_mx.strftime('%H:%M'),
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
        'productos':               productos,
        'dist_tat':                dist_tat,
        'embudo':                  embudo,
        'rechazos_total':          rech_total,
        'rechazos_tasa':           rech_tasa,
        'rechazos_por_producto':   rech_prod,
        'horario':                 horario,
        'pendientes':              pend_tabla,
        'ciudades':                ciudades,
    }

    log(f"  ✓ {mes_largo(mes_actual)} | TaT {tat_act}h | Aprobación {apr_act}% | {len(dm)} sol")
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
        inyectar_en_html(D, DASHBOARD_HTML)
        log("✅ REPORTE ACTUALIZADO EXITOSAMENTE")
        log("=" * 55)
    except Exception as e:
        log(f"❌ ERROR: {e}")
        log(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
