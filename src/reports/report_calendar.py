import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
import seaborn as sns
import holidays
import os
import json
from jinja2 import Template
from pathlib import Path

def eda_calendar(transformed_path, output_dir, log, template_path):
    """
    Generates a suite of data visualizations to analyze Airbnb occupancy patterns.
    
    This includes:
    1. Daily occupancy trends with rolling averages and holiday overlays.
    2. Weekly patterns comparing weekdays vs. weekends.
    3. Monthly heatmap cross-referenced with days of the week.
    4. Categorical analysis of occupancy rates during special events.
    """
    
    # Configuration and Theme
    ACCENT = "#FF5A5F"  # Airbnb Red
    BLUE = "#45CFEB"
    os.makedirs(output_dir, exist_ok=True)
    
    df = pd.read_parquet(transformed_path)
    log.info(f"Dataset loaded for EDA: {df.shape[0]} rows, {df.shape[1]} columns")

    # --------- DIARY OCUPATION RATE ---------
    daily_occ = df.groupby('date')['booked'].mean()
    daily_occ_7d  = daily_occ.rolling(7,  center=True).mean()
    daily_occ_30d = daily_occ.rolling(30, center=True).mean()

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.fill_between(daily_occ.index, daily_occ, alpha=0.15, color=ACCENT)
    ax.plot(daily_occ.index, daily_occ,      color=ACCENT,  alpha=0.4, lw=0.8, label='Diaria')
    ax.plot(daily_occ_7d.index,  daily_occ_7d,  color=BLUE,   lw=1.5,  label='Media 7d')
    ax.plot(daily_occ_30d.index, daily_occ_30d, color='black', lw=2,    label='Media 30d')

    data_year = df['date'].dt.year.mode()[0]
    events = [
        (f'{data_year}-08-15', f'{data_year}-08-22', 'Feria de Málaga', "#F1B060"),
        (f'{data_year}-12-06', f'{data_year}-12-08', 'Puente de diciembre', "#45CFEB"),
        (f'{data_year}-12-12', f'{data_year}-12-19', 'Zambombas flamencas', "#4863DA"),
        (f'{data_year}-12-20', f'{data_year}-01-06', 'Navidad', '#9B59B6'),
        (f'{data_year}-06-23', f'{data_year}-06-23', 'Noche de San Juan', '#E74C3C'),
    ]

    holidays_es = holidays.Spain(years=[data_year, data_year+1], subdiv='AN')
    viernes_santos = [d for d, name in holidays_es.items() if 'Viernes Santo' in name and d.year == data_year]

    if viernes_santos:
        viernes = pd.Timestamp(viernes_santos[0])
        events.append((
            str(viernes - pd.Timedelta(days=7)),  # Domingo de Ramos
            str(viernes + pd.Timedelta(days=1)),  # Sábado Santo
            'Semana Santa',
            '#2ECC71'
        ))

    for start, end, label, color in events:
        ax.axvspan(pd.Timestamp(start), pd.Timestamp(end), alpha=0.12, color=color, label=label)

    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b\n%Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.set_title('Tasa de Ocupación Diaria', fontsize=14, fontweight='bold')
    ax.set_xlabel(''); ax.set_ylabel('Tasa de Ocupación')
    ax.legend(ncol=3, fontsize=9)
    plt.tight_layout()
    fig.savefig(os.path.join(output_dir, "ocupacion_diaria.png"), dpi=150, bbox_inches='tight')
    plt.close(fig)

    # --------- WEAKLY OCUPATION PLOT ---------
    day_labels_es = ['Lun','Mar','Mié','Jue','Vie','Sáb','Dom']

    weekly_occ = (df.groupby('day_of_week')['booked']
                .agg(['mean','sem'])
                .reset_index()
                .sort_values('day_of_week'))

    fig, ax = plt.subplots(figsize=(9, 4))
    # We use day_of_week index for coloring logic
    colors = [ACCENT if x >= 5 else BLUE for x in weekly_occ['day_of_week']]
    bars = ax.bar(day_labels_es, weekly_occ['mean'],
                color=colors,
                yerr=weekly_occ['sem']*1.96, capsize=4, edgecolor='white')
    
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.set_title('Tasa de Ocupación por Día de la Semana (IC 95%)', fontweight='bold')
    ax.set_ylabel('Ocupación media')
    ax.axvline(4.5, color='grey', linestyle='--', lw=1, label='Inicio fin de semana')
    ax.legend(fontsize=9)
    
    for bar, val in zip(bars, weekly_occ['mean']):
        ax.text(bar.get_x() + bar.get_width()/2, val + 0.003, f'{val:.1%}',
                ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    fig.savefig(os.path.join(output_dir, "ocupacion_semanal.png"), dpi=150, bbox_inches='tight')
    plt.close(fig)

    # --------- HEATMAP OCUPATION BY MONTH ---------
    month_labels = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
    pivot = (df
            .groupby(['month', 'day_of_week'])['booked']
            .mean()
            .unstack('day_of_week'))
    pivot.index = month_labels[:len(pivot)]
    pivot.columns = day_labels_es

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.heatmap(pivot, annot=True, fmt='.0%', cmap='YlOrRd',
                linewidths=0.5, ax=ax, vmin=0, vmax=1,
                cbar_kws={'label': 'Tasa de ocupación'})
    ax.set_title('Mapa de Calor: Ocupación Media (Mes × Día de Semana)', fontweight='bold')
    ax.set_xlabel('Día'); ax.set_ylabel('Mes')
    plt.tight_layout()
    fig.savefig(os.path.join(output_dir, "ocupacion_heatmap.png"), dpi=150, bbox_inches='tight')
    plt.close(fig)

    # --------- SPECIAL EVENTS PLOT ---------
    event_stats = (df
                .groupby('event')
                .agg(
                    ocupacion_media=('booked', 'mean'),
                    noches_min_media=('minimum_nights', 'mean'),
                    noches_max_media=('maximum_nights', 'mean'),
                    n_registros=('booked', 'count')
                )
                .sort_values('ocupacion_media', ascending=False))
    
    fig, ax = plt.subplots(figsize=(9, 4))
    event_stats['ocupacion_media'].sort_values().plot(
        kind='barh', ax=ax, color=ACCENT
    )
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    ax.set_title('Ocupación Media por Período/Evento', fontweight='bold')
    ax.set_xlabel('Ocupación media')
    plt.tight_layout()
    fig.savefig(os.path.join(output_dir, "analisis_eventos.png"), dpi=150, bbox_inches='tight')
    plt.close(fig)

    # --------- PAYLOAD GENERATION FOR HTML ---------
    def series_to_list(s):
        return [None if pd.isna(v) else round(float(v), 4) for v in s]
    
    payload = {
        "meta": {
            "rows":     int(len(df)),
            "date_min": str(df['date'].min().date()),
            "date_max": str(df['date'].max().date()),
        },
        "kpis": {
            "occ_mean":    round(float(df['booked'].mean()), 4),
            "occ_weekend": round(float(df[df['day_of_week'] >= 5]['booked'].mean()), 4),
            "occ_weekday": round(float(df[df['day_of_week'] <  5]['booked'].mean()), 4),
            "peak_date":   str(daily_occ.idxmax().date()),
            "peak_value":  round(float(daily_occ.max()), 4),
        },
        "daily": {
            "dates":  daily_occ.index.strftime('%Y-%m-%d').tolist(),
            "values": series_to_list(daily_occ),
            "ma7":    series_to_list(daily_occ_7d),
            "ma30":   series_to_list(daily_occ_30d),
            "events": [
                {"start": start, "end": end, "label": label, "color": color}
                for start, end, label, color in events
            ],
        },
        "weekly": {
            "labels": day_labels_es,
            "mean":   [round(float(v), 4) for v in weekly_occ['mean']],
            "ci95":   [round(float(v * 1.96), 4) for v in weekly_occ['sem']],
        },
        "heatmap": {
            "months":  month_labels,
            "days":    day_labels_es,
            "matrix":  [
                [None if pd.isna(v) else round(float(v), 4) for v in row]
                for row in pivot.reindex(
                    columns=day_labels_es,
                    fill_value=float('nan')
                ).values
            ],
        },
        "events_stats": {
            "labels":     event_stats.index.tolist(),
            "ocupacion":  [round(float(v), 4) for v in event_stats['ocupacion_media']],
            "min_nights": [round(float(v), 2) for v in event_stats['noches_min_media']],
            "max_nights": [round(float(v), 2) for v in event_stats['noches_max_media']],
            "n":          [int(v)             for v in event_stats['n_registros']],
        },
    }
    
    # --------- RENDER TO JINJA2 ---------
    TEMPLATE_PATH = template_path
    OUTPUT_HTML = Path(output_dir) / "eda_report_ocupacion.html"
    
    if TEMPLATE_PATH.exists():
        template_src = TEMPLATE_PATH.read_text(encoding='utf-8')
        template = Template(template_src)
        html_rendered = template.render(payload=json.dumps(payload, ensure_ascii=False))
        OUTPUT_HTML.write_text(html_rendered, encoding='utf-8')
        log.info(f"EDA HTML generado en: {OUTPUT_HTML}")
    else:
        log.warning(f"Template not found at {TEMPLATE_PATH}. Skipping HTML generation.")
    
    return