import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task
import logging
import holidays
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import seaborn as sns

plt.rcParams.update({'figure.dpi': 120, 'figure.facecolor': 'white'})
ACCENT = '#E8504A'
BLUE   = '#3A86FF'
GREEN  = '#2DC653'

BASE_DIR = "/home/vboxuser/SDPD2/proy_SSDD_II/data/"
PATH_LOCAL_CSV = os.path.join(BASE_DIR, "calendar.csv")
OUTPUT_DIR= "/tmp/airbnb_calendar_output"

log = logging.getLogger(__name__)

def handle_missing_values(df, variable, threshold=0.8):
    """
    Handles missing values for a specific variable by either dropping the column 
    if the null count exceeds a threshold or cleaning and converting it to float.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        variable (str): The name of the column to process.
        threshold (float): The maximum allowed ratio of missing values (0 to 1). Defaults to 0.8.

    Returns:
        pd.DataFrame: The processed DataFrame with the column either modified or removed.
    """
    na_count = df[variable].isna().sum()
    if na_count > threshold * len(df):
        log.info(f"Dropping variable {variable} due to high missing percentage.")
        df.drop(columns=[variable], inplace=True)
    else:
        log.info(f"Keeping variable {variable} with missing percentage below threshold.")
        df[variable] = df[variable].str.replace(r'[\$,]', '', regex=True).astype(float)

    return df

def get_event(date):
    """
    Categorizes a given date into specific seasonal events or holidays, 
    including Easter (Semana Santa), Málaga's Fair, and summer periods.

    Args:
        date (datetime or pd.Timestamp): The date to be categorized.

    Returns:
        str: The name of the detected event (e.g., 'Semana Santa', 'Feria de Málaga', 
            'Verano') or 'Temporada normal' if no special event is matched.
    """
    year = date.year
    holiday_es = holidays.Spain(years=year, subdiv='AN')
    easter = [d for d, name in holiday_es.items() if 'Viernes Santo' in name]
    
    if easter:
        viernes_santo = pd.Timestamp(easter[0])
        ramos = viernes_santo - pd.Timedelta(days=7)
        if ramos <= date <= viernes_santo + pd.Timedelta(days=1):
            return 'Semana Santa'
        
    if pd.Timestamp(f'{year}-08-15') <= date <= pd.Timestamp(f'{year}-08-22'): 
        return 'Feria de Málaga'
    
    if date.month == 6 and date.day == 23:
        return 'Noche de San Juan'
    
    if date.month in [6, 7, 8]:
        return 'Verano'
    
    if (date.month == 12 and date.day >= 20) or (date.month == 1 and date.day <= 6):
        return 'Navidad/Reyes'
    
    return 'Temporada normal'

@dag(
    dag_id='airbnb_calendar_taskflow',
    schedule=None,
    start_date=datetime(2026, 3, 27),
    catchup=False,
    tags=['malaga', 'calendar', 'taskflow'],
    default_args={
        'owner': 'vboxuser',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)

def calendar_pipeline():
    
    @task()
    def extract():
        """
        Reads the raw calendar CSV file using a predefined schema, processes the 
        data types, and saves the resulting dataset into a Parquet file.

        Returns:
            str: The file path where the processed Parquet dataset is stored.
        """
        dtype_dict = {
            'listing_id': 'int64',
            'date': 'str',
            'available': 'str',
            'price': 'str',
            'adjusted_price': 'str',
            'minimum_nights': 'int64',
            'maximum_nights': 'int64',
        }

        df = pd.read_csv(PATH_LOCAL_CSV, dtype=dtype_dict) 

        # Save dataset in parket format
        processed_path = os.path.join(OUTPUT_DIR, "processed_calendar.parquet")
        df.to_parquet(processed_path, index=False)

        return processed_path

    @task()
    def transform(processed_path: str):
        """
        Performs data cleaning and feature engineering on the calendar dataset. 
        
        This includes converting types (date, boolean, numeric), handling missing 
        values for pricing, and generating new features such as 'event' (holidays), 
        'booked' status, and time-based flags (weekend, month).

        Args:
            processed_path (str): Path to the input Parquet file.

        Returns:
            str: Path to the finalized transformed Parquet file.
        """
        # Load dataset
        df = pd.read_parquet(processed_path)
        log.info(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")

        # --------- DATE TO DATETIME ---------
        df['date'] = pd.to_datetime(df['date'], errors='coerce') # coerce for missing data to be NAN
        date_na = df['date'].isna().sum()
        if date_na > 0:
            log.warning(f"Found {date_na} missing values in 'date' column after conversion.")

        # --------- AVAILAVEL TO BOOL ---------
        df['available'] = df['available'].map({'t': True, 'f': False})
        log.info('Available variable converted to boolean.')

        # Dinamic gestión for price an adjusted_price variables
        for col in ['price', 'adjusted_price']:
            if col in df.columns:
                df = handle_missing_values(df, col)

        log.info(f'Dataset cleaned: {df.shape[0]} rows, {df.shape[1]} columns')

        # Minimum and maximum nights conversion to int32
        for col in ['minimum_nights', 'maximum_nights']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')
                log.info(f'{col} variable converted to Int32 with NA handling.')

        # --------- EVENT FEATURE CREATION ---------
        unique_date = df['date'].unique()
        event_map = {date: get_event(pd.Timestamp(date)) for date in unique_date}
        df['event'] = df['date'].map(event_map)
        log.info('Event variable created based on date.')

        # --------- BOOKED FEATURE CREATION ---------
        df['booked'] = ~df['available']
        log.info('Booked variable created as inverse of available.')

        # --------- MONTH FEATURE CREATION ---------
        df['month'] = df['date'].dt.month

        # --------- DAY_OF_WEEK FEATURE CREATION ---------
        df['day_of_week'] = df['date'].dt.dayofweek

        # --------- IS_WEEKEND FEATURE CREATION ---------
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        log.info('New date-based variables created: month, day_of_week, is_weekend.')

        # Save de DataFrame in parquet format
        transformed_path = os.path.join(OUTPUT_DIR, "transformed_calendar.parquet")
        df.to_parquet(transformed_path, index=False)
        log.info(f'Transformed dataset saved to {transformed_path}')

        return transformed_path
    
    @task()
    def validate(transformed_path: str):
        """
        Performs data quality checks on the transformed dataset to ensure 
        schema integrity and correct data types before final delivery.

        Args:
            transformed_path (str): Path to the Parquet file to be validated.

        Returns:
            str: The same path if validation passes, allowing for task chaining.
        """
        df = pd.read_parquet(transformed_path)
        log.info(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")

        # Verify if availabe is bool tipe
        if not pd.api.types.is_bool_dtype(df['available']):
            log.warning("Variable available not transformed correctly")

        if not pd.api.types.is_datetime64_dtype(df['date']):
            log.warning("Variable date not transformed correctly")
    
        return transformed_path

    @task()
    def EDA(transformed_path: str):
        """
        Generates a suite of data visualizations to analyze Airbnb occupancy patterns.
        
        This includes:
        1. Daily occupancy trends with rolling averages and holiday overlays.
        2. Weekly patterns comparing weekdays vs. weekends.
        3. Monthly heatmap cross-referenced with days of the week.
        4. Categorical analysis of occupancy rates during special events.

        Args:
            transformed_path (str): Path to the transformed Parquet file.

        Saves:
            PNG plots for daily, weekly, and monthly trends to the output directory.
        """
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
        fig.savefig(os.path.join(OUTPUT_DIR, "ocupacion_diaria.png"), dpi=150, bbox_inches='tight')
        plt.close(fig)

        # --------- WEAKLY OCUPATION PLOT ---------
        day_labels_es = ['Lun','Mar','Mié','Jue','Vie','Sáb','Dom']

        weekly_occ = (df.groupby('day_of_week')['booked']
                    .agg(['mean','sem'])
                    .reset_index()
                    .sort_values('day_of_week'))

        fig, ax = plt.subplots(figsize=(9, 4))
        bars = ax.bar(day_labels_es, weekly_occ['mean'],
                    color=[ACCENT if x >= 5 else BLUE for x in weekly_occ['day_of_week']],
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
        fig.savefig(os.path.join(OUTPUT_DIR, "ocupacion_semanal.png"), dpi=150, bbox_inches='tight')
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
        fig.savefig(os.path.join(OUTPUT_DIR, "ocupacion_heatmap.png"), dpi=150, bbox_inches='tight')
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
        fig.savefig(os.path.join(OUTPUT_DIR, "analisis_eventos.png"), dpi=150, bbox_inches='tight')
        plt.close(fig)
        return
    
    @task()
    def load(transformed_path: str):
        log.info("Tarea Load alcanzada correctamente.")
        log.info(f"Datos listos para cargar desde: {transformed_path}")
        log.info("Integración con Apache Spark pendiente de implementación.")
    
    # Order of execution
    processed_path = extract()
    transformed_path = transform(processed_path)
    validate(transformed_path)
    EDA(transformed_path)
    load(transformed_path)

dag_instance = calendar_pipeline()