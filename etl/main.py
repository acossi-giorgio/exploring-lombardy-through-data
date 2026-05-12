from sqlalchemy import create_engine, text
import pipelines as pipelines
import sources as sources
import tables as tables
import os
from logger import logger
from utils import MunicipalityLocator

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "db": "pap",
    "user": "admin",
    "password": "admin",
    "schema": "pap"
}

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
    f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['db']}"
)

DATE = {
    "start": '2023-01-01',
    "end": '2023-12-31'
}

INIT_DB_SQL_SCRIPT = os.path.join(os.path.dirname(__file__), "init_db.sql")
DROP_DB_SQL_SCRIPT = os.path.join(os.path.dirname(__file__), "drop_db.sql")

DATASETS_PATH = os.path.join(os.path.dirname(__file__), "..", "datasets")

AREA_C_MILANO_FOLDER_PATH = f'{DATASETS_PATH}/area-c-milano'
AREA_C_GATE_FILE = f'{AREA_C_MILANO_FOLDER_PATH}/area_c_varchi.csv'
INGRESS_AREA_C_FILE = f'{AREA_C_MILANO_FOLDER_PATH}/ingressi_area_c_2023.csv'
DECODE_AREA_C_FILE = f'{AREA_C_MILANO_FOLDER_PATH}/areac_decode.csv'

ISTAT_FOLDER_PATH = f'{DATASETS_PATH}/istat'
MUNICIPALITY_STATISTICS_FILE = f'{ISTAT_FOLDER_PATH}/statistiche_comuni.csv'
CENSUS_FILE = f'{ISTAT_FOLDER_PATH}/censimento_popolazione_2023.csv'

TOURISM_FLOW_FILE = f'{DATASETS_PATH}/flussi-turistici/flussi_turistici.csv'

AIR_MONITORING_FOLDER_PATH = f'{DATASETS_PATH}/monitoraggio-aria'
AIR_MONITORING_SENSOR_FILE = f'{AIR_MONITORING_FOLDER_PATH}/anagrafica_sensori_aria.csv'
AIR_MONITORING_SAMPLE_FILE = f'{AIR_MONITORING_FOLDER_PATH}/dati_sensori_aria_2023.csv'

WEATHER_MONITORING_FOLDER_PATH = f'{DATASETS_PATH}/monitoraggio-meteo'
WEATHER_MONITORING_SENSOR_FILE = f'{WEATHER_MONITORING_FOLDER_PATH}/anagrafica_sensori_meteo.csv'
WEATHER_MONITORING_SAMPLE_FILE = f'{WEATHER_MONITORING_FOLDER_PATH}/dati_sensori_ambiente_2023.csv'

MUNICIPALITY_GEOJSON_FILE = f'{DATASETS_PATH}/comuni.geojson'

logger.info("Connect to database")
engine = create_engine(DATABASE_URL)


connect = engine.connect() 

logger.info("Drop db")
connect.execute(text(open(DROP_DB_SQL_SCRIPT, "r", encoding="utf-8").read()))

logger.info("Init db")
connect.execute(text(open(INIT_DB_SQL_SCRIPT, "r", encoding="utf-8").read()))

connect.commit()


logger.info("Database initialized successfully")

logger.info("Import sources")
area_c_gate_source = sources.AreaCGateRegistrySource(AREA_C_GATE_FILE)
area_c_ingress_source = sources.AreaCGateIngressSource(INGRESS_AREA_C_FILE)
area_c_decode_source = sources.AreaCDecodeSource(DECODE_AREA_C_FILE)
municipality_statistics_source = sources.MunicipalityStatisticsSource(MUNICIPALITY_STATISTICS_FILE)
tourism_flow_source = sources.TouristFlowsSource(TOURISM_FLOW_FILE)
census_etl_source = sources.CensusETLSource(CENSUS_FILE)
air_monitoring_station_registry_source = sources.AirMonitoringStationRegistrySource(AIR_MONITORING_SENSOR_FILE)
air_monitoring_sample_source = sources.AirMonitoringSampleSource(AIR_MONITORING_SAMPLE_FILE)
weather_air_monitoring_station_registry_source = sources.WeatherMonitoringStationRegistrySource(WEATHER_MONITORING_SENSOR_FILE)
weather_air_monitoring_sample_source = sources.WeatherMonitoringSampleSource(WEATHER_MONITORING_SAMPLE_FILE)

municipality_location_source = sources.MunicipalityLocationSource(MUNICIPALITY_GEOJSON_FILE)
logger.info("Sources imported successfully")

logger.info("Run pipelines")

logger.info("Running LocationPipeline (location_dt)")
location_elt_pipeline = pipelines.LocationPipeline(
    municipality_location_source=municipality_location_source,
    municipality_statistics_source=municipality_statistics_source,
    air_monitoring_source=air_monitoring_station_registry_source,
    weather_monitoring_source=weather_air_monitoring_station_registry_source,
    area_c_gate_source=area_c_gate_source,
    engine=engine,
    table_name="location_dt",
    schema=DB_PARAMS["schema"]
)
location_elt_pipeline.run()

location_table = tables.LocationTable(df=location_elt_pipeline.get_df())
logger.info(f"LocationTable + {len(location_table.df)} rows")

logger.info("Running DatePipeline (date_dt)")
date_etl_pipeline = pipelines.DatePipeline(
    engine=engine,
    table_name="date_dt",
    schema=DB_PARAMS["schema"],
    start_date=DATE["start"],
    end_date=DATE["end"]
)
date_etl_pipeline.run()

date_table = tables.DateTable(df=date_etl_pipeline.get_df())
logger.info(f"DateTable loaded {len(date_table.df)} rows")

logger.info("Running MonthPipeline (month_dt)")
month_etl_pipeline = pipelines.MonthPipeline(
    engine=engine,
    table_name="month_dt",
    schema=DB_PARAMS["schema"],
    start_date=DATE["start"],
    end_date=DATE["end"]
)
month_etl_pipeline.run()

month_table = tables.MonthTable(df=month_etl_pipeline.get_df())
logger.info(f"MonthTable loaded {len(month_table.df)} rows")

logger.info("Running MunicipalityPipeline (municipality_dt)")
municipality_etl_pipeline = pipelines.MunicipalityPipeline(
    municipality_location_source=municipality_location_source,
    municipality_statistics_source=municipality_statistics_source,
    engine=engine,
    table_name="municipality_dt",
    schema=DB_PARAMS["schema"],
    location_table=location_table
)
municipality_etl_pipeline.run()

municipality_table = tables.MunicipalityTable(df=municipality_etl_pipeline.get_df())
logger.info(f"MunicipalityTable loaded {len(municipality_table.df)} municipalities")

logger.info("Initializing MunicipalityLocator from GeoJSON")
municipality_locator = MunicipalityLocator(geojson_path=MUNICIPALITY_GEOJSON_FILE)
logger.info("MunicipalityLocator ready")

logger.info("Running StationPipeline (station_dt)")
station_etl_pipeline = pipelines.StationPipeline(
    air_monitoring_source=air_monitoring_station_registry_source,
    weather_monitoring_source=weather_air_monitoring_station_registry_source,
    engine=engine,
    table_name="station_dt",
    schema=DB_PARAMS["schema"],
    location_table=location_table,
    municipality_table=municipality_table,
    municipality_locator=municipality_locator
)
station_etl_pipeline.run()

station_table = tables.StationTable(df=station_etl_pipeline.get_df())
logger.info(f"StationTable loaded {len(station_table.df)} stations")

logger.info("Running SensorPipeline (sensor_dt)")
sensor_etl_pipeline = pipelines.SensorPipeline(
    engine=engine,
    table_name="sensor_dt",
    schema=DB_PARAMS["schema"],
    municipality_table=municipality_table,
    air_monitoring_source=air_monitoring_station_registry_source,
    weather_monitoring_source=weather_air_monitoring_station_registry_source,
    station_table=station_table
)
sensor_etl_pipeline.run()

sensor_table = tables.SensorTable(df=sensor_etl_pipeline.get_df())
logger.info(f"SensorTable loaded {len(sensor_table.df)} sensors")

logger.info("Running PopulationPipeline (population_dt)")
population_etl_pipeline = pipelines.PopulationPipeline(
    census_source=census_etl_source,
    engine=engine,
    table_name="population_dt",
    schema=DB_PARAMS["schema"],
    municipality_table=municipality_table,
    date_table=date_table
)
population_etl_pipeline.run()
logger.info("PopulationPipeline completed")

logger.info("Running GatePipeline (gate_dt)")
gate_etl_pipeline = pipelines.GatePipeline(
    area_c_gate_source=area_c_gate_source,
    engine=engine,
    table_name="gate_dt",
    schema=DB_PARAMS["schema"],
    location_table=location_table
)
gate_etl_pipeline.run()
logger.info("GatePipeline completed")

logger.info("Running VehiclePipeline (vehicle_dt)")
vehicle_etl_pipeline = pipelines.VehiclePipeline(
    area_c_ingress_source=area_c_ingress_source,
    area_c_decode_source=area_c_decode_source,
    engine=engine,
    table_name="vehicle_dt",
    schema=DB_PARAMS["schema"]
)
vehicle_etl_pipeline.run()

vehicle_table = tables.VehicleTable(df=vehicle_etl_pipeline.get_df())
logger.info(f"VehicleTable loaded {len(vehicle_table.df)} vehicle types")

logger.info("Running SamplePipeline (sample_ft)")
sample_etl_pipeline = pipelines.SamplePipeline(
    weather_sample_source=weather_air_monitoring_sample_source,
    air_sample_source=air_monitoring_sample_source,
    engine=engine,
    table_name="sample_ft",
    schema=DB_PARAMS["schema"],
    date_table=date_table,
    sensor_table=sensor_table
)
sample_etl_pipeline.run()
logger.info("SamplePipeline completed")

logger.info("Running OvernightsPipeline (overnights_ft)")
overnights_etl_pipeline = pipelines.OvernightsPipeline(
    tourist_flows_source=tourism_flow_source,
    engine=engine,
    table_name="overnights_ft",
    schema=DB_PARAMS["schema"],
    municipality_table=municipality_table,
    month_table=month_table
)
overnights_etl_pipeline.run()
logger.info("OvernightsPipeline completed")

logger.info("Running GateAccessPipeline (gate_access_ft)")
gate_access_etl_pipeline = pipelines.GateAccessPipeline(
    area_c_ingress_source=area_c_ingress_source,
    area_c_decode_source=area_c_decode_source,
    engine=engine,
    table_name="gate_access_ft",
    schema=DB_PARAMS["schema"],
    vehicle_table=vehicle_table,
    date_table=date_table
)
gate_access_etl_pipeline.run()
