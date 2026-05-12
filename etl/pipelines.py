import pandas as pd
from sqlalchemy import Engine
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import tables as tables
import sources as sources
from logger import logger
from utils import MunicipalityLocator

@dataclass
class Pipeline(ABC):
    
    def run(self):
        name = self.__class__.__name__
        logger.info(f"{name} - Starting extract")
        self.extract()
        logger.info(f"{name} - Starting transform")
        self.transform()
        logger.info(f"{name} - Starting load")
        self.load()
        logger.info(f"{name} - Completed")

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def get_df(self) -> pd.DataFrame:
        pass

@dataclass
class LocationPipeline(Pipeline):
    municipality_location_source: sources.Source
    municipality_statistics_source: sources.Source
    air_monitoring_source: sources.Source
    weather_monitoring_source: sources.Source
    area_c_gate_source: sources.Source
    engine: Engine
    table_name: str
    schema: str
    municipality_location_source_df: pd.DataFrame = field(init=False, default=None)
    municipality_statistics_df: pd.DataFrame = field(init=False, default=None)
    air_monitoring_df: pd.DataFrame = field(init=False, default=None)
    weather_monitoring_df: pd.DataFrame = field(init=False, default=None)
    area_c_gate_df: pd.DataFrame = field(init=False, default=None)
    location_df: pd.DataFrame = field(init=False, default=None)

    def extract(self):
        self.air_monitoring_df = self.air_monitoring_source.extract()
        self.weather_monitoring_df = self.weather_monitoring_source.extract()
        self.area_c_gate_df = self.area_c_gate_source.extract()
        self.municipality_statistics_df = self.municipality_statistics_source.extract()
        self.municipality_location_source_df = self.municipality_location_source.extract()

    def transform(self):
        municipality_df = self.municipality_statistics_df
        municipality_df = municipality_df[municipality_df['Codice Regione'] == 3].copy()
        municipality_df = pd.merge(
            municipality_df,
            self.municipality_location_source_df,
            left_on="Denominazione (Italiana e straniera)",
            right_on="municipality",
            how="left"
        )
        municipality_df = municipality_df.rename(columns={
            'Altitudine del centro (metri)': 'altitude'
        })
        municipality_df['altitude'] = municipality_df['altitude'].replace('N.D.', 0)
        municipality_df['altitude'] = municipality_df['altitude'].str.replace(',', '', regex=False).astype(float)
        municipality_df = municipality_df[['latitude', 'longitude', 'altitude']]
        self.weather_monitoring_df = self.weather_monitoring_df[['lat', 'lng', 'Quota']]
        self.weather_monitoring_df = self.weather_monitoring_df.rename(columns={
            'lat': 'latitude',
            'lng': 'longitude',
            'Quota': 'altitude'
        })
        self.air_monitoring_df = self.air_monitoring_df[['lat', 'lng', 'Quota']]
        self.air_monitoring_df = self.air_monitoring_df.rename(columns={
            'lat': 'latitude',
            'lng': 'longitude',
            'Quota': 'altitude'
        })
        self.area_c_gate_df = self.area_c_gate_df[['LAT_Y_4326', 'LONG_X_4326']]
        self.area_c_gate_df = self.area_c_gate_df.rename(columns={
            'LAT_Y_4326': 'latitude',
            'LONG_X_4326': 'longitude'
        })
        self.location_df = pd.concat([self.weather_monitoring_df, self.air_monitoring_df, self.area_c_gate_df, municipality_df], ignore_index=True)
        self.location_df = self.location_df.drop_duplicates()
        self.location_df = self.location_df.dropna(subset=['latitude', 'longitude'])
        self.location_df = self.location_df.reset_index(drop=True)
        self.location_df.insert(0, "location_id", range(1, len(self.location_df) + 1))
        self.location_df = self.location_df.astype(object).where(pd.notnull(self.location_df), None)

    def load(self):
        self.location_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )

    def get_df(self) -> pd.DataFrame:
        return self.location_df.copy()

@dataclass
class DatePipeline(Pipeline):
    engine: Engine
    table_name: str
    schema: str
    start_date: str
    end_date: str
    date_rage: pd.DatetimeIndex = field(init=False, default=None)
    date_df: pd.DataFrame = field(init=False, default=None)

    def extract(self):
        self.date_range = pd.date_range(start=self.start_date, end=self.end_date)

    def transform(self):
        self.date_df = pd.DataFrame({
            'date_id': range(1, len(self.date_range) + 1),
            'date': self.date_range,
            'day': self.date_range.day.astype(int),
            'month': self.date_range.month.astype(int),
            'year': self.date_range.year.astype(int),
        })

    def load(self):
        self.date_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )

    def get_df(self) -> pd.DataFrame:
        return self.date_df.copy()

@dataclass
class MonthPipeline(Pipeline):
    engine: Engine
    table_name: str
    schema: str
    start_date: str
    end_date: str
    date_rage: pd.DatetimeIndex = field(init=False, default=None)
    month_df: pd.DataFrame = field(init=False, default=None)

    def extract(self):
        self.date_range = pd.date_range(start=self.start_date, end=self.end_date)

    def transform(self):
        self.month_df = pd.DataFrame({
            'month': self.date_range.month.astype(int),
            'year': self.date_range.year.astype(int),
        })
        self.month_df = self.month_df.drop_duplicates()
        self.month_df['month_id'] = range(1, len(self.month_df) + 1)

    def load(self):
        self.month_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )
    
    def get_df(self) -> pd.DataFrame:
        return self.month_df.copy()

@dataclass
class MunicipalityPipeline(Pipeline):
    municipality_location_source: sources.Source
    municipality_statistics_source: sources.Source
    engine: Engine
    table_name: str
    schema: str
    location_table: tables.LocationTable
    municipality_location_source_df: pd.DataFrame = field(init=False, default=None)
    municipality_statistics_df: pd.DataFrame = field(init=False, default=None)
    municipality_df: pd.DataFrame = field(init=False, default=None)

    def extract(self):
        self.municipality_statistics_df = self.municipality_statistics_source.extract()
        self.municipality_location_source_df = self.municipality_location_source.extract()

    def transform(self):
        stats_df = self.municipality_statistics_df
        stats_df = stats_df[stats_df["Codice Regione"] == 3].copy()

        stats_df['municipality_id'] = stats_df['Codice Istat del Comune (alfanumerico)']
        stats_df['name'] = stats_df['Denominazione (Italiana e straniera)']
        stats_df["area"] = stats_df["Superficie territoriale (kmq) al 01/01/2024"].str.replace(",", ".").astype(float)
        stats_df["urbanization"] = stats_df["Grado di urbanizzazione"].astype(int)
        stats_df["littoral"] = stats_df["Comune litoraneo"].astype(bool)
        stats_df["isle"] = stats_df["Comune isolano"].astype(bool)
        stats_df["coast"] = stats_df["Zone costiere"].astype(bool)
        stats_df["altitude_zone"] = stats_df["Zona altimetrica"].astype(int)
        stats_df = stats_df[stats_df['Altitudine del centro (metri)'] != 'N.D.']
        stats_df["altitude"] = stats_df["Altitudine del centro (metri)"].str.replace(',', '', regex=False).astype(float)
        stats_df = stats_df[['municipality_id', 'name', 'area', 'urbanization', 'littoral', 'isle', 'coast', 'altitude_zone', 'altitude']]

        urbanization_map = {
            1: "Città/Zone densamente popolate",
            2: "Piccole città/sobborghi o Zone a densità intermedia di popolazione",
            3: "Zone rurali/sparse"
        }
        stats_df["urbanization"] = stats_df["urbanization"].map(urbanization_map)

        altitude_zone_map = {
            1: "Montagna interna",
            2: "Montagna litoranea",
            3: "Collina interna",
            4: "Collina litoranea",
            5: "Pianura"
        }
        stats_df["altitude_zone"] = stats_df["altitude_zone"].map(altitude_zone_map)

        location_df = self.municipality_location_source_df[['municipality', 'latitude', 'longitude']]

        merged_df = pd.merge(
            stats_df,
            location_df,
            left_on="name",
            right_on="municipality",
            how="left"
        )

        merged_df = merged_df.dropna(subset=['latitude', 'longitude']).copy()

        merged_df["location_id"] = merged_df.apply(
            lambda row: self.location_table.get_id(
                latitude=row["latitude"],
                longitude=row["longitude"],
                altitude=row["altitude"]
            ),
            axis=1,
            result_type='reduce'
        )

        self.municipality_df = merged_df[['municipality_id', 'name', 'area', 'urbanization', 'littoral', 'isle', 'coast', 'altitude_zone', 'location_id']]

    def load(self):
        self.municipality_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )
        
    def get_df(self) -> pd.DataFrame:
        return self.municipality_df.copy()


@dataclass
class StationPipeline(Pipeline):
    air_monitoring_source: sources.Source
    weather_monitoring_source: sources.Source
    engine: Engine
    table_name: str
    schema: str
    location_table: tables.LocationTable
    municipality_table: tables.MunicipalityTable
    municipality_locator: MunicipalityLocator = field(default=None)
    air_df: pd.DataFrame = field(init=False, default=None)
    weather_df: pd.DataFrame = field(init=False, default=None)
    station_df: pd.DataFrame = field(init=False, default=None)

    def extract(self):
        self.air_df = self.air_monitoring_source.extract()
        self.weather_df = self.weather_monitoring_source.extract()

    def transform(self):

        self.air_df = self.air_df[['Idstazione', 'NomeStazione', 'Comune', 'lat', 'lng', 'Quota']]
        self.air_df = self.air_df.rename(columns={
            'Idstazione': 'IdStazione'
        })
        self.weather_df = self.weather_df[['IdStazione', 'NomeStazione', 'lat', 'lng', 'Quota']]
        self.weather_df = self.weather_df[(self.weather_df['lat'] != 0.0) | (self.weather_df['lng'] != 0.0)]

        def resolve_municipality(row):
            municipality = self.municipality_locator.find_municipality(row['lat'], row['lng'])
            if municipality is None:
                logger.warning(f"Coordinate ({row['lat']}, {row['lng']}) not in Lombardy region")
                return None
            municipality_id = self.municipality_table.get_id(municipality_name=municipality)
            if municipality_id is None:
                logger.warning(f"Municipality {municipality} not in Lombardy region")
                return None
            return municipality

        self.weather_df['Comune'] = self.weather_df.apply(resolve_municipality, axis=1)
        self.weather_df = self.weather_df.dropna(subset=['Comune'])

        merged_df = pd.concat([self.air_df, self.weather_df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=['IdStazione'], keep='first')
        merged_df = merged_df[['IdStazione', 'NomeStazione', 'Comune', 'lat', 'lng', 'Quota']]

        # corner case
        merged_df['Comune'] = merged_df['Comune'].replace('Cornale', 'Cornale e Bastida')
        merged_df['Comune'] = merged_df['Comune'].replace('Borgofranco sul Po', 'Borgocarbonara')
        merged_df['Comune'] = merged_df['Comune'].replace('Galliate', 'Galliate Lombardo')
        merged_df['Comune'] = merged_df['Comune'].replace('Pieve di Coriano', 'Borgo Mantovano')
        merged_df['Comune'] = merged_df['Comune'].replace('Piadena', 'Piadena Drizzona')
        merged_df['Comune'] = merged_df['Comune'].replace('Orio Litta SS234', 'Orio Litta')
        merged_df['Comune'] = merged_df['Comune'].replace('Zona Omogenea Milano Nord', 'Milano')
        merged_df['Comune'] = merged_df['Comune'].replace('Unione dei comuni della Valvarrone', 'Valvarrone')
        merged_df['Comune'] = merged_df['Comune'].replace('Toscolano Maderno', 'Toscolano-Maderno')
        merged_df['Comune'] = merged_df['Comune'].replace('Gera Lario Ponte del Passo', 'Gera Lario')
        merged_df['Comune'] = merged_df['Comune'].replace('Cinisello Balsamo Parco Nord', 'Cinisello Balsamo')
        merged_df['Comune'] = merged_df['Comune'].replace('Paderno Dugnano Palazzolo', 'Paderno Dugnano')
        merged_df['Comune'] = merged_df['Comune'].replace('Sueglio Monte Letee', 'Sueglio')
        merged_df['Comune'] = merged_df['Comune'].replace('Paderno Dugnano Palazzolo Parco Borghetto', 'Paderno Dugnano')
        merged_df['Comune'] = merged_df['Comune'].replace('Lavena Ponte Tresa depuratore', 'Lavena Ponte Tresa')

        merged_df = merged_df[~merged_df['Comune'].isin(['Melara', 'Ceneselli', 'Salionze', 'Circolo di Sessa', 'ISOLA S.ANTONIO SS 211'])]

        merged_df = merged_df.drop_duplicates()
        merged_df = merged_df.astype(object).where(pd.notnull(merged_df), None)

        merged_df["location_id"] = merged_df.apply(
            lambda row: self.location_table.get_id(
                latitude=row["lat"],
                longitude=row["lng"],
                altitude=row['Quota']
            ),
            axis=1
        )

        merged_df['municipality_id'] = merged_df.apply(
            lambda row: self.municipality_table.get_id(municipality_name=row["Comune"]),
            axis=1
        )

        merged_df = merged_df.rename(columns={
            'IdStazione': 'station_id',
            'NomeStazione': 'name',
        })
        
        air_ids = set(self.air_df['IdStazione'])
        weather_ids = set(self.weather_df['IdStazione'])

        def get_type(station_id):
            in_air = station_id in air_ids
            in_weather = station_id in weather_ids
            match (in_air, in_weather):
                case (True, True):
                    return 'air/weather'
                case (True, False):
                    return 'air'
                case (False, True):
                    return 'weather'
                case _:
                    return 'unknown'

        merged_df['type'] = merged_df['station_id'].apply(get_type)

        self.station_df = merged_df[['station_id', 'name', 'municipality_id', 'location_id', 'type']]
        
    def load(self):
        self.station_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )
    
    def get_df(self) -> pd.DataFrame:
        return self.station_df.copy()
    
@dataclass
class SensorPipeline(Pipeline):
    engine: Engine
    table_name: str
    schema: str
    municipality_table: tables.LocationTable
    air_monitoring_source: sources.Source
    weather_monitoring_source: sources.Source
    station_table: tables.StationTable
    air_df: pd.DataFrame = field(init=False, default=None)
    weather_df: pd.DataFrame = field(init=False, default=None)
    sensor_df: pd.DataFrame = field(init=False, default=None)
        
    def extract(self):
        self.air_df = self.air_monitoring_source.extract()
        self.weather_df = self.weather_monitoring_source.extract()

    def transform(self):
        self.air_df = self.air_df[['IdSensore', 'Idstazione', 'DataStart', 'DataStop', 'UnitaMisura', 'NomeTipoSensore']]
        self.air_df['type'] = 'air'
        self.weather_df = self.weather_df[['IdSensore', 'IdStazione', 'DataStart', 'DataStop', 'Unità DiMisura', 'Tipologia']]
        self.weather_df = self.weather_df.rename(columns={
            'IdStazione': 'Idstazione',
            'Unità DiMisura': 'UnitaMisura',
            'Tipologia': 'NomeTipoSensore'
        })
        self.weather_df['type'] = 'weather'
        self.sensor_df = pd.concat([self.air_df, self.weather_df], ignore_index=True)
        self.sensor_df = self.sensor_df[self.sensor_df.apply(
            lambda row: self.station_table.get_id(station_id=row['Idstazione']) is not None,
            axis=1
        )]
        self.sensor_df = self.sensor_df.drop_duplicates()
        self.sensor_df = self.sensor_df.astype(object).where(pd.notnull(self.sensor_df), None)
        self.sensor_df = pd.DataFrame({
            'sensor_id': self.sensor_df['IdSensore'],
            'name': self.sensor_df['NomeTipoSensore'],
            'type': self.sensor_df['type'],
            'date_start': pd.to_datetime(self.sensor_df['DataStart'], format='%d/%m/%Y', errors='coerce'),
            'date_stop': pd.to_datetime(self.sensor_df['DataStop'], format='%d/%m/%Y', errors='coerce'),
            'uom': self.sensor_df['UnitaMisura'],
            'station_id': self.sensor_df['Idstazione']
        })  

    def load(self):
        self.sensor_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )
    
    def get_df(self) -> pd.DataFrame:
        return self.sensor_df.copy()

@dataclass
class PopulationPipeline(Pipeline):
    census_source: sources.Source
    engine: Engine
    table_name: str
    schema: str
    municipality_table: tables.MunicipalityTable
    date_table: tables.DateTable
    population_df: pd.DataFrame = field(init=False, default=None)
    
    def extract(self):
        self.census_df = self.census_source.extract()

    def transform(self):
        municipalities = self.municipality_table.get_municipalities_id()
        self.census_df = self.census_df[['Codice comune', 'Comune', 'Popolazione censita al 31 dicembre - Totale']]
        self.census_df = self.census_df[self.census_df["Codice comune"].isin(municipalities)]
        self.census_df['date_id'] = self.date_table.get_id(31, 12, 2023)
        self.census_df = self.census_df.rename(columns={
            'Codice comune': 'municipality_id',
            'Popolazione censita al 31 dicembre - Totale': 'population'
        })
        self.population_df = self.census_df[['municipality_id', 'population', 'date_id']]
        self.population_df = self.population_df.drop_duplicates()
        self.population_df.insert(0, "population_id", range(1, len(self.population_df) + 1))

    def load(self):
        self.population_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )
    
    def get_df(self) -> pd.DataFrame:
        return self.population_df.copy()

@dataclass
class GatePipeline(Pipeline):
    area_c_gate_source: sources.Source
    engine: Engine
    table_name: str
    schema: str
    location_table: tables.LocationTable
    area_c_gate_source_df: pd.DataFrame = field(init=False, default=None)
    gate_df: pd.DataFrame = field(init=False, default=None)
    
    def extract(self):
        self.area_c_gate_source_df = self.area_c_gate_source.extract()

    def transform(self):
        self.area_c_gate_source_df['location_id'] = self.area_c_gate_source_df.apply(
            lambda row: self.location_table.get_id(row['LAT_Y_4326'], row['LONG_X_4326']),
            axis=1
        )
        self.gate_df = pd.DataFrame({
            'gate_id': self.area_c_gate_source_df['id_amat'],
            'name': self.area_c_gate_source_df['label'],
            'location_id': self.area_c_gate_source_df['location_id'],
        })
            
    def load(self):
        self.gate_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )

    def get_df(self) -> pd.DataFrame:
        return self.gate_df.copy()

@dataclass
class VehiclePipeline(Pipeline):
    area_c_ingress_source: sources.Source
    area_c_decode_source: sources.Source
    engine: Engine
    table_name: str
    schema: str
    area_c_ingress_source_df: pd.DataFrame = field(init=False, default=None)
    area_c_decode_source_df: pd.DataFrame = field(init=False, default=None)
    vehicle_df: pd.DataFrame = field(init=False, default=None)

    def extract(self):
        self.area_c_ingress_source_df = self.area_c_ingress_source.extract()
        self.area_c_decode_source_df = self.area_c_decode_source.extract()

    def transform(self):
        def decode_field(name, value):
            decodes = self.area_c_decode_source_df[self.area_c_decode_source_df['fieldname'] == name]
            if decodes.empty:
                return value
            match = decodes[decodes['id_amat'] == value]
            return match['descrizione'].values[0] if not match.empty else value

        self.area_c_ingress_source_df = self.area_c_ingress_source_df[['esenti', 'moto', 'residenti', 'veicoli_servizio', 'categoria_euro', 'tipologia_alimentazione', 'categoria_veicolo', 'classe_areac', 'fap']]
        self.area_c_ingress_source_df = self.area_c_ingress_source_df.drop_duplicates()

        self.vehicle_df = pd.DataFrame({
            'vehicle_id': range(1, len(self.area_c_ingress_source_df) + 1),
            'service': self.area_c_ingress_source_df['veicoli_servizio'].apply(lambda row: decode_field('veicoli_servizio', row)),
            'type': self.area_c_ingress_source_df['moto'].apply(lambda row: decode_field('moto', row)),
            'fuel': self.area_c_ingress_source_df['tipologia_alimentazione'].apply(lambda row: decode_field('tipologia_alimentazione', row)),
            'euro': self.area_c_ingress_source_df['categoria_euro'].apply(lambda row: decode_field('categoria_euro', row)),
            'fap': self.area_c_ingress_source_df['fap'].apply(lambda row: decode_field('fap', row)),
            'allowed': self.area_c_ingress_source_df['esenti'].apply(lambda row: decode_field('esenti', row)),
            'resident': self.area_c_ingress_source_df['residenti'].apply(lambda row: decode_field('residenti', row)),
            'category': self.area_c_ingress_source_df['categoria_veicolo'].apply(lambda row: decode_field('categoria_veicolo', row)),
            'class': self.area_c_ingress_source_df['classe_areac'].apply(lambda row: decode_field('classe_areac', row))
        })


    def load(self):
        self.vehicle_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists="append",
            index=False
        )

    def get_df(self) -> pd.DataFrame:
        return self.vehicle_df.copy()
    
@dataclass
class SamplePipeline(Pipeline):
    weather_sample_source: sources.Source
    air_sample_source: sources.Source
    engine: Engine
    table_name: str
    schema: str
    date_table: tables.DateTable
    sensor_table: tables.SensorTable
    weather_sample_df: pd.DataFrame = field(init=False, default=None)
    air_sample_df: pd.DataFrame = field(init=False, default=None)
    sample_df: pd.DataFrame = field(init=False, default=None)
    
    def extract(self):
        self.weather_sample_df = self.weather_sample_source.extract()
        self.air_sample_df = self.air_sample_source.extract()

    def transform(self):        
        self.weather_sample_df = self.weather_sample_df[['idSensore', 'Data', 'Valore', 'Stato']]  # idSensore, Data, Valore, Stato
        self.air_sample_df = self.air_sample_df[['idSensore', 'Data', 'Valore', 'Stato']]  # idSensore, Data, Valore, Stato, idOperatore
        tmp_df = pd.concat([self.weather_sample_df, self.air_sample_df], ignore_index=True)

        sensors_id = self.sensor_table.get_id()
        tmp_df = tmp_df[tmp_df['idSensore'].isin(sensors_id)]
        tmp_df = tmp_df[tmp_df["Stato"] == "VA"]  
        tmp_df["Data"] = pd.to_datetime(tmp_df["Data"], utc=True, errors='coerce')
        tmp_df["date"] = tmp_df["Data"].dt.date

        agg_df = tmp_df.groupby(["idSensore", "date"]).agg(
            mean_value=("Valore", "mean"),
            min_value=("Valore", "min"),
            max_value=("Valore", "max"),
            std_dev=("Valore", "std")
        ).reset_index()

        agg_df = agg_df.round(3)

        agg_df["date_id"] = agg_df.apply(
            lambda row: self.date_table.get_id(
                day=row["date"].day,
                month=row["date"].month,
                year=row["date"].year
            ),
            axis=1
        )        
        self.sample_df = pd.DataFrame({
            'sensor_id': agg_df['idSensore'],
            'date_id': agg_df['date_id'],
            'mean_value': agg_df['mean_value'],
            'min_value': agg_df['min_value'],
            'max_value': agg_df['max_value'],
            'std_dev': agg_df['std_dev']
        })
        
    def load(self):
        self.sample_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )
    
    def get_df(self) -> pd.DataFrame:
        return self.sample_df.copy()

@dataclass
class GateAccessPipeline(Pipeline):
    area_c_ingress_source: sources.Source
    area_c_decode_source: sources.Source
    engine: Engine
    table_name: str
    schema: str
    vehicle_table: tables.VehicleTable
    date_table: tables.DateTable
    area_c_decode_source_df: pd.DataFrame = field(init=False, default=None)
    area_c_ingress_source_df: pd.DataFrame = field(init=False, default=None)
    gate_access_df: pd.DataFrame = field(init=False, default=None)
    
    def extract(self):
        self.area_c_ingress_source_df = self.area_c_ingress_source.extract()
        self.area_c_decode_source_df = self.area_c_decode_source.extract()

    def transform(self):
        def decode_field(name, value):
            decodes = self.area_c_decode_source_df[self.area_c_decode_source_df['fieldname'] == name]
            if decodes.empty:
                return value
            match = decodes[decodes['id_amat'] == value]
            return match['descrizione'].values[0] if not match.empty else value
        
        df = self.area_c_ingress_source_df.copy()
        df["dataora"] = pd.to_datetime(df["dataora"], utc=True, errors='coerce')
        df["dataora"] = df["dataora"].dt.tz_convert('Europe/Rome') # TODO check if is necessary
        df["data"] = df["dataora"].dt.date
        
        df["numero_transiti"] = df["numero_transiti"].astype(str)
        df["numero_transiti"] = df["numero_transiti"].str.replace(r"[^\d]", "", regex=True)
        df["numero_transiti"] = df["numero_transiti"].astype(int)

        df = df.groupby(['id_varco', 'data', 'veicoli_servizio', 'moto', 'tipologia_alimentazione', 'categoria_euro', 'fap', 'esenti', 'residenti', 'categoria_veicolo', 'classe_areac'], as_index=False).agg(
            count=("numero_transiti", "sum")
        )
        
        df["date_id"] = df.apply(
            lambda row: self.date_table.get_id(
                day=row["data"].day,
                month=row["data"].month,
                year=row["data"].year
            ),
            axis=1
        )

        df["vehicle_id"] = df.apply(
            lambda row: self.vehicle_table.get_id(
                service=decode_field("veicoli_servizio", row["veicoli_servizio"]),
                type=decode_field("moto", row["moto"]),
                fuel=decode_field("tipologia_alimentazione", row["tipologia_alimentazione"]),
                euro=decode_field("categoria_euro", row["categoria_euro"]),
                fap=decode_field("fap", row["fap"]),
                allowed=decode_field("esenti", row["esenti"]),
                resident=decode_field("residenti", row["residenti"]),
                category=decode_field("categoria_veicolo", row["categoria_veicolo"]),
                area_class=decode_field("classe_areac", row["classe_areac"])
            ),
            axis=1
        )
        
        df = df.rename(columns={
            'id_varco': 'gate_id',
            'numero_transiti': 'count'
        })
        self.gate_access_df = df[['gate_id', 'date_id', 'vehicle_id', 'count']]

    def load(self):
        self.gate_access_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )
        
    def get_df(self) -> pd.DataFrame:
        return self.gate_access_df.copy()

@dataclass
class OvernightsPipeline(Pipeline):
    tourist_flows_source: sources.Source
    engine: Engine
    table_name: str 
    schema: str
    municipality_table: tables.MunicipalityTable
    month_table: tables.LocationTable
    tourist_flows_df: pd.DataFrame = field(init=False, default=None)
    overnights_df: pd.DataFrame = field(init=False, default=None)
    
    def extract(self):
        self.tourist_flows_df = self.tourist_flows_source.extract()

    def transform(self):
        def month_to_number(month: str) -> int:
            months = {
                "gennaio": 1,
                "febbraio": 2,
                "marzo": 3,
                "aprile": 4,
                "maggio": 5,
                "giugno": 6,
                "luglio": 7,
                "agosto": 8,
                "settembre": 9,
                "ottobre": 10,
                "novembre": 11,
                "dicembre": 12
            }
            return months.get(month.lower(), None)
    
        df = self.tourist_flows_df.copy()
        df = df[df['Anno'] == 2023]
        municipalities = self.municipality_table.get_municipalities_id()
        df = df[df['Codice ISTAT'].isin(municipalities)]
    
        df = df[['Codice ISTAT', 'Mese', 'Anno', 'Presenze']]
        df['month_id'] = df.apply(
            lambda row: self.month_table.get_id(
                month=month_to_number(row['Mese']),
                year=row['Anno']
            ),
            axis=1
        )
        df = df.rename(columns={
            'Codice ISTAT': 'municipality_id',
            'Presenze': 'count'
        })
        df['count'] = df['count'].astype(int)
        self.overnights_df = df[['municipality_id', 'month_id', 'count']]

    def load(self):
        self.overnights_df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False
        )
    
    def get_df(self) -> pd.DataFrame:
        return self.overnights_ft_df.copy()