import pandas as pd
from dataclasses import dataclass
from abc import ABC, abstractmethod
from tabulate import tabulate
from logger import logger

@dataclass
class Table(ABC):

    @abstractmethod
    def get_id(self, *args, **kwargs):
        pass

@dataclass
class LocationTable(Table):
    df: pd.DataFrame
    
    def get_id(self, latitude, longitude, altitude=None):
        if altitude is None:
            match = self.df[
                (self.df['latitude'] == latitude) &
                (self.df['longitude'] == longitude) &
                (self.df['altitude'].isna())
            ]
        else:
            match = self.df[
                (self.df['latitude'] == latitude) &
                (self.df['longitude'] == longitude) &
                (self.df['altitude'] == altitude)
            ]
        if match.empty:
            raise ValueError(f"No matching location found for latitude: {latitude}, longitude: {longitude}, altitude: {altitude}")
        return match.iloc[0]['location_id']
    
@dataclass
class MunicipalityTable(Table):

    df: pd.DataFrame
    
    def get_id(self, municipality_name):
        match = self.df[
            (self.df['name'] == municipality_name)
        ]
        if match.empty:
            return None
        return match.iloc[0]['municipality_id']
    
    def get_municipalities_id(self):
        municipalities = self.df['municipality_id'].dropna().unique()
        return sorted(municipalities.tolist())
    
    
@dataclass
class DateTable(Table):

    df: pd.DataFrame
    
    def get_id(self, day, month, year):
        match = self.df[
            (self.df['day'] == day) &
            (self.df['month'] == month) &
            (self.df['year'] == year)
        ]
        if match.empty:
            raise ValueError("No matching date found")
        return match.iloc[0]['date_id']

@dataclass
class VehicleTable(Table):
    
    df: pd.DataFrame

    def get_id(self, service, type, fuel, euro, fap, allowed, resident, category, area_class):
        match = self.df[
            (self.df['service'] == service) &
            (self.df['type'] == type) &
            (self.df['fuel'] == fuel) &
            (self.df['euro'] == euro) &
            (self.df['fap'] == fap) &
            (self.df['allowed'] == allowed) &
            (self.df['resident'] == resident) &
            (self.df['category'] == category) &
            (self.df['class'] == area_class)
        ]
        if match.empty:
            raise ValueError("No matching vehicle found")
        return match.iloc[0]['vehicle_id']

@dataclass
class MonthTable(Table):

    df: pd.DataFrame
    
    def get_id(self, year, month):
        match = self.df[
            (self.df['year'] == year) &
            (self.df['month'] == month)
        ]
        if match.empty:
            raise ValueError("No matching month found")
        return match.iloc[0]['month_id']

@dataclass
class StationTable(Table):

    df: pd.DataFrame
    
    def get_id(self, station_id):
        match = self.df[
            (self.df['station_id'] == station_id)
        ]
        if match.empty:
            return None
        return match.iloc[0]['station_id']
    

@dataclass
class SensorTable(Table):
    
    df: pd.DataFrame
        
    def get_id(self):
        sensors = self.df['sensor_id'].dropna().unique()
        return sorted(sensors.tolist())
