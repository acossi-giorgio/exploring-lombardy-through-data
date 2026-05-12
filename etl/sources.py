import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
import geopandas as gpd

@dataclass
class Source(ABC):
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        pass

@dataclass
class CSVSource(Source):
    file_path: str
    sep: str = ','
    encoding: str = 'utf-8'
    _df: Optional[pd.DataFrame] = field(init=False, default=None)

    def extract(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_csv(self.file_path, sep=self.sep, encoding=self.encoding, low_memory=False)
        return self._df.copy()

@dataclass
class AreaCGateRegistrySource(CSVSource):
    sep: str = ';'

@dataclass
class AreaCGateIngressSource(CSVSource):
    pass

@dataclass
class AreaCDecodeSource(CSVSource):
    sep: str = ';'

@dataclass
class CensusETLSource(CSVSource):
    sep: str = ';'

@dataclass
class TouristFlowsSource(CSVSource):
    pass

@dataclass
class MunicipalityStatisticsSource(CSVSource):
    pass

@dataclass
class WeatherMonitoringStationRegistrySource(CSVSource):
    pass

@dataclass
class WeatherMonitoringSampleSource(CSVSource):
    pass

@dataclass
class AirMonitoringStationRegistrySource(CSVSource):
    pass

@dataclass
class AirMonitoringSampleSource(CSVSource):
    pass

@dataclass
class MunicipalityLocationSource(Source):
    geojson_path: str
    _df: Optional[pd.DataFrame] = field(init=False, default=None)

    def extract(self) -> pd.DataFrame:
        if self._df is None:
            gdf = gpd.read_file(self.geojson_path)
            gdf = gdf[gdf["COD_REG"] == 3].copy()
            gdf = gdf.to_crs(epsg=4326)
            gdf_proj = gdf.to_crs(epsg=32632)
            centroids = gdf_proj.geometry.centroid
            centroids_wgs = gpd.GeoSeries(centroids, crs="EPSG:32632").to_crs(epsg=4326)
            self._df = pd.DataFrame({
                "municipality": gdf["COMUNE"].values,
                "latitude": centroids_wgs.y.values,
                "longitude": centroids_wgs.x.values
            })
        return self._df.copy()