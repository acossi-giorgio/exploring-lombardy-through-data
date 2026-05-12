from dataclasses import dataclass
import geopandas as gpd
from shapely.geometry import Point

@dataclass
class MunicipalityLocator:
    geojson_path: str
    gdf: gpd.GeoDataFrame = None
    sindex: any = None

    def __post_init__(self):
        self.gdf = gpd.read_file(self.geojson_path)
        self.gdf = self.gdf.to_crs(epsg=4326)
        self.sindex = self.gdf.sindex

    def find_municipality(self, latitude: float, longitude: float) -> str | None:
        if latitude == 0.0 and longitude == 0.0:
            return None
        point = Point(longitude, latitude)
        candidates_idx = list(self.sindex.intersection(point.bounds))
        candidates = self.gdf.iloc[candidates_idx]
        match = candidates[candidates.contains(point)]
        if match.empty:
            return None
        return match.iloc[0]["COMUNE"]