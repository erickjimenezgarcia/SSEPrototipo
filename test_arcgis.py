from arcgis.gis import GIS
import json, os
from dotenv import load_dotenv

load_dotenv()

USERNAME = os.getenv("ARCGIS_USER")
PASSWORD = os.getenv("ARCGIS_PASS")
ITEM_ID = os.getenv("ARCGIS_ITEM_ID")

# Conectarse a ArcGIS Enterprise
gis = GIS("https://geosunass.sunass.gob.pe/gisportal", 
          username=USERNAME, 
          password=PASSWORD)


def area_drenaje_geojson():
    """
    Devuelve un dict con el GeoJSON de la capa Área_de_drenaje (id 19).
    No devuelve JSONResponse ni nada de FastAPI, solo datos puros.
    """
    item = gis.content.get(ITEM_ID)
    if not item:
        # lanza excepción, la maneja la capa API
        raise ValueError("Item no encontrado")

    # buscar layer 19
    area_layer = None
    for lyr in item.layers:
        props = lyr.properties
        if getattr(props, "id", None) == 19 or getattr(props, "name", "") == "Área_de_drenaje":
            area_layer = lyr
            break

    if area_layer is None:
        raise ValueError("Layer 19 no encontrada")

    fs = area_layer.query(where="1=1", out_fields="*",out_sr=4326)
    geojson_str = fs.to_geojson
    geojson_dict = json.loads(geojson_str)

    return geojson_dict