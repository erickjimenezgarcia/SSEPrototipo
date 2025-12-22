from arcgis.gis import GIS
import json, os, requests
from dotenv import load_dotenv

load_dotenv()

USERNAME = os.getenv("ARCGIS_USER")
PASSWORD = os.getenv("ARCGIS_PASS")
ITEM_ID = os.getenv("ARCGIS_ITEM_ID")
ITEM_ID_SECTORES = os.getenv("ITEM_ID_SECTORES")


# Conectarse a ArcGIS Enterprise
gis = GIS("https://geosunass.sunass.gob.pe/gisportal", 
          username=USERNAME, 
          password=PASSWORD)



def listar_layers(item_id: str):
    """Para ver IDs y nombres de sublayers y elegir el correcto."""
    item = gis.content.get(item_id)
    if not item:
        raise ValueError(f"Item no encontrado: {item_id}")

    info = []
    for lyr in getattr(item, "layers", []):
        p = lyr.properties
        info.append({
            "id": getattr(p, "id", None),
            "name": getattr(p, "name", None),
            "geometryType": getattr(p, "geometryType", None),
            "url": getattr(lyr, "url", None)
        })
    return info


def layer_a_geojson(item_id: str, layer_id: int | None = None, layer_name: str | None = None, where: str = "1=1"):
    """
    Devuelve GeoJSON (dict) de un sublayer de un item.
    - layer_id: el 'id' del sublayer (ej: 0, 19, etc.)
    - layer_name: nombre de capa si prefieres buscar por nombre
    """
    item = gis.content.get(item_id)
    if not item:
        raise ValueError(f"Item no encontrado: {item_id}")

    layers = getattr(item, "layers", [])
    if not layers:
        raise ValueError("El item no tiene layers (¿seguro es Feature Service/Map Service?)")

    target = None
    for lyr in layers:
        p = lyr.properties
        if layer_id is not None and getattr(p, "id", None) == layer_id:
            target = lyr
            break
        if layer_name and str(getattr(p, "name", "")).strip().lower() == layer_name.strip().lower():
            target = lyr
            break

    # Si no especificas nada, toma el primer sublayer
    if target is None and layer_id is None and not layer_name:
        target = layers[0]

    if target is None:
        raise ValueError(f"No encontré el sublayer (layer_id={layer_id}, layer_name={layer_name}). "
                         f"Usa listar_layers() para ver disponibles.")

    # OJO: return_all_records=True evita el límite típico (maxRecordCount)
    fs = target.query(
        where=where,
        out_fields="*",
        out_sr=4326,
        return_geometry=True,
        return_all_records=True
    )

    geojson_str = fs.to_geojson
    return json.loads(geojson_str)


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

def area_sectores_geojson():
    item = gis.content.get(ITEM_ID_SECTORES)
    if not item:
        raise ValueError("Item no encontrado")

    layer = item.layers[0]  # sublayer 0

    max_rc = getattr(layer.properties, "maxRecordCount", 2000)
    url = f"{layer.url}/query"

    token = getattr(gis._con, "token", None)

    offset = 0
    all_features = []

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": max_rc,
        }
        if token:
            params["token"] = token

        r = requests.get(url, params=params, timeout=120)
        r.raise_for_status()
        gj = r.json()

        feats = gj.get("features", [])
        all_features.extend(feats)

        # corte por paginación
        exceeded = gj.get("exceededTransferLimit", False)
        if (not exceeded) and (len(feats) < max_rc):
            break

        if len(feats) == 0:
            break

        offset += len(feats)

    return {"type": "FeatureCollection", "features": all_features}