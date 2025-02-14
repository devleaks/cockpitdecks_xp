import requests
import logging
import base64

logging.basicConfig(level=logging.INFO)


DATA = "data"


# See: https://stackoverflow.com/questions/1305532/how-to-convert-a-nested-python-dict-to-object/1305663#1305663
# Alternative
# https://stackoverflow.com/questions/4984647/accessing-dict-keys-like-an-attribute
class DatarefInternal(object):
    def __init__(self, d):
        for k, v in d.items():
            if isinstance(k, (list, tuple)):
                setattr(self, k, [obj(x) if isinstance(x, dict) else x for x in v])
            else:
                setattr(self, k, obj(v) if isinstance(v, dict) else v)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({ {a: getattr(self, a) for a in dir(self) if not a.startswith('__')} })"


class XPlaneDatarefDatabase:

    def __init__(self, api_url: str, update: bool = False) -> None:
        """Loads all dataref definitions for later use

        Args:
            api_url (str): Like so: api_url = "http://192.168.1.140:8080/api/v1"
            update (bool): [description] (default: `False`)
        """
        self.api_url = api_url.rstrip("/")  # remove trailing / to be sure
        self._datarefs = {}
        if update:
            self.update()

    def update(self):
        response = requests.get(self.api_url + "/datarefs")
        resp = response.json()
        dl = resp.get(DATA, {})
        self._datarefs = {d.get("name"): DatarefInternal(d) for d in dl}

    def _get_value(self, ref: DatarefInternal) -> int | float | str | None:
        url = f"{self.api_url}/datarefs/{ref.id}/value"
        response = requests.get(url)
        data = response.json()
        if DATA in data:
            if type(data[DATA]) in [str, bytes]:
                return base64.b64decode(data[DATA])[:-1].decode("ascii").strip("\u0000")
            return data[DATA]
        logger.error(f"no value for {ref.name}")
        return None

    def get_definition(self, name):
        return self._datarefs.get(name)

    def get_type(self, name, subtype: bool = False):
        d = self._datarefs.get(name)
        if d.value_type in ["data", "str", "string"]:
            return str
        elif d.value_type in ["double", "float"]:
            return float
        elif d.value_type == "int":
            return int
        elif d.value_type.endswith("_array"):
            if subtype:
                subt = d.value_type.replace("_array", "")
                if subt in ["data", "str", "string"]:
                    subt = str
                elif subt in ["double", "float"]:
                    subt = float
                elif subt == "int":
                    subt = int
                else:
                    subt = None
                return subt
            return list
        else:
            return None

    def get_value(self, name):
        return self._get_value(ref=self.get_definition(name=name))


if __name__ == "__main__":
    api_url = "http://192.168.1.140:8080/api/v1"
    w = XPlaneDatarefDatabase(api_url=api_url, update=True)
    print(len(w._datarefs))

    for dref in ["sim/flightmodel/position/latitude", "sim/aircraft/view/acf_ICAO", "AirbusFBW/PopUpStateArray"]:
        d = w.get_definition(name=dref)
        print(d)
        print(d.name, d.id)
        v = w.get_value(name=dref)
        print(v, type(v), w.get_type(name=dref, subtype=True))
