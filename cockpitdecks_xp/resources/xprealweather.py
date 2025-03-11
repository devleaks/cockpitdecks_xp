"""
A METAR is a weather situation at a named location, usually an airport.
"""

from __future__ import annotations
import logging
import re
from threading import Lock
from datetime import datetime, timezone
from enum import Enum
from typing import List, Any

import requests

# import io
# from tabulate import tabulate
from avwx import Station, Metar

from cockpitdecks.resources.weather import WeatherData
from cockpitdecks.resources.geo import distance

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


class WEATHER_LOCATION(Enum):
    # From X-Plane, two sources of weather informations
    #
    AIRCRAFT = "aircraft"
    REGION = "region"


def nowutc() -> datetime:
    return datetime.now(timezone.utc)


class XPRealWeatherData(WeatherData):
    # Data accessor shell class.
    # Must be supplied with type/mode of weather (aircraft or region)
    # and wether to force update on first start
    # Make dataref accessible through instance attributes like weather.temp.
    #

    CLOUD_LAYERS = 3
    WIND_LAYERS = 13

    def __init__(self, name: str, simulator, weather_type: str = WEATHER_LOCATION.REGION.value, update: bool = True):
        WeatherData.__init__(self, name=name, config={})
        self.simulator = simulator  # where to get data from

        self.imperial = False  # currently unused, reminder to think about it (METAR differ in US/rest of the world)
        self.move_with_aircraft = False
        self.min_distance_km = 30  # km, airliner = 900km/h, 15km/min, check occurs every minute

        self._station = None

        self.xp_real_weather_type = weather_type
        self.xp_real_weather: XPRealWeatherDatarefs | None = None
        self.wind_layers: List[WindLayer] = []  #  Defined wind layers. Not all layers are always defined. up to 13 layers(!)
        self.cloud_layers: List[CloudLayer] = []  #  Defined cloud layers. Not all layers are always defined. up to 3 layers

        self.generated_metar_raw: str | None = None
        self.previous_weather = []

        # working variables
        self.reading_datarefs = Lock()
        self._weather_last_checked = None  # None means either never collected and/or force collection

        # Meta data
        self._last_position: tuple = (0, 0)

        # Debugging values
        # self._check_freq = 30 # seconds
        # self._station_check_freq = 60  # seconds
        # self._weather_check_freq = 90  # seconds

    @property
    def connected(self):
        if type(self.simulator) is str:  # cheat for testing
            return True
        return self.simulator.connected

    @property
    def api_url(self) -> str | None:
        if type(self.simulator) is str:  # cheat for testing
            return self.simulator
        return self.simulator.api_url + "/datarefs" if self.simulator.api_url is not None else None

    @property
    def label(self):
        return self.station.icao if self.station is not None else "Weather"

    def metar(self) -> str | None:
        """Returns raw METAR if available"""
        return self.generated_metar_raw

    # ################################################
    # WeatherData Interface
    #
    def set_station(self, station: Any):
        if type(station) is Station:
            self.station = station
            return
        newstation = Station.from_icao(ident=station)
        if newstation is not None:
            self.station = newstation
            return
        logger.warning(f"could not find station {station} ({type(station)})")

    def check_station(self) -> bool:
        """Returns whether Station needs check and updating"""
        # 1. No station, need to get one
        if not hasattr(self, "_station") or self._station is None:
            logger.debug("no station")
            return True
        # 2. Station and long time since check
        now = nowutc()
        if (now - self._station_last_checked).seconds < self._station_check_freq:
            logger.debug("station not expired")
            if self.further_than(self.min_distance_km):  # did we move far?
                logger.debug("moved")
                (nearest, coords) = Station.nearest(lat=self._last_position[0], lon=self._last_position[1], max_coord_distance=150000)
                if nearest is not None:
                    if nearest.icao != self.station.icao:
                        logger.debug(f"new nearest {nearest.icao}")
                        self._station = nearest
                        return True
                    else:
                        logger.debug(f"nearest {nearest.icao} unchanged")
            else:
                logger.debug("not moved enough")
            # else we did not move far enough, no need to update station
        else:  # time to check
            logger.debug("station expired, need to check")
            return True
        return False

    def station_changed(self):
        """Called when station has changed"""
        logger.debug("station changed, invalidating weather")
        self._weather_last_checked = None  # forget about old values
        self._station_last_checked = nowutc()
        self.weather_changed()

    def check_weather(self) -> bool:
        """Checks whether weather needs updating for whatever reason"""
        if not hasattr(self, "_weather") or self._weather is None or self._weather_last_checked is None:
            logger.debug("no weather")
            return True
        now = nowutc()
        if (now - self._weather_last_checked).seconds > self._weather_check_freq:
            logger.debug("weather expired")
            return True
        else:
            logger.debug("weather not expired")
        return False

    def weather_changed(self):
        """Called when weather data has changed"""
        if self.update_weather():
            logger.debug("weather updated")
            super().weather_changed()
        else:
            logger.debug("weather unchanged")

    # ################################################
    # Utility functions
    #
    def further_than(self, kilometers):
        if self.station is None:
            logger.info("no station")
            return True
        position = self.collect_weather_datarefs(position_only=True)
        self._last_position = (position["sim/flightmodel/position/latitude"], position["sim/flightmodel/position/longitude"])
        station = (self.station.latitude, self.station.longitude)
        dist = distance(station, self._last_position)
        logger.info(f"further: {round(dist, 1)} vs {kilometers}km")
        return dist > kilometers

    def update_weather(self) -> bool:
        if not self.connected:
            logger.warning("not connected")
            return False

        if self.api_url is None:
            logger.warning("no api url")
            return False

        DATAREF_WEATHER = AIRCRAFT_DATAREFS if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value else REGION_DATAREFS
        DATAREF_CLOUD = DATAREF_AIRCRAFT_CLOUD if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value else DATAREF_REGION_CLOUD
        DATAREF_WIND = DATAREF_AIRCRAFT_WIND if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value else DATAREF_REGION_WIND

        with self.reading_datarefs:
            now = nowutc()
            newcollection = True
            if self._weather_last_checked is None:
                logger.debug("dataref initial collection")
                weather_drefs = self.collect_weather_datarefs()
                self._weather_last_checked = now
            elif (now - self._weather_last_checked).seconds > self._weather_check_freq:
                logger.debug("datarefs expired")
                weather_drefs = self.collect_weather_datarefs()
                self._weather_last_checked = now
            else:
                logger.debug(f"datarefs not expired, last collected at {self._weather_last_checked}")
                newcollection = False

        if not newcollection:  # not updated
            return False

        self.xp_real_weather = XPRealWeatherDatarefs(DATAREF_WEATHER, weather_drefs)
        self.wind_layers: List[WindLayer] = []  #  Defined wind layers. Not all layers are always defined. up to 13 layers(!)
        self.cloud_layers: List[CloudLayer] = []  #  Defined cloud layers. Not all layers are always defined. up to 3 layers

        for i in range(self.CLOUD_LAYERS):
            self.cloud_layers.append(CloudLayer(DATAREF_CLOUD, weather_drefs, i))

        for i in range(self.WIND_LAYERS):
            self.wind_layers.append(WindLayer(DATAREF_WIND, weather_drefs, i))

        updated = self.make_metar()  # reports if METAR has changed

        if updated:
            self._weather_last_updated = nowutc()

        return updated

    def collect_weather_datarefs(self, position_only: bool = False) -> dict:
        DATA = "data"
        IDENT = "id"

        def get_dataref_specs(path: str) -> dict | None:
            api_url = self.api_url
            payload = {"filter[name]": path}
            response = requests.get(api_url, params=payload)
            resp = response.json()
            if DATA in resp:
                return resp[DATA][0]
            logger.error(resp)
            return None

        def get_dataref_id(path: str) -> int | None:
            specs = get_dataref_specs(path)
            if specs is not None and IDENT in specs:
                return specs[IDENT]
            logger.error(specs)
            return None

        def get_dataref_value(path: str):
            dref = get_dataref_specs(path)
            if dref is None or IDENT not in dref:
                logger.error(f"error for {path}")
                return None
            url = f"{self.api_url}/{dref[IDENT]}/value"
            response = requests.get(url)
            data = response.json()
            if DATA in data:
                return data[DATA]
            logger.error(f"no value for {path}")
            return None

        WEATHER_DATAFEFS = AIRCRAFT_DATAREFS if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value else REGION_DATAREFS
        if position_only:
            WEATHER_DATAFEFS = DATAREF_LOCATION

        if position_only:
            logger.info("collecting position datarefs..")
        else:
            logger.info(f"collecting {self.xp_real_weather_type} weather datarefs..")
        weather_datarefs = {}
        debug = True
        if debug:
            print("collecting ", end="", flush=True)
        for d in WEATHER_DATAFEFS.values():
            v = get_dataref_value(d)
            weather_datarefs[d] = v
            # logger.debug(f"{d}={v}")
            if debug:
                print(".", end="", flush=True)
        if debug:
            print(" done")
        logger.info(f"..collected {len(weather_datarefs)} datarefs")

        # flatten arrays
        for d, v in weather_datarefs.items():
            if type(v) is list:  # "dataref": [value, value, ...]
                weather_datarefs = weather_datarefs | {f"{d}[{i}]": v[i] for i in range(len(v))}  # "dataref[i]": value(i)
        return weather_datarefs

    # ################################################
    # Layer utility functions
    #
    def sort_layers_by_alt(self):
        # only keeps layers with altitude
        cloud_alts = filter(lambda x: x.base is not None, self.cloud_layers)
        self.cloud_layers = sorted(cloud_alts, key=lambda x: x.base)
        if not len(self.cloud_layers) > 0:
            logger.warning("no cloud layer with altitude?")
        wind_alts = filter(lambda x: x.alt_msl is not None, self.wind_layers)
        self.wind_layers = sorted(wind_alts, key=lambda x: x.alt_msl)
        if not len(self.wind_layers) > 0:
            logger.warning("no wind layer with altitude?")

    def cloud_layer_at(self, alt=0) -> CloudLayer | None:
        # Returns cloud layer at altitude alt (MSL)
        self.sort_layers_by_alt()
        for l in self.cloud_layers:
            if alt <= l.base:
                return l
        return None

    def wind_layer_at(self, alt=0) -> WindLayer | None:
        # Returns wind layer at altitude alt (MSL)
        # Collect level bases with index
        self.sort_layers_by_alt()
        for l in self.wind_layers:
            if alt <= l.alt_msl:
                return l
        return None

    def print_cloud_layers_alt(self):
        i = 0
        for l in self.cloud_layers:
            print(f"[{i}]", getattr(l, "base"), getattr(l, "tops"))
            i = i + 1

    def print_wind_layers_alt(self):
        i = 0
        for l in self.wind_layers:
            print(f"[{i}]", getattr(l, "alt_msl"))
            i = i + 1

    # ################################################
    # Weather values
    #
    def weather_wind(self) -> tuple:  # tuple can be (None, None)
        speed = None
        direct = None
        if len(self.wind_layers) > 0:
            hasalt = list(filter(lambda x: x.alt_msl is not None, self.wind_layers))
            if len(hasalt) > 0:
                lb = sorted(hasalt, key=lambda x: x.alt_msl)
                lowest = lb[0]
                if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
                    speed = round(lowest.speed_kts)
                    direct = lowest.direction
                else:
                    speed = round(lowest.wind_speed * 1.943844)
                    direct = lowest.wind_dir
                if speed < 0:  # -1: speed not available
                    speed = None
            else:
                logger.warning("no wind layer with altitude")
        return (speed, direct)

    def weather_visibility(self) -> int | float | None:
        # We use SI, no statute miles
        if self.xp_real_weather.visibility is None:
            return None
        dist = round(self.xp_real_weather.visibility * 1609)  ## m
        return 9999 if dist > 9999 else 100 * round(dist / 100)

    def is_cavok(self) -> bool:
        # needs refining according to METAR conventions
        # 1. look at current overall visibility
        nocov = False
        dist = 0
        if self.xp_real_weather.visibility is not None:
            dist = round(self.xp_real_weather.visibility * 1609)  ## m
            nocov = True
        # 2. look at each cloud layer coverage
        self.sort_layers_by_alt()
        i = 0
        while nocov and i < len(self.cloud_layers):
            cl = self.cloud_layers[i]
            nocov = cl.coverage is None or cl.coverage < 0.125  # 1/8, some say < 0.05
            i = i + 1
        return dist > 9999 and nocov

    def weather_temperatures(self) -> tuple:  # tuple can be (None, None)
        # Temperature
        t1 = None
        if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
            if hasattr(self.xp_real_weather, "temp") and self.xp_real_weather.temp is not None:
                t1 = self.xp_real_weather.temp
        else:
            if hasattr(self.xp_real_weather, "temperature_msl") and self.xp_real_weather.temperature_msl is not None:
                t1 = self.xp_real_weather.temperature_msl
        # Dew point of lowest wind layer
        self.sort_layers_by_alt()
        l = self.wind_layers[0]
        t2 = None
        if hasattr(l, "dew_point") and l.dew_point is not None:
            t2 = l.dew_point
        return (t1, t2)

    def weather_pressure(self) -> str:
        t1 = None
        if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
            if hasattr(self.xp_real_weather, "qnh") and self.xp_real_weather.qnh is not None:
                t1 = self.xp_real_weather.qnh
        else:
            if hasattr(self.xp_real_weather, "qnh_pas") and self.xp_real_weather.qnh_pas is not None:
                t1 = self.xp_real_weather.qnh_pas
        return t1

    # ################################################
    # METAR and METAR groups
    #
    def get_station(self) -> Station:
        """Get station at location/time weather data was last fetched"""
        if self.xp_real_weather is None:
            return (None, (0, 0))
        lat = self.xp_real_weather.latitude
        lon = self.xp_real_weather.longitude
        return Station.nearest(lat=lat, lon=lon, max_coord_distance=150000)

    def metar_group_station_icao(self, remember: bool = False) -> str:
        if self.station is not None:
            return self.station.icao
        (nearest, coords) = self.get_station()
        if nearest is not None:
            if remember:
                self._station = nearest
            return nearest.icao
        return "ICAO"

    def metar_group_time(self) -> str:
        t = datetime.now().astimezone(tz=timezone.utc)
        m = "00"
        if t.minute > 30:
            m = "30"
        return t.strftime(f"%d%H{m}Z")

    def metar_group_auto(self) -> str:
        return "AUTO"

    def metar_group_wind(self) -> str:
        ret = "00000KT"
        if len(self.wind_layers) > 0:
            hasalt = list(filter(lambda x: x.alt_msl is not None, self.wind_layers))
            if len(hasalt) > 0:
                lb = sorted(hasalt, key=lambda x: x.alt_msl)
                lowest = lb[0]
                speed = 0
                direct = None
                if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
                    speed = round(lowest.speed_kts)
                    direct = lowest.direction
                else:
                    speed = round(lowest.wind_speed * 1.943844)
                    direct = lowest.wind_dir
                if direct is None:
                    ret = "VRB"
                else:
                    ret = f"{round(direct):03d}"
                if speed < 0:  # -1: speed not available
                    ret = ret + "//KT"
                else:
                    ret = ret + f"{speed:02d}KT"
                # @todo add gusting later
            else:
                logger.warning("no wind layer with altitude")
        return ret

    def metar_group_visibility(self) -> str:
        # We use SI, no statute miles
        if self.xp_real_weather.visibility is not None:
            dist = round(self.xp_real_weather.visibility * 1609)  ## m
            if dist > 9999:
                return "9999"
            dist = 100 * round(dist / 100)
            return f"{dist:04d}"
        else:
            return "NOVIS"

    def metar_group_rvr(self) -> str:
        # if station is an airport and airport has runways
        return ""

    def metar_group_phenomenae(self) -> str:
        # hardest: From
        # 1 Does it rain, snow? does water come down?
        # 2 Surface wind
        # 3 Cloud type, coverage and base of lowest layer
        # 4 Visibility
        # Determine:
        # Heavy/Moderate/Light
        # Weather::
        # BC Patches
        # BL Blowing
        # DL Distant lightning
        # DR Drifting
        # FZ Freezing
        # MI Shallow
        # PR Partial
        # SH Showers
        # TS Thunderstorm
        # VC in the Vicinty
        # Phenomenae::
        # BR Mist
        # DU Dust
        # DS Duststorm
        # DZ Drizzle
        # FC Funnel cloud
        # FG Fog
        # FU Smoke
        # GR Hail
        # GS Small hail/snow pellets
        # HZ Haze
        # PL Ice pellets
        # PO Dust devil
        # RA Rain
        # SA Sand
        # SG Snow grains
        # SN Snow
        # SQ Squall
        # SS Sandstorm
        # VA Volcanic ash
        # UP Unidentified precipitation
        #
        phenomenon = ""
        self.sort_layers_by_alt()
        vis = 9999
        if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
            if self.xp_real_weather.visibility is not None:
                vis = round(self.xp_real_weather.precipitations)  ## m
            water = 0
            if self.xp_real_weather.visibility is not None:
                water = round(self.xp_real_weather.precipitations)  ## m
            lc = self.cloud_layers[0]

        # Mist or fog
        # Water saturation 100%, temp <= dew point temp

        # Rain
        # water > 0, friction decreased
        # may be snow?
        # if temp < 2, friction decreased "sa lot"

        return phenomenon

    def metar_group_clouds(self) -> str:

        def to_fl(m, r: int = 10):
            # Convert meters to flight level (1 FL = 100 ft). Round flight level to r if provided, typically rounded to 10, at Patm = 1013 mbar
            fl = m / 30.48
            if r is not None and r > 0:
                fl = r * int(fl / r)
            return fl

        clouds = ""
        self.sort_layers_by_alt()
        last = -1
        for l in self.cloud_layers:
            local = ""
            cov = int(l.coverage / 0.125) if l.coverage is not None else 0
            # alt = to_fl(l.base, 5)
            # print(l.base, l.base * 3.042, ":", cov)
            if cov >= last:
                s = ""
                if cov > 0 and cov <= 2:
                    s = "FEW"
                elif cov > 2 and cov <= 4:
                    s = "SCT"
                elif cov > 4 and cov <= 7:
                    s = "BKN"
                elif cov > 7:
                    s = "OVC"
                if s != "":
                    alt = to_fl(l.base, 5)
                    local = f"{s}{alt:03d}"
                last = cov
            clouds = clouds + " " + local
        return clouds.strip()

    def metar_group_temperatures(self) -> str:
        def negtemp(temp):
            # format negative temperature (celsius)
            # no temperature is //
            if temp is None or (type(temp) is str and temp == "//"):
                return "//"
            return f"M{abs(temp):02d}" if temp < 0 else f"{t1:02d}"

        # Temperature
        t1 = None
        if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
            if hasattr(self.xp_real_weather, "temp") and self.xp_real_weather.temp is not None:
                t1 = round(self.xp_real_weather.temp)
            else:
                t1 = "//"
        else:
            if hasattr(self.xp_real_weather, "temperature_msl") and self.xp_real_weather.temperature_msl is not None:
                t1 = round(self.xp_real_weather.temperature_msl)
            else:
                t1 = "//"
        temp = negtemp(t1)
        # Dew point of lowest wind layer
        self.sort_layers_by_alt()
        l = self.wind_layers[0]
        t1 = None
        if hasattr(l, "dew_point") and l.dew_point is not None:
            t1 = round(l.dew_point)
        else:
            t1 = "//"
        # Can return "/////" if no temp
        temp = temp + "/" + negtemp(t1)
        return temp

    def metar_group_pressure(self) -> str:
        press = None
        if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
            press = f"Q{round(self.xp_real_weather.qnh/100)}"
        else:
            press = f"Q{round(self.xp_real_weather.qnh_pas/100)}"
        return press

    def metar_group_forecast(self) -> str:
        return ""

    def metar_group_remarks(self) -> str:
        return ""

    def make_metar(self, alt=None) -> bool:
        """Generate METAR from dataref values

        Generated string is placed in self.generated_metar_raw.
        METAR is placed in self._weather if valid

        Returns True if METAR has changed and is valid (can be used).
        """
        metar = self.metar_group_station_icao(remember=self.station is None)
        metar = metar + " " + self.metar_group_time()
        metar = metar + " " + self.metar_group_auto()
        metar = metar + " " + self.metar_group_wind()
        if self.is_cavok():
            metar = metar + " CAVOK"
            metar = metar + " " + self.metar_group_rvr()
        else:
            metar = metar + " " + self.metar_group_visibility()
            metar = metar + " " + self.metar_group_rvr()
            metar = metar + " " + self.metar_group_phenomenae()
            metar = metar + " " + self.metar_group_clouds()
        metar = metar + " " + self.metar_group_temperatures()
        metar = metar + " " + self.metar_group_pressure()
        metar = metar + " " + self.metar_group_forecast()
        metar = metar + " " + self.metar_group_remarks()

        # Cleanup and parse
        metar = re.sub(" +", " ", metar)  # clean multiple spaces
        metar = metar.strip()

        updated = False
        if self.generated_metar_raw is not None:  # keep previous
            if metar != self.generated_metar_raw:
                self.previous_weather.append(self.generated_metar_raw)
                self.generated_metar_raw = metar
                updated = True
                logger.debug(f"generated METAR {self.generated_metar_raw} updated")
            else:
                logger.debug(f"generated METAR {self.generated_metar_raw} unchanged")
        else:  # first metar
            self.generated_metar_raw = metar
            updated = True
            logger.debug(f"generated METAR {self.generated_metar_raw} created")

        if updated or self.weather is None:
            oldweather = self.weather
            try:
                self._weather = Metar.from_report(report=self.generated_metar_raw)
                logger.info(f"METAR {self._weather.raw}")
                return True
            except:
                logger.warning(f"failed to parse METAR {self.generated_metar_raw}", exc_info=True)
                if self.weather is not None:
                    logger.warning(f"METAR discrepency generated={self.generated_metar_raw}, previous={self.weather.raw}")
                    self._weather = oldweather  # so that imaging can work
        else:
            logger.debug(f"METAR {self.weather.raw} not changed")
        return False

    # ################################################
    # Summary output
    #
    def get_lines(self, layer_index: int = 0) -> list:
        lines = []

        if self.xp_real_weather is None:
            lines.append(f"Mode: {self.xp_real_weather_type}")
            lines.append("No weather")
            return lines

        dt = "NO TIME"
        if self._weather_last_updated is not None:
            dt = self._weather_last_updated.strftime("%d %H:%M")
        lines.append(f"{dt} /M:{self.xp_real_weather_type[0:4]}")

        press = self.weather_pressure()
        press = f"{round(press/100)}" if press is not None else "no pressure"
        lines.append(f"Press: {press} hPa")

        temp, dewp = self.weather_temperatures()
        temp = f"{round(temp)}" if temp is not None else "no temperature"
        lines.append(f"Temp: {temp} °C")

        vis = self.weather_visibility()
        vis = round(vis, 1) if vis is not None else "no visibility info"
        lines.append(f"Vis: {vis} m ({round(vis * 0.0006213712, 1)} sm)")

        # Wind layer info
        widx = layer_index % len(self.wind_layers)
        currwl = self.wind_layers[widx]

        dewp = round(currwl.dew_point, 1)
        lines.append(f"DewP: {dewp} °C (W{widx})")

        if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
            speed = round(currwl.speed_kts)
            direct = currwl.direction
        else:
            speed = round(currwl.wind_speed * 1.943844)
            direct = currwl.wind_dir
        if direct is None:
            wind_direct_str = "variable"
        else:
            wind_direct_str = f"{round(direct):03d}"
        if speed < 0:  # -1: speed not available
            wind_speed_str = "// kt"
        else:
            wind_speed_str = f"{speed:02d} kt"
        lines.append(f"Winds: {wind_speed_str} {wind_direct_str}° (W{widx})")

        # Cloud layer info
        cidx = layer_index % len(self.cloud_layers)
        currcl = self.cloud_layers[cidx]
        if currcl.coverage is not None:
            covr8 = int(currcl.coverage * 8)
            covcode = "SKC"
            if 0.5 < covr8 <= 2:
                covcode = "FEW"
            elif 2 < covr8 <= 4:
                covcode = "SCT"
            elif 4 < covr8 <= 7:
                covcode = "BKN"
            elif 7 < covr8:
                covcode = "OVC"
        else:
            covr = "/"
            covcode = "///"
        lines.append(f"Clouds: {covr8}/8 {covcode} {CLOUD_TYPE[int(currcl.cloud_type)] if currcl.cloud_type is not None else ''}")

        if currcl.base is not None:
            ceil = round(currcl.base)
            ceilfl = f"{round(ceil * 3.28084 / 100):03d}"
        else:
            ceil = "////"
            ceilfl = "///"
        lines.append(f"Ceil FL{ceilfl} ({ceil}m) (C{cidx})")

        return lines

    # Past data (station plot interface for trends)
    def get_metar_for(self, icao: str) -> list:
        return []

    def get_older_metar(self, icao: str) -> list:
        return []

    # def print(self, level=logging.INFO):
    #     # debug, test
    #     width = 70
    #     output = io.StringIO()
    #     print("\n", file=output)
    #     print("=" * width, file=output)
    #     MARK_LIST = ["DATAREF", "VALUE"]
    #     table = []
    #     csv = []

    #     DATAREF_WEATHER = DATAREF_AIRCRAFT_WEATHER if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value else DATAREF_REGION_WEATHER
    #     DATAREF_CLOUD = DATAREF_AIRCRAFT_CLOUD if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value else DATAREF_REGION_CLOUD
    #     DATAREF_WIND = DATAREF_AIRCRAFT_WIND if self.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value else DATAREF_REGION_WIND

    #     for k, v in DATAREF_WEATHER.items():
    #         line = (v, getattr(self.xp_real_weather, k))
    #         table.append(line)  # print(v, getattr(self.xp_real_weather, k))
    #         csv.append(line)
    #     i = 0
    #     for l in self.cloud_layers:
    #         for k, v in DATAREF_CLOUD.items():
    #             line = (f"{v}[{i}]", getattr(l, k))  # print(f"{v}[{i}]", getattr(l, k))
    #             table.append(line)
    #             csv.append(line)
    #         i = i + 1
    #     i = 0
    #     for l in self.wind_layers:
    #         for k, v in DATAREF_WIND.items():
    #             line = (f"{v}[{i}]", getattr(l, k))  # print(f"{v}[{i}]", getattr(l, k))
    #             table.append(line)
    #             csv.append(line)
    #         i = i + 1
    #     # table = sorted(table, key=lambda x: x[0])  # absolute emission time
    #     print(tabulate(table, headers=MARK_LIST), file=output)
    #     print("-" * width, file=output)
    #     print(f"reconstructed METAR: {self.generated_metar_raw}", file=output)
    #     print("=" * width, file=output)

    #     contents = output.getvalue()
    #     output.close()
    #     logger.log(level, f"{contents}")


# ######################################################################################
# Mapping between python class instance attributes and datarefs:
# weather.baro get dataref "sim/weather/aircraft/barometer_current_pas" current value.
#
DATAREF_TIME = {
    "local_hours": "sim/cockpit2/clock_timer/local_time_hours",
    "local_minutes": "sim/cockpit2/clock_timer/local_time_minutes",
    "zulu_hours": "sim/cockpit2/clock_timer/zulu_time_hours",
    "zulu_minutes": "sim/cockpit2/clock_timer/zulu_time_minutes",
    "day_of_month": "sim/cockpit2/clock_timer/current_day",
    "day_of_year": "sim/time/local_date_days",
}

DATAREF_LOCATION = {"latitude": "sim/flightmodel/position/latitude", "longitude": "sim/flightmodel/position/longitude"}

DATAREF_AIRCRAFT_WEATHER = {
    "alt_error": "sim/weather/aircraft/altimeter_temperature_error",
    "baro": "sim/weather/aircraft/barometer_current_pas",
    "gravity": "sim/weather/aircraft/gravity_mss",
    "precipitations": "sim/weather/aircraft/precipitation_on_aircraft_ratio",
    "qnh": "sim/weather/aircraft/qnh_pas",
    "rel_humidity": "sim/weather/aircraft/relative_humidity_sealevel_percent",
    "speed_of_sound": "sim/weather/aircraft/speed_sound_ms",
    "temp": "sim/weather/aircraft/temperature_ambient_deg_c",
    "temp_leading_edge": "sim/weather/aircraft/temperature_leadingedge_deg_c",
    "thermal_rete": "sim/weather/aircraft/thermal_rate_ms",
    "visibility": "sim/weather/aircraft/visibility_reported_sm",
    "wave_ampl": "sim/weather/aircraft/wave_amplitude",
    "wave_dir": "sim/weather/aircraft/wave_dir",
    "wave_length": "sim/weather/aircraft/wave_length",
    "wave_speed": "sim/weather/aircraft/wave_speed",
    "wind_dir": "sim/weather/aircraft/wind_now_direction_degt",
    "wind_speed": "sim/weather/aircraft/wind_now_speed_msc",
}

DATAREF_AIRCRAFT_CLOUD = {
    "base": "sim/weather/aircraft/cloud_base_msl_m",
    "coverage": "sim/weather/aircraft/cloud_coverage_percent",
    "tops": "sim/weather/aircraft/cloud_tops_msl_m",
    "cloud_type": "sim/weather/aircraft/cloud_type",  # Blended cloud types per layer. 0 = Cirrus, 1 = Stratus, 2 = Cumulus, 3 = Cumulo-nimbus. Intermediate values are to be expected.
}

DATAREF_AIRCRAFT_WIND = {
    "alt_msl": "sim/weather/aircraft/wind_altitude_msl_m",
    "direction": "sim/weather/aircraft/wind_direction_degt",
    "speed_kts": "sim/weather/aircraft/wind_speed_kts",
    "temp_alotf": "sim/weather/aircraft/temperatures_aloft_deg_c",
    "dew_point": "sim/weather/aircraft/dewpoint_deg_c",
    "turbulence": "sim/weather/aircraft/turbulence",
    "shear_dir": "sim/weather/aircraft/shear_direction_degt",
    "shear_kts": "sim/weather/aircraft/shear_speed_kts",
}

DATAREF_REGION_WEATHER = {
    "change_mode": "sim/weather/region/change_mode",  # How the weather is changing. 0 = Rapidly Improving, 1 = Improving, 2 = Gradually Improving, 3 = Static, 4 = Gradually Deteriorating, 5 = Deteriorating, 6 = Rapidly Deteriorating, 7 = Using Real Weather
    "qnh_base": "sim/weather/region/qnh_base_elevation",
    "qnh_pas": "sim/weather/region/qnh_pas",
    "rain_pct": "sim/weather/region/rain_percent",
    "runway_friction": "sim/weather/region/runway_friction",  # The friction constant for runways (how wet they are). Dry = 0, wet(1-3), puddly(4-6), snowy(7-9), icy(10-12), snowy/icy(13-15)
    "pressure_msl": "sim/weather/region/sealevel_pressure_pas",
    "temperature_msl": "sim/weather/region/sealevel_temperature_c",
    "thermal_rate": "sim/weather/region/thermal_rate_ms",
    "update": "sim/weather/region/update_immediately",  # If this is true, any weather region changes EXCEPT CLOUDS will take place immediately instead of at the next update interval (currently 60 seconds).
    "variability": "sim/weather/region/variability_pct",  # How randomly variable the weather is over distance. Range 0 - 1.
    "visibility": "sim/weather/region/visibility_reported_sm",
    "wave_amp": "sim/weather/region/wave_amplitude",
    "wave_dir": "sim/weather/region/wave_dir",
    "wave_length": "sim/weather/region/wave_length",
    "wave_speed": "sim/weather/region/wave_speed",
    "source": "sim/weather/region/weather_source",
}

DATAREF_REGION_CLOUD = {
    "base": "sim/weather/region/cloud_base_msl_m",
    "coverage": "sim/weather/region/cloud_coverage_percent",
    "tops": "sim/weather/region/cloud_tops_msl_m",
    "cloud_type": "sim/weather/region/cloud_type",
}

DATAREF_REGION_WIND = {
    "alt_msl": "sim/weather/region/atmosphere_alt_levels_m",
    "dew_point": "sim/weather/region/dewpoint_deg_c",
    "temp_aloft": "sim/weather/region/temperatures_aloft_deg_c",
    "temp_alt_msl": "sim/weather/region/temperature_altitude_msl_m",
    "wind_alt_msl": "sim/weather/region/wind_altitude_msl_m",
    "wind_dir": "sim/weather/region/wind_direction_degt",
    "wind_speed": "sim/weather/region/wind_speed_msc",
    "turbulence": "sim/weather/region/turbulence",
    "shear_dir": "sim/weather/region/shear_direction_degt",
    "shear_speed": "sim/weather/region/shear_speed_msc",
}

CLOUD_TYPE = ["Cirrus", "Stratus", "Cumulus", "Cumulo-nimbus"]
#
# ######################################################################################

# Flavor
AIRCRAFT_DATAREFS = DATAREF_TIME | DATAREF_LOCATION | DATAREF_AIRCRAFT_WEATHER | DATAREF_AIRCRAFT_CLOUD | DATAREF_AIRCRAFT_WIND
REGION_DATAREFS = DATAREF_TIME | DATAREF_LOCATION | DATAREF_REGION_WEATHER | DATAREF_REGION_CLOUD | DATAREF_REGION_WIND


class DatarefAccessor:
    # Maps an object attribute to a dict entry
    # attr_db = { "temp": "sim/weather/aircraft/temperature_ambient_deg_c" }
    # weather.temp --> self.__datarefs__.get( weather.attr_db.get("temp") )
    def __init__(self, attr_db: dict, drefs: dict, index: int | None = None):
        self.attr_db = attr_db
        self.__datarefs__ = drefs
        self.__drefidx__ = index

    def __getattr__(self, name: str):
        # print("converting", name)
        name = f"{self.attr_db[name]}"
        if self.__drefidx__ is not None:
            name = name + f"[{self.__drefidx__}]"
        # print("getting", name)
        # return dref.value if dref is not None else None # if dict values are datarefs, not values
        return self.__datarefs__.get(name)


class WindLayer(DatarefAccessor):
    def __init__(self, attr_db: dict, drefs: dict, index):
        DatarefAccessor.__init__(self, attr_db=attr_db, drefs=drefs, index=index)


class CloudLayer(DatarefAccessor):
    def __init__(self, attr_db: dict, drefs: dict, index):
        DatarefAccessor.__init__(self, attr_db=attr_db, drefs=drefs, index=index)


class XPRealWeatherDatarefs(DatarefAccessor):
    def __init__(self, attr_db: dict, drefs: dict):
        DatarefAccessor.__init__(self, attr_db=attr_db, drefs=drefs)


class Time(DatarefAccessor):
    def __init__(self, attr_db: dict, drefs: dict):
        DatarefAccessor.__init__(self, attr_db=attr_db, drefs=drefs)


# Tests
if __name__ == "__main__":
    api_url = "http://192.168.1.140:8080/api/v1/datarefs"
    w = XPRealWeatherData(name="test-main", simulator=api_url, weather_type=WEATHER_LOCATION.REGION.value, update=True)
    w.update_weather()
    # w.print(level=logging.DEBUG)  # writes to logger.debug

    # w.print_cloud_layers_alt()
    # print("cl base", w.cloud_layer_at(0).base)

    # w.print_wind_layers_alt()
    # print("wl base", w.wind_layer_at(0).alt_msl)

    # print("sample values")
    # if w.xp_real_weather_type == WEATHER_LOCATION.AIRCRAFT.value:
    #     print("baro", w.xp_real_weather.baro)
    #     print("qnh", w.xp_real_weather.qnh)
    # else:
    #     print("qnh base", w.xp_real_weather.qnh_pas)
    # print("cloud type", w.cloud_layers[2].cloud_type)
    # print("wind layer alt", w.wind_layers[7].alt_msl)

    print(w.generated_metar_raw)
    print("\n".join(w.weather.summary.split(", ")))
    print("---")
    print("\n".join(w.get_lines()))

    # w.update()
