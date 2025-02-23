# Custom observable that transform (lat, lon) into the the ICAO code
# of the closest weather station (real life, fetch by AVWX).
# The ICAO code of the closest station is written to weather-station Cockpitdecks variable.
# WEATHER_STATION_MONITORING = "weather-station"
#
import logging
from datetime import datetime, timedelta

from avwx import Station

from cockpitdecks import WEATHER_STATION_MONITORING
from cockpitdecks.observable import Observable
from cockpitdecks.simulator import Simulator, SimulatorVariable

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


LATITUDE = "sim/flightmodel/position/latitude"
LONGITUDE = "sim/flightmodel/position/longitude"
COORDINATES = {LATITUDE, LONGITUDE}


class WeatherStationObservable(Observable):
    """Special observable that monitor the aircraft position
    and update the closest weather/airport station every check_time seconds
    if necessary.
    """

    DEFAULT_STATION = "EBBR"

    def __init__(self, simulator: Simulator):
        wso_config = {
            "name": type(self).__name__,
            "multi-datarefs": [LATITUDE, LONGITUDE],
            "actions": [{}],  # Action is specific, in our case: (lat, lon) -> weather station icao
        }
        super().__init__(config=wso_config, simulator=simulator)
        self.check_time = 30  # seconds
        self._last_checked = datetime.now() - timedelta(seconds=self.check_time)
        self._last_updated = datetime.now()
        self._no_coord_warn = 0
        self._value.update_value(new_value=self.DEFAULT_STATION)
        self._set_dataref = simulator.get_variable(name=SimulatorVariable.internal_variable_name(path=WEATHER_STATION_MONITORING), is_string=True)
        self._set_dataref.update_value(new_value=self.DEFAULT_STATION, cascade=True)
        logger.debug(f"set initial station to {self.DEFAULT_STATION}")

    def get_variables(self) -> set:
        return COORDINATES

    def simulator_variable_changed(self, data: SimulatorVariable):
        if (datetime.now() - self._last_checked).seconds < self.check_time:
            return  # too early to change

        if data.name not in COORDINATES:
            return  # not for me, should never happen

        lat = self.sim.get_simulator_variable_value(LATITUDE)
        lon = self.sim.get_simulator_variable_value(LONGITUDE)
        if lat is None or lon is None:
            if (self._no_coord_warn % 10) == 0:
                logger.warning("no coordinates")
            self._no_coord_warn = self._no_coord_warn + 1
            return

        self._last_checked = datetime.now()
        (nearest, coords) = Station.nearest(lat=lat, lon=lon, max_coord_distance=150000)
        if nearest.icao != self._value.value():
            logger.info(f"changed weather station to {nearest.icao} ({round(lat, 6)}, {round(lon, 6)})")
            self.value = nearest.icao
            self._set_dataref.update_value(new_value=nearest.icao, cascade=True)
            self._last_updated = datetime.now()
        else:
            logger.debug("checked, no change")
