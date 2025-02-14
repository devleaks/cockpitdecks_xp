import logging
from datetime import datetime, timedelta

from avwx import Station

from cockpitdecks.observable import Observable
from cockpitdecks.variable import Variable
from cockpitdecks.simulator import Simulator, SimulatorVariable

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


LATITUDE = "sim/flightmodel/position/latitude"
LONGITUDE = "sim/flightmodel/position/longitude"


class WeatherStationObservable(Observable):
    """Special observable that monitor the aircraft position
    and update the closest weather/airport station every check_time seconds
    if necessary.
    """

    DEFAULT_STATION = "EBBR"
    WEATHER_STATION_VARIABLE = "weather-station-icao"

    def __init__(self, simulator: Simulator):
        wso_config = {
            "name": "weather station observable",
            "multi-datarefs": [LATITUDE, LONGITUDE],
            "actions": [{"set_dataref": self.WEATHER_STATION_VARIABLE}],  # Action is specific, in our case: (lat, lon) -> weather station icao
        }
        super().__init__(config=wso_config, simulator=simulator)
        self.check_time = 30  # seconds
        self._last_checked = datetime.now() - timedelta(seconds=self.check_time)
        self._last_updated = datetime.now()
        self._no_coord_warn = 0
        self._value.update_value(new_value=self.DEFAULT_STATION)
        logger.debug(f"changed station to {self.DEFAULT_STATION}")

    def get_variables(self) -> set:
        return {LATITUDE, LONGITUDE}

    def simulator_variable_changed(self, data: SimulatorVariable):
        if (datetime.now() - self._last_checked).seconds < self.check_time:
            return

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
            logger.debug(f"changed station to {nearest.icao} ({lat}, {lon})")
            self._value.update_value(new_value=nearest.icao, cascade=True)
            self._last_updated = datetime.now()
        else:
            logger.debug(f"checked, no change")
