# Custom observable that transform (lat, lon) into the the ICAO code
# of the closest weather station (real life, fetch by AVWX).
# The ICAO code of the closest station is written to weather-station Cockpitdecks variable.
# WEATHER_STATION_MONITORING = "weather-station"
#
import logging
from datetime import datetime, timedelta, timezone

from suntime import Sun

from cockpitdecks import DAYTIME
from cockpitdecks.observable import Observable
from cockpitdecks.simulator import Simulator, SimulatorVariable, SimulatorVariableListener

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


LATITUDE = "sim/flightmodel/position/latitude"
LONGITUDE = "sim/flightmodel/position/longitude"
LOCAL_DATE = "sim/time/local_date_days"
ZULU_TIME_SEC = "sim/time/zulu_time_sec"


class DaytimeObservable(Observable, SimulatorVariableListener):
    """Special observable that monitor the aircraft position
    and update the closest weather/airport station every check_time seconds
    if necessary.
    """

    OBSERVABLE_NAME = "daytime"

    def __init__(self, simulator: Simulator):
        wso_config = {
            "name": type(self).__name__,
            "actions": [{}],  # Action is specific, in our case: (lat, lon) -> weather station icao
        }
        Observable.__init__(self, config=wso_config, simulator=simulator)
        self.check_time = 10 * 60  # seconds
        self._last_checked = datetime.now() - timedelta(seconds=self.check_time)
        self._last_updated = datetime.now()
        self._no_coord_warn = 0
        self._value = 1
        self._set_dataref = simulator.get_variable(name=SimulatorVariable.internal_variable_name(path=DAYTIME), is_string=True)
        self._set_dataref.update_value(new_value=self._value, cascade=False)
        logger.debug(f"set initial daytime to daytime={self._value}")

    def get_variables(self) -> set:
        return {LATITUDE, LONGITUDE, LOCAL_DATE, ZULU_TIME_SEC}

    def simulator_variable_changed(self, data: SimulatorVariable):
        if (datetime.now() - self._last_checked).seconds < self.check_time:
            return  # too early to change

        if data.name not in self.get_variables():
            return  # not for me, should never happen

        lat = self.sim.get_simulator_variable_value(LATITUDE)
        lon = self.sim.get_simulator_variable_value(LONGITUDE)
        if lat is None or lon is None:
            if (self._no_coord_warn % 10) == 0:
                logger.warning("no coordinates")
            self._no_coord_warn = self._no_coord_warn + 1
            return

        days = self.sim.get_simulator_variable_value(LOCAL_DATE)
        if days is None:
            logger.warning("no days since new year")
            return

        secs = self.sim.get_simulator_variable_value(ZULU_TIME_SEC)
        if secs is None:
            logger.warning("no seconds since midnight")
            return

        self._last_checked = datetime.now()
        sun = Sun(lat, lon)  # we have a location
        dt = datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc) + timedelta(days=days) + timedelta(seconds=secs)
        sr = sun.get_sunrise_time(dt)
        ss = sun.get_sunset_time(dt)
        # https://github.com/SatAgro/suntime/issues/30
        if sr.day != ss.day:
            ss = ss + timedelta(days=1)
        daytime = 1 if sr <= dt <= ss else 0
        logger.debug(f"at {dt}, sunrise={sr}, sunset={ss}, daytime={daytime} (nb: all times in UTC)")

        if daytime != self._value:
            self._value = daytime
            self._set_dataref.update_value(new_value=daytime, cascade=True)
            self._last_updated = datetime.now()
            logger.info("day time" if daytime == 1 else "night time")
        else:
            logger.debug("checked, no change")
