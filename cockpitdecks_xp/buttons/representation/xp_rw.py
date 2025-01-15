# ###########################
# Button that displays the real weather in X-Plane.
#
# It gets updated when real wheather changes.
# These buttons are highly XP specific.
#
from datetime import datetime
import logging

from cockpitdecks import now
from .xp_wb import XPWeatherBaseIcon
from .xp_wd import XPWeatherData, CLOUD_TYPE, WEATHER_LOCATION


logger = logging.getLogger(__name__)
# logger.setLevel(SPAM_LEVEL)
# logger.setLevel(logging.DEBUG)


class XPRealWeatherIcon(XPWeatherBaseIcon):
    """
    Depends on simulator weather
    """

    REPRESENTATION_NAME = "xp-real-weather"

    MIN_UPDATE = 600  # seconds between two station updates

    def __init__(self, button: "Button"):
        self.weather = button._config.get(self.REPRESENTATION_NAME, {})
        self.mode = self.weather.get("mode", WEATHER_LOCATION.AIRCRAFT.value)
        self.xpweather = None

        self._upd_calls = 0

        self._weather_last_updated = None
        self._icon_last_updated = None
        self._cache_metar = None

        self.cloud_index = 0
        self.wind_index = 0

        self._last_value = 0

        XPWeatherBaseIcon.__init__(self, button=button)

    @property
    def api_url(self):
        a = self.button.sim.api_url
        return a

    def init(self):
        if self._inited:
            return
        self.weather_icon = self.select_weather_icon()
        self._inited = True
        logger.debug(f"inited")

    def should_update(self) -> bool:
        UPDATE_TIME_SECS = 300
        if self.xpweather is None:
            return True
        now = datetime.now().timestamp()
        return (now - self.xpweather.last_updated) > UPDATE_TIME_SECS

    def update_weather(self):
        if self.should_update():
            api_url = self.api_url
            if api_url is not None:
                api_url = api_url + "/datarefs"
                self.xpweather = XPWeatherData(api_url=api_url, weather_type=self.mode, update=True)
            else:
                return False
        if self._cache_metar is not None:
            if self._cache_metar == self.xpweather.make_metar():
                logger.debug(f"XP weather unchanged")
                return False  # weather unchanged
        self._cache_metar = self.xpweather.make_metar()
        self.weather_icon = self.select_weather_icon()
        self._weather_last_updated = now()
        # self.notify_weather_updated()
        logger.info(f"X-Plane real weather updated: {self._cache_metar}")
        logger.debug(self.xpweather.get_metar_desc(self._cache_metar))
        return True

    def is_updated(self, force: bool = False) -> bool:
        self._upd_calls = self._upd_calls + 1
        if self._last_value != self.button.value:
            return True
        if self.update_weather():
            if self._icon_last_updated is not None:
                return self._weather_last_updated > self._icon_last_updated
            return True
        return False

    def get_lines(self) -> list:
        bv = self.button.value
        if bv is None:
            bv = 0
        else:
            bv = int(bv)
        lines = []
        if self.xpweather is None:
            lines.append(f"Mode: {self.mode}")
            lines.append("No weather")
            return lines
        return self.xpweather.get_metar_lines(layer_index=bv)

    def describe(self) -> str:
        return "The representation is specific to X-Plane and show X-Plane internal weather fetched from wind and cloud layers."
