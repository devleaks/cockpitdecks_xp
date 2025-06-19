# ###########################
# Representation of a Metar in short textual summary form
#
import logging

from cockpitdecks.buttons.representation import WeatherBaseIcon
from ...resources.xprealweather import XPRealWeatherData, WEATHER_LOCATION

logger = logging.getLogger(__name__)
# logger.setLevel(SPAM_LEVEL)
# logger.setLevel(logging.DEBUG)


class XPRealWeatherMetarIcon(WeatherBaseIcon):

    REPRESENTATION_NAME = "xp-real-weather-metar"

    def __init__(self, button: "Button"):
        WeatherBaseIcon.__init__(self, button=button)
        icao = self.weather.get("station", self.DEFAULT_STATION)
        self.width = self.weather.get("width", 21)
        self.set_label(icao)
        self.weather_data = XPRealWeatherData(name=button.name, simulator=button.sim, weather_type=WEATHER_LOCATION.REGION.value)
        self.weather_data.add_listener(self)

    # #############################################
    # Cockpitdecks Representation interface
    #
    def get_lines(self) -> list | None:
        # From METAR
        if self.weather_data.weather is not None:
            return self.weather_data.weather.summary.split(",")
        # Backup! (from raw data)
        return self.weather_data.get_lines()
