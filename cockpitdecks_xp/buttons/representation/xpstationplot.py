# ###########################
# Representation of a Metar in short textual summary form
#
import logging

from cockpitdecks.buttons.representation import WeatherStationPlot
from ...resources.xprealweather import XPRealWeatherData, WEATHER_LOCATION

logger = logging.getLogger(__name__)
# logger.setLevel(SPAM_LEVEL)
# logger.setLevel(logging.DEBUG)


class XPRealWeatherStationPlot(WeatherStationPlot):

    REPRESENTATION_NAME = "xp-real-weather-station-plot"

    def __init__(self, button: "Button"):
        WeatherStationPlot.__init__(self, button=button)
        self.weather_data = XPRealWeatherData(name=button.button_name, simulator=button.sim, weather_type=WEATHER_LOCATION.REGION.value)
        self.weather_data.add_listener(self)
