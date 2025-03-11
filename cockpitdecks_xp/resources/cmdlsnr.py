#
import logging

from cockpitdecks.observable import Observable
from cockpitdecks.simulator import Simulator, SimulatorEvent

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


MAP = "sim/map/show_current"


class MapCommandObservable(Observable):
    """Special observable that monitor the aircraft position
    and update the closest weather/airport station every check_time seconds
    if necessary.
    """

    def __init__(self, simulator: Simulator):
        wso_config = {
            "name": type(self).__name__,
            "events": [MAP],
            "actions": [{}],
        }
        Observable.__init__(self, config=wso_config, simulator=simulator)

    def get_events(self) -> set:
        return {MAP}

    def simulator_event(self, data: SimulatorEvent):
        if data.name not in self.get_events():
            return  # not for me, should never happen

        logger.info("map activated")
