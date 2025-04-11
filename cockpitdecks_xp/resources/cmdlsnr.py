# Demonstration conceptual "Permanent" observable.
# This one registers a simulator event (map activation)
#
import logging

from cockpitdecks.observable import Observable
from cockpitdecks.simulator import Simulator, SimulatorActivityListener, SimulatorActivity

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


MAP = "sim/map/show_current"


class MapCommandObservable(Observable, SimulatorActivityListener):
    """Special observable that monitor the aircraft position
    and update the closest weather/airport station every check_time seconds
    if necessary.
    """

    OBSERVABLE_NAME = "map-command"

    def __init__(self, simulator: Simulator):
        wso_config = {
            "name": type(self).__name__,
            "activities": [MAP],
            "actions": [{}],
        }
        Observable.__init__(self, config=wso_config, simulator=simulator)

    def get_activities(self) -> set:
        return {MAP}

    def simulator_activity_received(self, data: SimulatorActivity):
        if data.name not in self.get_activities():
            return  # not for me, should never happen

        logger.info("map activated")
