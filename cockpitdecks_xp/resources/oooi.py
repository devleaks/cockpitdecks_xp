# Demonstration conceptual "Permanent" observable.
# Reports OOOI events on console
# AS an exercise:
# Should be created as a "soft" Observable (example):
# Challenge is to use generic, always valid datarefs.
# Using custom datarefs like ToLiss FBW is often easier but only works on ToLiss aircrafts.
#
# - name: Off block
#   typ: onchange
#   enabled: True
#   dataref: sim/flightmodel2/position/groundspeed
#   actions:
#       condition: ${sim/flightmodel2/position/groundspeed} 2 > ${data:oooi} none eq and
#       set-dataref: data:oooi
#       value: OUT
#       message: Out
# - name: Takeoff
#   typ: onchange
#   enabled: True
#   dataref: sim/flightmodel/position/y_agl
#   actions:
#       condition: ${sim/flightmodel/position/y_agl} 50 > ${data:oooi} OUT eq and
#       set-dataref: data:oooi
#       value: OFF
#       message: Off
# - name: Landing
#   typ: onchange
#   enabled: True
#   dataref: sim/flightmodel/position/y_agl
#   actions:
#       condition: ${sim/flightmodel/position/y_agl} 10 < ${data:oooi} OFF eq and
#       set-dataref: data:oooi
#       value: ON
#       message: On
# - name: On block
#   typ: onchange
#   enabled: True
#   dataref: sim/flightmodel2/position/groundspeed
#   actions:
#       condition: ${sim/flightmodel2/position/groundspeed} 0.01 < ${data:oooi} ON eq and
#       set-dataref: data:oooi
#       value: IN
#       message: In
#
import logging
from enum import Enum
from datetime import datetime, timezone
from typing import Dict, Any

from cockpitdecks.observable import Observable
from cockpitdecks.simulator import Simulator, SimulatorVariableListener, SimulatorVariable


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


GROUND_SPEED = "sim/flightmodel2/position/groundspeed"
AGL = "sim/flightmodel/position/y_agl"
TRACKING = "sim/cockpit2/gauges/indicators/ground_track_mag_pilot"  # The ground track of the aircraft in degrees magnetic
HEADING = "sim/cockpit2/gauges/indicators/compass_heading_deg_mag"  # Indicated heading of the wet compass, in degrees.

OOOI_SIMULATOR_VARIABLES = {GROUND_SPEED, AGL, TRACKING, HEADING}


class OOOI(Enum):
    OUT = "off-block"  # When the aircraft leaves the gate or parking position
    OFF = "takeoff"  # When the aircraft takes off from the runway
    ON = "landing"  # When the aircraft lands on the destination runway
    IN = "on-block"  # When the aircraft arrives at the gate or parking position


class PHASE(Enum):
    ON_BLOCK = "on blocks"
    TAXI_OUT = "taxi out"
    ON_HOLD = "on hold"
    TAKEOFF_ROLL = "takeoff"
    FLYING = "air"
    LANDING_ROLL = "landing"
    TAXI_IN = "taxi in"


# Thresholds
#
STOPPED_SPEED_MARGIN = 0.1
TAXI_SPEED_MARGIN = 11  # 11m/s = 40 km/h
ROLL_SPEED_MARGIN = 50  # 50m/s = 97knt
AIR_SPEED_MARGIN = 72  # 72m/s = 140knt, should be in air...

AIR_AGL_MARGIN = 30  # meters


EPOCH = datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

def now():
    return datetime.now(timezone.utc)


class OOOIObservable(Observable, SimulatorVariableListener):
    """Special observable that monitor the aircraft position
    and update the closest weather/airport station every check_time seconds
    if necessary.
    """

    OBSERVABLE_NAME = "oooi"

    def __init__(self, simulator: Simulator):
        oooi_config = {
            "name": type(self).__name__,
            "actions": [{}],
        }
        Observable.__init__(self, config=oooi_config, simulator=simulator)

        self.callsign = ""  # call sign of sender
        self.station = ""  # station to report OOOI to

        self.first: Dict[str, Any] = {}
        self.last: Dict[str, Any] = {}

        self.speed_trend = None
        self.current_state: PHASE | None = None

        self.eta: datetime | None = None

        self.current_oooi: OOOI | None = None
        self.all_oooi: Dict[OOOI, datetime] = {}

    def __str__(self) -> str:
        return self.report(display=False)

    def get_variables(self) -> set:
        return OOOI_SIMULATOR_VARIABLES

    @property
    def oooi(self) -> OOOI | None:
        return self.current_oooi

    @oooi.setter
    def oooi(self, report: OOOI):
        if self.current_state == report:
            return  # no change
        self.current_oooi = report
        self.all_oooi[report] = now()
        self.report()

    @property
    def inited(self) -> bool:
        return len([d for d in self.first if d is not None]) == len(OOOI_SIMULATOR_VARIABLES)

    @property
    def pushback(self) -> bool:
        if not self.inited:
            return False
        h = self.first.get(HEADING)
        t = self.first.get(TRACKING)
        if h > 270 and t < 90:
            t = t + 360
        elif h < 90 and t > 270:
            h = h + 360
        return abs(h - t) > 40  # we are not moving in the direction of the heading of the aircraft

    def set_eta(self, eta: datetime):
        # when we get one...
        first = self.eta is None
        self.eta = eta
        logger.info(f"eta {self.eta.replace(second=0, microsecond=0)}")
        if not first:
            self.report()
            self.last_eta = now()

    def inital_state(self):
        if self.inited:
            return
        for d in OOOI_SIMULATOR_VARIABLES:
            if d not in self.first or self.first.get(d) is None:
                v = self.sim.get_simulator_variable_value(d)
                if v is not None:
                    self.first[d] = v
                    self.last[d] = v
                    logger.debug(f"SET {d} {v}")
        if not self.inited:
            return
        # We have a first value for all variables, try to determine initial state
        speed = self.first.get(GROUND_SPEED)
        agl = self.first.get(AGL)
        # 1. Are we in the air?
        if speed > AIR_SPEED_MARGIN and agl > AIR_AGL_MARGIN:
            logger.debug("we are in the air")
            self.current_state = PHASE.FLYING
            logger.info(f"speed {round(speed, 2)} > {AIR_SPEED_MARGIN}, alt {round(agl, 2)} > {AIR_AGL_MARGIN}, assuming {PHASE.FLYING.value}")
            return
        else:  # 2. We are on the ground.
            logger.debug("we are on the ground")

            # 2.1 Are we moving?
            if speed < STOPPED_SPEED_MARGIN:
                self.current_state = PHASE.ON_BLOCK
                logger.info(f"speed {round(speed, 2)} < {STOPPED_SPEED_MARGIN}, assuming {PHASE.ON_BLOCK.value}")
                return
            if speed < TAXI_SPEED_MARGIN:
                self.current_state = PHASE.TAXI_OUT
                logger.info(f"speed {round(speed, 2)} < {TAXI_SPEED_MARGIN}, assuming {PHASE.TAXI_OUT.value}")
                return
            if speed > ROLL_SPEED_MARGIN:
                if self.speed_trend is not None:
                    if self.speed_trend > 0:
                        self.current_state = PHASE.TAKEOFF_ROLL
                        logger.info(f"speed {round(speed, 2)} > {TAKEOFF_SPEED_MARGIN}, assuming {PHASE.TAKEOFF_ROLL.value}")
                    elif self.speed_trend <= 0:
                        self.current_state = PHASE.LANDING_ROLL
                        logger.info(f"speed {round(speed, 2)} > {TAKEOFF_SPEED_MARGIN}, assuming {PHASE.LANDING_ROLL.value}")
                return
        logger.debug(f"inited {self.first}")

    def report(self, display: bool = True) -> str:
        """Build short string with all values, displays it on console

        Returns:
            str: string with all values
        """
        def strfdelta(tdelta):
            ret = ""
            if tdelta.days > 0:
                ret = f"{tdelta.days} d "
            h, rem = divmod(tdelta.seconds, 3600)
            ret = ret + f"{h:02d}"
            m, s = divmod(rem, 60)
            ret = ret + f"{m:02d}{s:02d}:"
            return ret

        TIME_FMT = "%H%M"

        def pt(ts: datetime | None):
            if ts is None:
                return "----"
            if ts == EPOCH:
                return "...?"
            return ts.strftime(TIME_FMT)

        report = f"{self.icao_dept}/{self.icao_dest}"
        off_set = False
        if self.all_oooi.get(OOOI.OUT) is not None:
            report = report + f" OUT/{pt(self.all_oooi.get(OOOI.OUT))}"
        else:
            report = report + " OUT/----"
        if self.all_oooi.get(OOOI.OFF) is not None:
            report = report + f" OFF/{pt(self.all_oooi.get(OOOI.OFF))}"
        else:
            report = report + " OFF/----"
            off_set = True

        if self.all_oooi.get(OOOI.ON) is not None:
            report = report + f" ON/{pt(self.all_oooi.get(OOOI.ON))}"

            if self.all_oooi.get(OOOI.IN) is not None:
                report = report + f" IN/{pt(self.all_oooi.get(OOOI.IN))}"
            else:
                report = report + " IN/----"
                if self.eta is not None and self.eta > self.all_oooi.get(OOOI.ON):  # ETA after landing might be ETA "at the gate"
                    report = report + f" ETA/{pt(self.eta)}"
        else:
            if not off_set or ALWAYS_FOUR:
                report = report + " ON/----"
                if ALWAYS_FOUR:
                    report = report + " IN/----"
            if self.eta is not None:
                report = report + f" ETA/{pt(self.eta)}"

        time_info = ""
        if self.all_oooi.get(OOOI.OFF) is not None and self.all_oooi.get(OOOI.ON) is not None:
            flight_time = self.all_oooi.get(OOOI.ON) - self.all_oooi.get(OOOI.OFF)
            time_info = f"flight time: {strfdelta(flight_time)}"
        if self.all_oooi.get(OOOI.OUT) is not None and self.all_oooi.get(OOOI.IN) is not None:
            block_time = self.all_oooi.get(OOOI.IN) - self.all_oooi.get(OOOI.OUT)
            if time_info != "":
                time_info = time_info = ", "
            time_info = time_info + f"block time: {strfdelta(block_time)}"

        if display:
            logger.info(report)
            if time_info != "":
                logger.info(time_info)
        return report

    def both_engine_off(self):
        return True

    def simulator_variable_changed(self, data: SimulatorVariable):
        if data.name not in self.get_variables():
            return  # not for me, should never happen

        if not self.inited:
            self.inital_state()
            return

        # For each state, check if there is a change:
        # if self.oooi is None:
        if self.current_state == PHASE.ON_BLOCK:
            if data.name == GROUND_SPEED:
                speed = data.value
                if self.oooi is None and speed > STOPPED_SPEED_MARGIN:  # we were ON_BLOCK, we are now moving... (may strong wind?)
                    self.oooi = OOOI.OUT

        if self.oooi == OOOI.OUT:
            if data.name == AGL:
                alt = data.value
                alt_diff = alt - self.first.get(AGL)
                if alt_diff > 50:
                    self.oooi = OOOI.OFF

        if self.oooi == OOOI.OFF:
            if data.name == AGL:
                alt = data.value
                if alt < 30:
                    flight_time = now() - self.all_oooi.get(OOOI.OFF, now())
                    if flight_time.seconds > 300:
                        self.oooi = OOOI.ON

        if self.oooi == OOOI.ON:
            if data.name == GROUND_SPEED:
                speed = data.value
                if speed < STOPPED_SPEED_MARGIN:  # we were ON_BLOCK, we are now moving... (may strong wind?)
                    taxi_time = now() - self.all_oooi.get(OOOI.ON, now())
                    # are both engine off?
                    if taxi_time.seconds > 60 and self.both_engine_off():
                        self.oooi = OOOI.IN

        if data.name == GROUND_SPEED:
            prev = self.last.get(GROUND_SPEED, 0)
            prev = round(prev, 2)
            speed = data.value
            speed = round(speed, 2)
            diff = prev - speed
            if abs(diff) > 1:
                self.speed_trend = -1 if diff < 0 else 1 if diff > 0 else 0
        self.last[data.name] = data.value

    def acars_report(self, eta: str = None) -> Dict:
        return {"from": self.callsign, "to": self.station, "acars_type": "progress", "packet": str(self)}
