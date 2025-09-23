# Class for interface with X-Plane using REST/WebSocket API.
# See https://developer.x-plane.com/article/x-plane-web-api
#
from __future__ import annotations

import os
import threading
import logging
from abc import ABC
from typing import Callable, List
from enum import IntEnum
from datetime import datetime, timedelta

from cockpitdecks_xp import __version__

from cockpitdecks import CONFIG_KW, ENVIRON_KW, SPAM_LEVEL, DEPRECATION_LEVEL, MONITOR_RESOURCE_USAGE, RESOURCES_FOLDER, OBSERVABLES_FILE, yaml
from cockpitdecks.variable import Variable
from cockpitdecks.strvar import StringWithVariables, Formula
from cockpitdecks.instruction import MacroInstruction
from cockpitdecks.simulator import Simulator, SimulatorEvent, SimulatorInstruction
from cockpitdecks.simulator import SimulatorVariable, SimulatorVariableListener
from cockpitdecks.resources.intvariables import COCKPITDECKS_INTVAR
from cockpitdecks.observable import Observables, Observable
from cockpitdecks.cockpit import CockpitInstruction

# from ..resources.beacon import XPlaneBeacon, BEACON_DATA_KW
from xpwebapi import beacon as XPlaneBeacon, DATAREF_DATATYPE
from xpwebapi.api import Dataref as DatarefAPI, Command as CommandAPI
from xpwebapi.ws import XPWebsocketAPI, CALLBACK_TYPE
from ..resources.daytimeobs import DaytimeObservable

logger = logging.getLogger(__name__)
logger.setLevel(DEPRECATION_LEVEL)  # To see which dataref are requested
# logger.setLevel(logging.DEBUG)

WEBAPILOGFILE = "webapi.log"
webapi_logger = logging.getLogger("webapi")
# webapi_logger.setLevel(logging.DEBUG)
if WEBAPILOGFILE is not None:
    formatter = logging.Formatter('"%(asctime)s" %(message)s')
    handler = logging.FileHandler(WEBAPILOGFILE, mode="w")
    handler.setFormatter(formatter)
    webapi_logger.addHandler(handler)
    webapi_logger.propagate = False


# #############################################
# CONFIGURATION AND OPTIONS
#
# Data too delicate to be put in constant.py
# !! adjust with care !!
# UDP sends at most ~40 to ~50 dataref values per packet.
RECONNECT_TIMEOUT = 10  # seconds, times between attempts to reconnect to X-Plane when not connected (except on initial startup, see dynamic_timeout)
RECEIVE_TIMEOUT = 5  # seconds, assumes no awser if no message recevied withing that timeout
WAIT_FOR_RESOURCE_TIMEOUT = 5

# Local tested maxima
XP_MIN_VERSION = 121400
XP_MIN_VERSION_STR = "12.1.4"
XP_MAX_VERSION = 121499
XP_MAX_VERSION_STR = "12.1.4"
API_MAX_VERSION_STR = "v2"

# default values
API_HOST = "127.0.0.1"
API_TPC_PORT = 8086
API_PATH = "/api"
API_MIN_VERSION_STR = "v1"
# see https://gist.github.com/devleaks/729bda6db10007b844111178694c7971
# when adressing api on remote host, this is the port number of the **proxy** to X-Plane standard :8086 port
REMOTE_TCP_PORT = 8080

USE_REST = True  # force REST usage for remote access, otherwise websockets is privileged

# #############################################
# PERMANENT DATAREFS
#
# Always requested datarefs (time and simulation speed)
#
ZULU_TIME_SEC = "sim/time/zulu_time_sec"
DATETIME_DATAREFS = [
    ZULU_TIME_SEC,
    "sim/time/local_date_days",
    "sim/time/local_time_sec",
    "sim/time/use_system_time",
]
REPLAY_DATAREFS = [
    "sim/time/is_in_replay",
    "sim/time/sim_speed",
    "sim/time/sim_speed_actual",
    "sim/time/paused",
]
RUNNING_TIME = "sim/time/total_flight_time_sec"  # Total time since the flight got reset by something
AIRCRAFT_LOADED = "sim/aircraft/view/acf_relative_path"  # Path to currently loaded aircraft

# (let's say time since plane was loaded, reloaded, or changed)
USEFUL_DATAREFS = []  # monitored to determine of cached data is valid  # Total time the sim has been up

PERMANENT_SIMULATOR_VARIABLES = set(USEFUL_DATAREFS)  # set(DATETIME_DATAREFS + REPLAY_DATAREFS + USEFUL_DATAREFS)
PERMANENT_SIMULATOR_EVENTS = {}  #

# changes too often, clutters web api log.
BLACK_LIST = [ZULU_TIME_SEC, "sim/flightmodel/position/latitude", "sim/flightmodel/position/longitude"]


# A value in X-Plane Simulator
#
class Dataref(SimulatorVariable, DatarefAPI):
    """
    A Dataref is an internal value of the simulation software made accessible to outside modules,
    plugins, or other software in general.

    Same attribute in both SimulatorVariable and DatarefAPI
    # logger.debug(list(set(dir(SimulatorVariable)) & set(dir(DatarefAPI))))

    """

    def __init__(self, path: str, simulator: XPlane, is_string: bool = False):
        # Data
        SimulatorVariable.__init__(self, name=path, simulator=simulator, data_type="string" if is_string else "float")
        DatarefAPI.__init__(self, path=path, api=simulator)

    def save(self):
        """Overwrites SimulatorVariable.save() to save to simulator"""
        logger.debug(f"saving {self.name}={self.value}..")
        # for a DatarefAPI, the new value to write has to be in ._new_value
        # transfer value from Cockpitdecks SimulatorVariable.value to WebAPI Dataref.value
        self._new_value = self.value
        self.write()
        logger.debug("..saved")

    @property
    def value(self):
        #
        #
        # TEMPORARY ADJUSTMENT FOR DATA/BYTES/TEXT DATAREF
        #
        #
        encoding = "ascii"
        value_bytes = super().value
        if type(value_bytes) is bytes and self.value_type == DATAREF_DATATYPE.DATA.value:
            value_bytes_stripped = value_bytes.rstrip(b"\x00")  # remove trailing 0 (bytes with value 0)
            if len(value_bytes_stripped) > 0:
                value_bytes = value_bytes_stripped
            if self._encoding is not None and self._encoding != encoding:
                logger.warning(f"string value encodings differ {self._encoding} vs {encoding}")
            try:
                value = value_bytes.decode(encoding)
                value = value.replace("\u0000", "")  # remove trailing 0 (bytes with value 0)
                self._encoding = encoding
                return value
            except:
                logger.warning(f"could not decode value {value_bytes} with encoding {encoding}", exc_info=True)
        return value_bytes


# An events from X-Plane Simulator
#
class DatarefEvent(SimulatorEvent):
    """Dataref Update Event

    Event is created with dataref and new value and enqueued.
    The value change and propagation only occurs when executed (run) by the Cockpit.
    """

    def __init__(self, sim: Simulator, dataref: str, value: float | str, cascade: bool, autorun: bool = True):
        self.dataref_path = dataref
        self.value = value
        self.cascade = cascade
        SimulatorEvent.__init__(self, sim=sim, autorun=autorun)

    def __str__(self):
        return f"{self.sim.name}:{self.dataref_path}={self.value}:{self.timestamp}"

    def info(self):
        return super().info() | {"path": self.dataref_path, CONFIG_KW.VALUE.value: self.value, "cascade": self.cascade}

    def run(self, just_do_it: bool = False) -> bool:
        if just_do_it:
            if self.sim is None:
                logger.warning("no simulator")
                return False

            # should be: dataref = self.sim.get_variable(self.dataref_path)
            dataref = self.sim.cockpit.variable_database.get(self.dataref_path)
            if dataref is None:
                logger.debug(f"dataref {self.dataref_path} not found in database")
                return False
            try:
                logger.debug(f"updating {dataref.name}..")
                self.handling()
                dataref.update_value(self.value, cascade=self.cascade)
                self.handled()
                logger.debug("..updated")
            except:
                logger.warning("..updated with error", exc_info=True)
                return False
        else:
            self.enqueue()
            logger.debug("enqueued")
        return True


# #############################################
# Instructions
#
# An instruction in X-Plane Simulator
# There are 3 types of instructions:
# - Instruction to execute a command
# - Instruction to execute a begin command followed by an end command
# - Instruction to change the value of a sataref ("write" the value in the simulator)
#
class XPlaneInstruction(SimulatorInstruction, ABC):
    """An Instruction sent to the XPlane Simulator to execute some action.

    This is more an abstract base class, with a new() factory to handle instruction block.
    """

    def __init__(self, name: str, simulator: XPlane, delay: float = 0.0, condition: str | None = None, button: "Button" = None) -> None:
        SimulatorInstruction.__init__(self, name=name, simulator=simulator, delay=delay, condition=condition)

    @classmethod
    def new(cls, name: str, simulator: XPlane, instruction_block: dict | list | tuple) -> XPlaneInstruction | None:
        # INSTRUCTIONS = [CONFIG_KW.BEGIN_END.value, CONFIG_KW.SET_SIM_VARIABLE.value, CONFIG_KW.COMMAND.value, CONFIG_KW.VIEW.value]

        def try_keyword(ib, keyw) -> XPlaneInstruction | None:
            command_block = ib.get(keyw)
            # single simple command to execute
            if type(command_block) is str:
                # Examples:
                #  command: AirbusFWB/SpeedSelPush
                #  long-press: AirbusFWB/SpeedSelPull
                #  view: AirbusFBW/PopUpSD
                #  => are all translated into the activation into the instruction block
                #  {"command": "AirbusFWB/SpeedSelPush"}
                #
                #  NB: The "long-press" is handled inside the activation when it detects a long press...
                #
                #  set-dataref: toliss/dataref/to/set
                #  => is translated into the activation into the instruction block
                #  {"set-dataref": "toliss/dataref/to/set"}
                #
                #  For Begin/End:
                #  activation-type: begin-end-command
                #  ...
                #  command: sim/apu/fire_test
                #  => is translated into the activation into the instruction block
                #  {"begin-end": "sim/apu/fire_test"}
                condition = ib.get(CONFIG_KW.CONDITION.value)
                delay = ib.get(CONFIG_KW.DELAY.value, 0.0)

                match keyw:

                    case CONFIG_KW.BEGIN_END.value:
                        return BeginEndCommand(name=name, simulator=simulator, path=command_block, delay=delay, condition=condition)

                    case CONFIG_KW.SET_SIM_VARIABLE.value:
                        # Parse instruction_block to get values
                        # to do: If no value to set, use value of parent dataref (dataref in parent block)
                        return SetDataref(
                            simulator=simulator,
                            path=command_block,
                            formula=instruction_block.get("formula"),
                            text_value=instruction_block.get("text"),
                            delay=delay,
                            condition=condition,
                        )

                    case CONFIG_KW.COMMAND.value:
                        if CockpitInstruction.is_cockpit_instruction(command_block):
                            ci = CockpitInstruction.new(cockpit=simulator.cockpit, name=name, instruction=command_block, instruction_block=ib)
                            if ci is not None:
                                return ci
                            logger.warning(f"{name}: could not create Cockpit Instruction ({command_block}, {ib})")
                        return Command(name=name, simulator=simulator, path=command_block, delay=delay, condition=condition)

                    case CONFIG_KW.VIEW.value:
                        logger.log(DEPRECATION_LEVEL, "«view» command no longer available, use regular command instead")
                        if CockpitInstruction.is_cockpit_instruction(command_block):
                            ci = CockpitInstruction.new(cockpit=simulator.cockpit, name=name, instruction=command_block, instruction_block=ib)
                            if ci is not None:
                                return ci
                            logger.warning(f"{name}: could not create Cockpit Instruction ({command_block}, {ib})")
                        return Command(name=name, simulator=simulator, path=command_block, delay=delay, condition=condition)

                    case CONFIG_KW.LONG_PRESS.value:
                        logger.log(DEPRECATION_LEVEL, "long press commands no longer available, use regular command instead")
                        return Command(name=name, simulator=simulator, path=command_block, delay=delay, condition=condition)

                    case _:
                        logger.warning(f"no instruction for {keyw}")
                        return None

            if type(command_block) in [list, tuple]:
                # List of instructions
                # Example:
                #  view: [{command: AirbusFBW/PopUpSD, condition: ${AirbusFBW/PopUpStateArray[7]} not}]
                return MacroInstruction(
                    name=name,
                    performer=simulator,
                    factory=simulator.cockpit,
                    instructions=command_block,
                    delay=instruction_block.get(CONFIG_KW.DELAY.value, 0.0),
                    condition=instruction_block.get(CONFIG_KW.CONDITION.value),
                )

            if type(command_block) is dict:
                # Single instruction block
                # Example:
                # - command: AirbusFBW/PopUpSD
                #   condition: ${AirbusFBW/PopUpStateArray[7]} not
                if CONFIG_KW.BEGIN_END.value in command_block:
                    cmdargs = command_block.get(CONFIG_KW.BEGIN_END.value)
                    if type(cmdargs) is str:
                        return BeginEndCommand(
                            name=name,
                            simulator=simulator,
                            path=cmdargs,
                            delay=command_block.get(CONFIG_KW.DELAY.value, 0.0),
                            condition=command_block.get(CONFIG_KW.CONDITION.value),
                        )

                # Single instruction block
                # Example:
                #  set-dataref: dataref/to/set
                #  formula: ${state:activation_count}
                #  delay: 2
                if CONFIG_KW.SET_SIM_VARIABLE.value in command_block:
                    cmdargs = command_block.get(CONFIG_KW.SET_SIM_VARIABLE.value)
                    if type(cmdargs) is str:
                        return SetDataref(
                            simulator=simulator,
                            path=cmdargs,
                            value=command_block.get(CONFIG_KW.VALUE.value),
                            formula=command_block.get(CONFIG_KW.FORMULA.value),
                            delay=command_block.get(CONFIG_KW.DELAY.value, 0.0),
                            condition=command_block.get(CONFIG_KW.CONDITION.value),
                        )

                # Single instruction block
                # Example:
                #  view: {command: AirbusFBW/PopUpSD, condition: ${AirbusFBW/PopUpStateArray[7]} not}
                for local_keyw in [CONFIG_KW.VIEW.value, CONFIG_KW.COMMAND.value, CONFIG_KW.LONG_PRESS.value]:
                    if local_keyw in command_block:
                        cmdargs = command_block.get(local_keyw)
                        if type(cmdargs) is str:
                            return Command(
                                name=name,
                                simulator=simulator,
                                path=cmdargs,
                                delay=command_block.get(CONFIG_KW.DELAY.value, 0.0),
                                condition=command_block.get(CONFIG_KW.CONDITION.value),
                            )

                kwlist = [CONFIG_KW.VIEW.value, CONFIG_KW.COMMAND.value, CONFIG_KW.SET_SIM_VARIABLE.value]
                logger.debug(f"could not find {kwlist} in {command_block}")

            # logger.debug(f"could not find {keyw} in {instruction_block}")
            return None

        if type(instruction_block) in [list, tuple]:
            return MacroInstruction(name=name, performer=simulator, factory=simulator.cockpit, instructions=instruction_block)

        if type(instruction_block) is not dict:
            logger.warning(f"invalid instruction block {instruction_block} ({type(instruction_block)})")

        if len(instruction_block) == 0:
            logger.debug(f"{name}: instruction block is empty")
            return None

        # Each of the keyword below can be a single instruction or a block
        # If we find the keyword, we build the corresponding Instruction.
        # if we don't find the keyword, or if what the keyword points at it not
        # a string (single instruction), an instruction block, or a list of instructions,
        # we return None to signify "not found". Warning message also issued.
        for keyword in [CONFIG_KW.BEGIN_END.value, CONFIG_KW.SET_SIM_VARIABLE.value, CONFIG_KW.COMMAND.value, CONFIG_KW.VIEW.value]:
            attempt = try_keyword(instruction_block, keyword)
            if attempt is not None:
                # logger.debug(f"got {keyword} in {instruction_block}")
                return attempt

        logger.warning(f"could not find instruction in {instruction_block}")
        return None


# Instructions to simulator
#
class Command(XPlaneInstruction, CommandAPI):
    """
    X-Plane simple Command, executed by CommandOnce API.
    """

    # The following command keywords are not executed, ignored with a warning
    NOT_A_COMMAND = [
        "none",
        "noop",
        "nooperation",
        "nocommand",
        "donothing",
    ]  # all forced to lower cases, -/:_ removed

    def __init__(self, simulator: XPlane, path: str, name: str | None = None, delay: float = 0.0, condition: str | None = None):
        XPlaneInstruction.__init__(self, name=name if name is not None else path, simulator=simulator, delay=delay, condition=condition)
        CommandAPI.__init__(self, path=path, api=simulator)
        self.path = path  # some/command

    def __str__(self) -> str:
        return f"{self.name} ({self.path})"

    @property
    def is_no_operation(self) -> bool:
        return self.path is not None and self.path.lower().replace("-", "") in Command.NOT_A_COMMAND

    def is_valid(self) -> bool:
        return super().is_vamie and not self.is_no_operation

    def _execute(self) -> bool:
        """Submit execution to API"""
        return CommandAPI.execute(self)


class BeginEndCommand(Command):
    """
    X-Plane long command, executed between CommandBegin/CommandEnd API.
    """

    def __init__(self, simulator: XPlane, path: str, name: str | None = None, delay: float = 0.0, condition: str | None = None):
        Command.__init__(self, simulator=simulator, path=path, name=name, delay=0.0, condition=condition)  # force no delay for commandBegin/End
        self.is_on = False

    def _execute(self) -> bool:
        """Execute command through API supplied at creation"""
        if not self.is_valid:
            logger.error(f"command {self.path} not found")
            return -1
        self.is_on = not self.is_on
        return self.simulator.set_command_is_active_without_duration(path=self.path, active=self.is_on)


class SetDataref(XPlaneInstruction):
    """
    Instruction to update the value of a dataref in X-Plane simulator.

    We only use XPlaneInstruction._execute().
    """

    def __init__(
        self,
        simulator: XPlane,
        path: str,
        value=None,
        formula: str | None = None,
        text_value: str | None = None,
        delay: float = 0.0,
        condition: str | None = None,
    ):
        XPlaneInstruction.__init__(self, name=path, simulator=simulator, delay=delay, condition=condition)

        # 1. Variable to set
        self.path = path  # some/dataref/to/set
        if Dataref.is_internal_variable(path):
            self._variable = simulator.cockpit.get_variable(path, factory=simulator.cockpit)
        else:
            self._variable = simulator.get_variable(path)

        # 2. Value to set
        # Case 1: Generic, non computed static fixed value
        self._value = value

        # Case 2: Formula for numeric value
        self._formula = formula
        self.formula = None
        if self._formula is not None:
            self.formula = Formula(owner=simulator, formula=formula)  # no button, no formula?

        # Case 3: Text value for string
        self._text_value = text_value
        self.text_value = None
        if self._text_value is not None:
            self.text_value = StringWithVariables(owner=simulator, message=self._text_value, name=f"{type(self).__name__}({self.path})")

        if self.formula is not None and self.text_value is not None:
            logger.warning(f"{type(self).__name__} for {self.path} has both formula and text value")

    def __str__(self) -> str:
        return "set-dataref: " + self.name

    @property
    def value(self):
        if self.formula is not None:
            if self.text_value is not None:
                logger.warning(f"{type(self).__name__} for {self.path} has both formula and text value, returning formula (text value ignored)")
            return self.formula.value
        if self.text_value is not None:
            return self.text_value.value
        return self._value

    @value.setter
    def value(self, value):
        # Set static value
        self._value = value

    @property
    def valid(self) -> bool:
        return isinstance(self._variable, Variable)

    def _execute(self) -> bool:
        """Execute command through API supplied at creation"""
        if not self.valid:
            logger.error(f"set dataref command is invalid ({self.path})")
            return False
        self._variable.value = self.value
        if isinstance(self._variable, Dataref):
            return self._variable.write()
        return True


# Events from simulator
#
class CommandActiveEvent(SimulatorEvent):
    """Command Active Event

    Sent by X-Plane when the command is activated. A command is activated "twice", once with the active=true,
    and once with active=false. For regular commands, either one can safely be ignored.
    When the event occurs it simply is reported on console log.
    To execute instructions following the coccurence of the event, it is necessry to define an Observable of type event
    and supply Instruction(s) to execute in the definition of the observable.
    """

    def __init__(self, sim: Simulator, command: str, is_active: bool, cascade: bool, autorun: bool = True):
        """Simulator Event: Something occurred in the simulator software.

        Args:
        """
        self.name = command
        self.is_active = is_active
        self.cascade = cascade
        SimulatorEvent.__init__(self, sim=sim, autorun=autorun)

    def __str__(self):
        return f"{self.sim.name}:{self.name}@{self.timestamp}"

    def info(self):
        return super().info() | {"path": self.name, "cascade": self.cascade}

    def run(self, just_do_it: bool = False) -> bool:
        # derifed classes may perform more sophisticated actions
        # to chain one or more action, use observables based on simulator events.

        if just_do_it:
            logger.debug(f"event {self.name} occured in simulator with active={self.is_active}")
            if self.sim is None:
                logger.warning("no simulator")
                return False
            activity = self.sim.cockpit.activity_database.get(self.name)
            if activity is None:
                logger.warning(f"activity {self.name} not found in database")
                return False
            try:
                logger.debug(f"activating {activity.name}..")
                self.handling()
                activity.activate(value=self.is_active, cascade=self.cascade)
                self.handled()
                logger.debug("..activated")
            except:
                logger.warning("..activated with error", exc_info=True)
                return False

        else:
            self.enqueue()
            logger.debug("enqueued")
        return True


# Connector to X-Plane status (COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS)
# 0 = Connection monitor to X-Plane is not running
# 1 = Connection monitor to X-Plane running, not connected to websocket
# 2 = Connected to websocket, WS receiver not running
# 3 = Connected to websocket, WS receiver running
# 4 = WS receiver has received data from simulator


# X-Plane Status
class XPLANE_STATUS(IntEnum):
    NO_SIMULATOR = 0
    BEACON_DETECTED = 1
    API_REACHABLE = 2
    MAIN_MENU = 3
    HAS_META_DATA = 4
    WS_CONNECTED = 5
    RECEIVING_DATA = 6
    AIRCRAFT_LOADED = 7  # i.e; has all meta data


# #############################################
# SIMULATOR
#
class XPlane(XPWebsocketAPI, Simulator, SimulatorVariableListener):
    """
    Get data from XPlane via network.
    Use a class to implement RAI Pattern for the UDP socket.

    Same attribute in both Simulator and XPWebsocketAPI
    # logger.debug(list(set(dir(Simulator)) & set(dir(XPWebsocketAPI))))

        execute
            Simulator.execute(Instruction)
            XPWebsocketAPI.execute(Command, duration)

        start
            Simulator.start -> abstract -> XPlane.start overrides
            XPWebsocketAPI.start -> start ws_receiver

            XPlane.start calls XPWebsocketAPI.start (i.e. "shadows" it)

        First attribute found is used, so ORDER OR PARENT CLASSES IS IMPORTANT:
        1. XPWebsocketAPI
        2. Simulator
        3. SimulatorVariableListener

    """

    name = "X-Plane"
    MAX_WARNING = 5  # number of times it reports it cannot connect

    def __init__(self, cockpit, environ):
        self._inited = False
        self._xplane_status = XPLANE_STATUS.MAIN_MENU  # dummy
        self.xplane_status = XPLANE_STATUS.NO_SIMULATOR  # real init with feedback

        # X-Plane class internals
        #
        self._running_time = Dataref(path=RUNNING_TIME, simulator=self)  # cheating, side effect, works for rest api only, do not force!
        self._aircraft_path = Dataref(path=AIRCRAFT_LOADED, simulator=self)

        self._wait_for_resource = threading.Event()
        self._wait_for_resource.set()
        self._check_for_resource = threading.Event()
        self._check_for_resource.set()
        self._terminating = False

        self._dataref_by_id = {}  # {dataref-id: Dataref}
        self._max_datarefs_monitored = 0  # max(len(self._dataref_by_id))

        self.cmdevents = set()  # list of command active events currently monitored
        self._max_events_monitored = 0

        self._permanent_observables: List[Observable] = []  # cannot create them now since XPlane does not exist yet (subclasses of Observable)
        self._observables: Observables | None = None  # local config observables for this simulator <sim>/resources/observables.yaml

        Simulator.__init__(self, cockpit=cockpit, environ=environ)
        self.cockpit.set_logging_level(__name__)

        SimulatorVariableListener.__init__(self, name=self.name)

        # Websocket
        #
        self._beacon = XPlaneBeacon()
        self._beacon.set_callback(self.beacon_callback)
        self.dynamic_timeout = RECONNECT_TIMEOUT
        self.xp_home = environ.get(ENVIRON_KW.SIMULATOR_HOME.value)
        self.api_host = environ.get(ENVIRON_KW.API_HOST.value, API_HOST)
        self.api_port = environ.get(ENVIRON_KW.API_PORT.value, API_TPC_PORT)
        self.api_path = environ.get(ENVIRON_KW.API_PATH.value, API_PATH)
        self.api_version = environ.get(ENVIRON_KW.API_VERSION.value, API_MIN_VERSION_STR)

        XPWebsocketAPI.__init__(self, host=self.api_host[0], port=self.api_host[1], api=self.api_path, api_version=self.api_version, use_rest=USE_REST)
        # XPWebsocketAPI callbacks
        self.add_callback(cbtype=CALLBACK_TYPE.ON_DATAREF_UPDATE, callback=self.dataref_newvalue_callback)
        self.add_callback(cbtype=CALLBACK_TYPE.ON_COMMAND_ACTIVE, callback=self.command_active_callback)
        self.add_callback(cbtype=CALLBACK_TYPE.ON_OPEN, callback=self._on_ws_open)
        self.add_callback(cbtype=CALLBACK_TYPE.ON_CLOSE, callback=self._on_ws_close)
        self.add_callback(cbtype=CALLBACK_TYPE.AFTER_START, callback=self._on_start)
        self.add_callback(cbtype=CALLBACK_TYPE.BEFORE_STOP, callback=self._on_stop)

        self.init()

    def __del__(self):
        if not self._inited:
            return
        self.register_bulk_command_is_active_event(paths=self.cmdevents, on=False)
        self.cmdevents = set()
        self.register_bulk_dataref_value_event(datarefs=self._dataref_by_id, on=False)
        self._dataref_by_id = {}
        self.disconnect()

    def init(self):
        if self._inited:
            return
        # Create internal variable to hold the connection status
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=0, cascade=True)
        self._inited = True

    def get_version(self) -> list:
        return [f"{type(self).__name__} {__version__}"]

    @property
    def xplane_status(self) -> XPLANE_STATUS:
        """Should use REST API for some purpose"""
        return self._xplane_status

    @property
    def xplane_status_str(self) -> str:
        """Should use REST API for some purpose"""
        return f"{XPLANE_STATUS(self._xplane_status).name}"

    @xplane_status.setter
    def xplane_status(self, xplane_status: XPLANE_STATUS):
        if self._xplane_status != xplane_status:
            self._xplane_status = xplane_status
            logger.info(f"X-Plane status is now {self.xplane_status_str}")

    # ################################
    # Factories
    #
    def variable_factory(self, name: str, is_string: bool = False, creator: str = None) -> Dataref:
        # logger.debug(f"creating xplane dataref {name}")
        if Dataref.is_internal_variable(name):
            logger.warning(f"request to create internal variable {name}")
        variable = Dataref(path=name, simulator=self, is_string=is_string)
        self.set_rounding(variable)
        self.set_frequency(variable)
        if creator is not None:
            variable._creator = creator
        return variable

    def instruction_factory(self, name: str, instruction_block: str | dict) -> XPlaneInstruction:
        # logger.debug(f"creating xplane instruction {name}")
        return XPlaneInstruction.new(name=name, simulator=self, instruction_block=instruction_block)

    def replay_event_factory(self, name: str, value):
        logger.debug(f"creating replay event {name}")
        return DatarefEvent(sim=self, dataref=name, value=value, cascade=True, autorun=False)

    # ################################
    # Others
    #
    def is_night(self) -> bool:
        obs = list(filter(lambda o: type(o) is DaytimeObservable, self.observables))
        return obs[0].is_night() if len(obs) > 0 else False

    def datetime(self, zulu: bool = False, system: bool = False) -> datetime:
        """Returns the *simulator* date and time of simulation

        Args:
        zulu (bool): Returns UTC time of simulation
        system (bool); Returns system time rather than simulation time.

        """
        if not self.cockpit.variable_database.exists(DATETIME_DATAREFS[0]):  # !! hack, means dref not created yet
            return super().datetime(zulu=zulu, system=system)
        now = datetime.now().astimezone()
        days = self.get_simulator_variable_value("sim/time/local_date_days")
        secs = self.get_simulator_variable_value("sim/time/local_date_sec")
        if not system and days is not None and secs is not None:
            simnow = datetime(year=now.year, month=1, day=1, hour=0, minute=0, second=0, microsecond=0).astimezone()
            simnow = simnow + timedelta(days=days) + timedelta(days=secs)
            return simnow
        return now

    # ################################
    # Observables
    #
    @property
    def observables(self) -> list:
        # This is the collection of "permanent" observables (coded)
        # and simulator observables (in <simulator base>/resources/observables.yaml)
        ret = self._permanent_observables
        if self._observables is not None:
            if hasattr(self._observables, "observables"):
                ret = ret + self._observables.observables
            elif type(self._observables) is dict:
                ret = ret + list(self._observables.values())
            elif type(self._observables) is list:
                ret = ret + self._observables
            else:
                logger.warning(f"observables: {type(self._observables)} unknown")
        return ret

    def load_observables(self):
        if self._observables is not None:  # load once
            return
        fn = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", RESOURCES_FOLDER, OBSERVABLES_FILE))
        if os.path.exists(fn):
            config = {}
            with open(fn, "r") as fp:
                config = yaml.load(fp)
            self._observables = Observables(config=config, simulator=self)
            for o in self._observables.get_observables():
                self.cockpit.register_observable(o)
            logger.info(f"loaded {len(self._observables.observables)} {self.name} simulator observables")
        else:
            logger.info(f"no {self.name} simulator observables")

    def create_permanent_observables(self):
        # Permanent observables are "coded" observables
        # They are created the first time add_permanently_monitored_simulator_variables() or add_permanently_monitored_simulator_events() is called
        cd_obs = self.cockpit.get_permanent_observables()
        if len(self._permanent_observables) > 0 or len(cd_obs) == 0:
            return
        self._permanent_observables = [obs(simulator=self) for obs in cd_obs]
        for o in self._permanent_observables:
            self.cockpit.register_observable(o)
        logger.info(f"loaded {len(self._permanent_observables)} permanent simulator observables")
        self.load_observables()

    #
    # Datarefs
    def aircraft_changed(self):
        """When notified that the aircraft has changed, we need to reload all datarefs and commands (since they all changed)"""
        self.reload_caches(force=True)

    def get_variables(self) -> set:
        """Returns the list of datarefs for which cockpitdecks wants to be notified of changes."""
        ret = set(PERMANENT_SIMULATOR_VARIABLES)
        # Simulator variables
        for obs in self.observables:
            ret = ret | obs.get_variables()
        # Cockpit variables
        cockpit_vars = self.cockpit.get_variables()
        if len(cockpit_vars) > 0:
            ret = ret | cockpit_vars
        # Aircraft variables
        if self.is_aircraft_loaded:
            aircraft_vars = self.cockpit.aircraft.get_variables()
            if len(aircraft_vars) > 0:
                ret = ret | aircraft_vars
        return ret

    def simulator_variable_changed(self, data: SimulatorVariable):
        pass

    # Events
    #
    def get_activities(self) -> set:
        """Returns the list of commands for which cockpitdecks wants to be notified of activation."""
        ret = set(PERMANENT_SIMULATOR_EVENTS)
        for obs in self.observables:
            ret = ret | obs.get_activities()
        # Add cockpit's, which includes aircraft's
        more = self.cockpit.get_activities()
        if len(more) > 0:
            ret = ret | more
        # Aircraft variables
        if self.is_aircraft_loaded:
            more = self.cockpit.aircraft.get_activities()
            if len(more) > 0:
                ret = ret | more
        return ret

    # Variable management
    #
    def add_permanently_monitored_simulator_variables(self):
        """Add simulator variables coming from different sources (cockpit, simulator itself, etc.)
        that are always monitored (for all aircrafts)
        """
        self.create_permanent_observables()
        varnames = self.get_variables()
        drefs = {}
        for d in varnames:
            dref = self.get_variable(d)
            if dref is not None:
                if not isinstance(dref, SimulatorVariable):
                    logger.debug(f"variable {dref.name} is not a simulator variable, not monitored")
                    continue
                drefs[d] = dref
        logger.info(f"monitoring {len(drefs)} permanent simulator variables")
        if len(drefs) > 0:
            self.add_simulator_variables_to_monitor(simulator_variables=drefs, reason="permanent simulator variables")

    def clean_simulator_variables_to_monitor(self):
        if not self.connected:
            return
        self.register_bulk_dataref_value_event(datarefs=self._dataref_by_id, on=False)
        self._dataref_by_id = {}
        super().clean_simulator_variable_to_monitor()
        logger.debug("done")

    def cleanup_monitored_simulator_variables(self):
        nolistener = {}
        for dref in self._dataref_by_id.values():
            ident = dref.ident
            if ident is None:
                logger.warning(f"{dref.name} identifier not found")
                continue
            if len(dref.listeners) == 0:
                nolistener[ident] = dref
        logger.info(f"no listener for {', '.join([d.name for d in nolistener.values()])}")
        # self.register_bulk_dataref_value_event(datarefs=nolistener, on=False)

    def print_currently_monitored_variables(self, with_value: bool = True):
        pass
        # logger.log(SPAM_LEVEL, ">>>>> currently monitored variables is disabled")
        # self.cleanup_monitored_simulator_variables()
        # return
        # if with_value:
        #     values = [f"{k}: {d.name}={d.value}" for k, d in self._dataref_by_id.items()]
        #     logger.log(SPAM_LEVEL, f">>>>> currently monitored variables:\n{'\n'.join(sorted(values))}")
        #     return
        # logger.log(SPAM_LEVEL, f">>>>> currently monitored variables:\n{'\n'.join(sorted([d.name for d in self._dataref_by_id.values()]))}")

    def add_simulator_variables_to_monitor(self, simulator_variables: dict, reason: str | None = None):
        if not self.connected:
            logger.debug(f"would add {list(filter(lambda d: not Dataref.is_internal_variable(d), simulator_variables))}")
            return
        if len(simulator_variables) == 0:
            logger.debug("no variable to add")
            return
        # Add those to monitor
        datarefs = {}
        effectives = {}
        for d in simulator_variables.values():
            if not isinstance(d, SimulatorVariable):
                logger.debug(f"variable {d.name} is not a simulator variable")
                continue
            if not d.is_monitored:
                ident = d.ident
                if ident is not None:
                    if d.is_array and d.index is not None:
                        if ident not in datarefs:
                            datarefs[ident] = []
                        datarefs[ident].append(d)
                    else:
                        datarefs[ident] = d
            d.inc_monitor()
            effectives[d.name] = d
        super().add_simulator_variables_to_monitor(simulator_variables=effectives)

        if len(datarefs) > 0:
            self.register_bulk_dataref_value_event(datarefs=datarefs, on=True)
            self._dataref_by_id = self._dataref_by_id | datarefs
            dlist = []
            for d in datarefs.values():
                if type(d) is list:
                    for d1 in d:
                        dlist.append(d1.name)
                else:
                    dlist.append(d.name)
            logger.log(SPAM_LEVEL, f">>>>> add_simulator_variables_to_monitor: {reason}: added {dlist}")
            self._max_datarefs_monitored = max(self._max_datarefs_monitored, len(self._dataref_by_id))
        else:
            logger.debug("no variable to add")
        self.print_currently_monitored_variables()
        if MONITOR_RESOURCE_USAGE:
            logger.info(
                f">>>>>> monitoring variables++{len(simulator_variables)}({len(datarefs)})/{len(self._dataref_by_id)}/{self._max_datarefs_monitored} {reason if reason is not None else ''}"
            )

    def remove_simulator_variables_to_monitor(self, simulator_variables: dict, reason: str | None = None):
        if not self.connected and len(self.simulator_variable_to_monitor) > 0:
            logger.debug(f"would remove {simulator_variables.keys()}/{self._max_datarefs_monitored}")
            return
        if len(simulator_variables) == 0:
            logger.debug("no variable to remove")
            return
        # Add those to monitor
        datarefs = {}
        effectives = {}
        for d in simulator_variables.values():
            if not isinstance(d, SimulatorVariable):
                logger.debug(f"variable {d.name} is not a simulator variable")
                continue
            if d.is_monitored:
                effectives[d.name] = d
                if not d.dec_monitor():  # will be decreased by 1 in super().remove_simulator_variable_to_monitor()
                    ident = d.ident
                    if ident is not None:
                        if d.is_array and d.index is not None:
                            if ident not in datarefs:
                                datarefs[ident] = []
                            datarefs[ident].append(d)
                        else:
                            datarefs[ident] = d
                else:
                    logger.debug(f"{d.name} monitored {d.monitored_count} times, not removed")
            else:
                logger.debug(f"no need to remove {d.name}, not monitored")
        super().remove_simulator_variables_to_monitor(simulator_variables=effectives)

        if len(datarefs) > 0:
            self.register_bulk_dataref_value_event(datarefs=datarefs, on=False)
            for i in datarefs.keys():
                if i in self._dataref_by_id:
                    del self._dataref_by_id[i]
                else:
                    logger.warning(f"no dataref for id={self.all_datarefs.equiv(ident=i)}")
            dlist = []
            for d in datarefs.values():
                if type(d) is list:
                    for d1 in d:
                        dlist.append(d1.name)
                else:
                    dlist.append(d.name)
            logger.log(SPAM_LEVEL, f">>>>> remove_simulator_variables_to_monitor: {reason}: removed {dlist}")
        else:
            logger.debug("no variable to remove")
        self.print_currently_monitored_variables()
        if MONITOR_RESOURCE_USAGE:
            logger.info(
                f">>>>> monitoring variables--{len(simulator_variables)}({len(datarefs)})/{len(self._dataref_by_id)}/{self._max_datarefs_monitored} {reason if reason is not None else ''}"
            )

    def add_all_simulator_variables_to_monitor(self):
        if not self.connected:
            return
        # Add permanently monitored drefs
        self.add_permanently_monitored_simulator_variables()
        # Add those to monitor
        datarefs = {}
        for path in self.simulator_variable_to_monitor.keys():
            d = self.get_variable(path)
            if not isinstance(d, SimulatorVariable):
                logger.debug(f"variable {d.name} is not a simulator variable")
                continue
            if d is not None:
                ident = d.ident
                if ident is not None:
                    datarefs[d.ident] = d
                    d.inc_monitor()  # increases counter
                else:
                    logger.warning(f"{d.name} identifier not found")
            else:
                logger.warning(f"no dataref {path}")

        if len(datarefs) > 0:
            self.register_bulk_dataref_value_event(datarefs=datarefs, on=True)
            self._dataref_by_id = self._dataref_by_id | datarefs
            logger.log(SPAM_LEVEL, f">>>>> add_all_simulator_variables_to_monitor: added {[d.path for d in datarefs.values()]}")
        else:
            logger.debug("no simulator variable to monitor")

    def remove_all_simulator_variables_to_monitor(self):
        datarefs = [d for d in self.cockpit.variable_database.database.values() if type(d) is Dataref]
        if not self.connected and len(datarefs) > 0:
            logger.debug(f"would remove {', '.join([d.name for d in datarefs])}")
            return
        # This is not necessary:
        # self.remove_simulator_variable_to_monitor(datarefs)
        super().remove_all_simulator_variable()

    # Event management
    #
    def add_permanently_monitored_simulator_events(self):
        # self.create_permanent_observables() should be called before
        # like in add_permanently_monitored_simulator_variables()
        self.create_permanent_observables()
        cmds = self.get_activities()
        logger.info(f"monitoring {len(cmds)} permanent simulator events")
        if len(cmds) > 0:
            self.add_simulator_events_to_monitor(simulator_events=cmds, reason="permanent simulator events")

    def clean_simulator_events_to_monitor(self):
        if not self.connected:
            return
        self.register_bulk_command_is_active_event(paths=self.cmdevents, on=False)
        self.cmdevents = set()
        super().clean_simulator_event_to_monitor()
        logger.debug("done")

    def print_currently_monitored_events(self):
        pass
        # logger.log(SPAM_LEVEL, f">>>>> currently monitored events:\n{'\n'.join(sorted(self.cmdevents))}")

    def add_simulator_events_to_monitor(self, simulator_events, reason: str | None = None):
        if not self.connected:
            logger.debug(f"would add {self.remove_internal_events(simulator_events.keys())}")
            return
        if len(simulator_events) == 0:
            logger.debug("no event to add")
            return
        # Add those to monitor
        super().add_simulator_events_to_monitor(simulator_events=simulator_events)
        paths = set()
        for d in simulator_events:
            if d not in self.cmdevents:  # if not already monitored
                paths.add(d)
            else:
                logger.debug(f"{d} already monitored {self.simulator_event_to_monitor[d]} times")
        self.register_bulk_command_is_active_event(paths=paths, on=True)
        self.cmdevents = self.cmdevents | paths
        self._max_events_monitored = max(self._max_events_monitored, len(self.cmdevents))
        logger.log(SPAM_LEVEL, f">>>>> add_simulator_event_to_monitor: {reason}: added {paths}")
        self.print_currently_monitored_events()
        if MONITOR_RESOURCE_USAGE:
            logger.info(
                f">>>>> monitoring events++{len(simulator_events)}/{len(self.cmdevents)}/{self._max_events_monitored} {reason if reason is not None else ''}"
            )

    def remove_simulator_events_to_monitor(self, simulator_events: dict, reason: str | None = None):
        if not self.connected and len(self.simulator_event_to_monitor) > 0:
            logger.debug(f"would remove {simulator_events.keys()}/{self._max_events_monitored}")
            return
        if len(simulator_events) == 0:
            logger.debug("no event to remove")
            return
        # Add those to monitor
        paths = set()
        for d in simulator_events:
            if d in self.simulator_event_to_monitor.keys():
                if self.simulator_event_to_monitor[d] == 1:  # will be decreased by 1 in super().remove_simulator_event_to_monitor()
                    paths.add(d)
                else:
                    logger.debug(f"{d} monitored {self.simulator_event_to_monitor[d]} times")
            else:
                if d in self.cmdevents:
                    logger.warning(f"should not see this, path={d}, event monitored not registered?")
                logger.debug(f"no need to remove {d}")
        self.register_bulk_command_is_active_event(paths=paths, on=False)
        self.cmdevents = self.cmdevents - paths
        super().remove_simulator_events_to_monitor(simulator_events=simulator_events)
        logger.log(SPAM_LEVEL, f">>>>> remove_simulator_events_to_monitor: {reason}: removed {paths}")
        self.print_currently_monitored_events()
        if MONITOR_RESOURCE_USAGE:
            logger.info(
                f">>>>> monitoring events--{len(simulator_events)}/{len(self.cmdevents)}/{self._max_events_monitored} {reason if reason is not None else ''}"
            )

    def remove_all_simulator_events_to_monitor(self):
        if not self.connected and len(self.cmdevents) > 0:
            logger.debug(f"would remove {', '.join(self.cmdevents)}")
            return
        before = len(self.cmdevents)
        self.register_bulk_command_is_active_event(paths=self.cmdevents, on=False)
        logger.log(SPAM_LEVEL, f">>>>> remove_simulator_events_to_monitor: remove all: removed {self.cmdevents}")
        super().remove_all_simulator_event()
        if MONITOR_RESOURCE_USAGE:
            logger.info(f">>>>> monitoring events--{before}/{len(self.cmdevents)}/{self._max_events_monitored} remove all")

    def add_all_simulator_events_to_monitor(self):
        if not self.connected:
            return
        # Add permanently monitored drefs
        self.add_permanently_monitored_simulator_events()
        # Add those to monitor
        paths = set(self.simulator_event_to_monitor.keys())
        self.register_bulk_command_is_active_event(paths=paths, on=True)
        self.cmdevents = self.cmdevents | paths
        self._max_events_monitored = max(self._max_events_monitored, len(self.cmdevents))
        logger.log(SPAM_LEVEL, f">>>>> add_all_simulator_events_to_monitor: added {paths}")

    @property
    def is_aircraft_loaded(self) -> bool:
        """Returns whether aircraft is loaded

        To do so, scan `sim/aircraft/view/acf_relative_path` dataref for non null value.
        """
        a = self.dataref_value(dataref=self._aircraft_path)
        if a is not None and a != "":
            self.xplane_status = XPLANE_STATUS.AIRCRAFT_LOADED
            return True
        # logger.info(f"{self.name}: aircraft loaded ({a})" if ret else "no aircraft")
        return False

    @property
    def is_valid(self) -> bool:
        """Returns whether aircraft loaded and aircraft datarefs are available"""
        if self.has_data:
            if self.is_aircraft_loaded:
                logger.debug(f"{self.name}: aircraft loaded")
                return True
            else:
                logger.debug(f"{self.name}: no aircraft loaded, invalid")
        logger.debug(f"{self.name}: no meta data loaded, invalid")
        return False

    @property
    def connected_and_valid(self) -> bool:
        res = f"{self.name} "
        connected = self.connected
        valid = False
        res = res + ("connected" if connected else "not connected")
        if connected:
            valid = self.is_valid
            res = res + (", aircraft loaded" if valid else ", no aircraft loaded")
        else:
            self.xplane_status = XPLANE_STATUS.WS_CONNECTED
        logger.info(res)
        return connected and valid

    @property
    def status_info(self) -> str:
        return f"beacon={self._beacon.status_str}, api={self.status_str}, simulator={self.x_plane_status_str}"

    def dataref_newvalue_callback(self, dataref: str, value):
        cascade = dataref in self.simulator_variable_to_monitor
        # print(f"DREF {dataref}={value} ({cascade})")
        e = DatarefEvent(sim=self, dataref=dataref, value=value, cascade=cascade)
        self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)

    def command_active_callback(self, command: str, active: bool):
        # print(f"CMD  {command}={active}")
        e = CommandActiveEvent(sim=self, command=command, is_active=active, cascade=True)
        self.inc(COCKPITDECKS_INTVAR.COMMAND_ACTIVE_ENQUEUED.value)

    # ################################
    # Sequence of connection
    #
    def check_resource(self, resource: str, test: Callable, timeout: float = WAIT_FOR_RESOURCE_TIMEOUT):
        if self._terminating:
            logger.info(f"{self.name}: terminating, not checking for {resource}..")
            return
        self._check_for_resource.clear()
        while not self._check_for_resource.is_set() and not self._terminating:  # loop over a micro timeout to check for resource périodically"
            if test():
                self._check_for_resource.set()
                self._wait_for_resource.set()
            else:
                logger.info(f"{self.name}: ..waiting for {resource}..")
                self._check_for_resource.wait(timeout)

    def wait_for_resource(self, resource: str, test: Callable, timeout: float = 3600.0) -> bool:  # wait two hours before giving up...
        """Checks availability of requested resource"""
        if self._terminating:
            logger.info(f"{self.name}: terminating, not waiting for {resource}..")
            return False
        logger.info(f"{self.name}: waiting for {resource}..")
        self._wait_for_resource.clear()
        self.check_resource(resource=resource, test=test, timeout=WAIT_FOR_RESOURCE_TIMEOUT)
        if ret := self._wait_for_resource.wait(timeout):
            logger.info(f"{self.name}: ..{resource} loaded")
        else:
            logger.info(f"{self.name}: ..{resource} not loaded")
        return ret

    def terminate_wait_for_resource(self):
        self._wait_for_resource.set()

    @property
    def waiting_for_resource(self) -> bool:
        return not self._wait_for_resource.is_set()

    def wait_for_beacon(self) -> bool:
        """Checks availability of `sim/time/total_running_time_sec`"""

        def test_data() -> bool:
            if self._beacon.is_running:
                return self._beacon.receiving_beacon
            logger.warning("beacon not running")
            return False

        return self.wait_for_resource(resource="beacon", test=test_data)

    def wait_for_restapi(self) -> bool:
        """Checks availability of `sim/time/total_running_time_sec`"""

        def test_data() -> bool:
            return self.rest_api_reachable

        return self.wait_for_resource(resource="rest api", test=test_data)

    def wait_for_metadata(self) -> bool:
        """Checks availability of `sim/time/total_running_time_sec`"""

        def test_data() -> bool:
            self.reload_caches(force=True)
            return self.has_data

        return self.wait_for_resource(resource="meta data", test=test_data)

    def wait_for_websocket(self) -> bool:
        """Checks availability of `sim/time/total_running_time_sec`"""

        def test_data() -> bool:
            if self.websocket_connection_monitor_running:
                return self.connected
            logger.warning("websocket connection monitor not running")
            return False

        return self.wait_for_resource(resource="websocket connection", test=test_data)

    def wait_for_aircraft(self) -> bool:
        """Checks availability of `sim/aircraft/view/acf_relative_path`"""

        def test_data() -> bool:
            self.reload_caches(force=True)
            return self.is_aircraft_loaded

        return self.wait_for_resource(resource="aircraft", test=test_data)

    def lost_connection(self, who: str):
        if self._terminating:
            logger.info(f"connection lost while terminating")
            return
        logger.warning("<*> " * 30)
        logger.warning(f"no answer from {who}, investigating..")

        # Do we have a beacon?
        self.wait_for_beacon()
        self.xplane_status = XPLANE_STATUS.BEACON_DETECTED

        # Is the REST API reachable?
        self.wait_for_restapi()
        self.xplane_status = XPLANE_STATUS.API_REACHABLE

        # Is meta data available in REST API?
        self.wait_for_metadata()
        self.xplane_status = XPLANE_STATUS.HAS_META_DATA

        # Is the Websocket available and open?
        self.wait_for_websocket()
        self.xplane_status = XPLANE_STATUS.WS_CONNECTED

        # Is the aircraft loade?
        self.wait_for_aircraft()
        self.xplane_status = XPLANE_STATUS.AIRCRAFT_LOADED

        logger.warning("..investigated")

    # ################################
    # Interface to XPWebsocketAPI
    #
    def req_stats(self):
        stats = {}
        for r, v in self._requests.items():
            if v not in stats:
                stats[v] = 1
            else:
                stats[v] = stats[v] + 1
        if self._stats != stats:
            self._stats = stats
            logger.log(SPAM_LEVEL, f"requests statistics: {stats}")

    def _on_start(self, connected: bool):
        if not connected:
            return
        if not self.has_data:
            logger.warning("no data")
            if not self.wait_for_metadata():
                logger.warning("could not load meta data, aborting start")
                return
        else:
            logger.info("meta data loaded")

        self.clean_simulator_variables_to_monitor()
        self.add_all_simulator_variables_to_monitor()
        self.clean_simulator_events_to_monitor()
        self.add_all_simulator_events_to_monitor()

        if not self.is_aircraft_loaded:
            logger.info("no aircraft")
            if not self.wait_for_aircraft():
                logger.warning("could not load aircraft, aborting start")
                return
        else:
            logger.info("aircraft loaded")

        if self.is_aircraft_loaded:
            logger.info("request to reload pages")
            self.cockpit.reload_pages()  # to request page variables and take into account updated values
        # logger.info(f"{self.name} started")

    def _on_stop(self, connected: bool):
        pass

    def _on_ws_open(self):
        pass

    def _on_ws_close(self):
        if not self._terminating:
            self.lost_connection(who="websocket closed")

    def connect(self, reload_cache: bool = False):
        """
        Starts connect loop.
        """
        self._terminating = False
        self._beacon.start_monitor()
        super().connect(reload_cache)

    def disconnect(self):
        """
        End connect loop and disconnect
        """
        self._terminating = True
        super().disconnect()
        self._beacon.stop_monitor()

    def cleanup(self):
        """
        Called when before disconnecting.
        Just before disconnecting, we try to cancel dataref UDP reporting in X-Plane
        """
        logger.info("..requesting X-Plane to stop sending updates..")
        self.clean_simulator_variables_to_monitor()
        self.clean_simulator_events_to_monitor()

    def terminate(self):
        logger.debug(f"{'currently not ' if self.websocket_listener_running else ''}running.. terminating..")
        logger.info(f"terminating {self.name}..")
        self._terminating = True
        if self.waiting_for_resource:
            logger.info("..terminating wait for resource..")
            self.terminate_wait_for_resource()
        # sends instructions to stop sending values/events
        logger.info("..request to stop sending value updates and events..")
        self.remove_all_simulator_variables_to_monitor()
        self.remove_all_simulator_events_to_monitor()
        # stop receiving events from similator (websocket)
        logger.info("..stopping websocket listener..")
        self.stop()
        # cleanup/reset monitored variables or events
        logger.info("..deleting references to datarefs..")
        self.cleanup()
        logger.info("..disconnecting from simulator..")
        self.disconnect()
        logger.info(f"..{self.name} terminated")


## #################################################@
#
# Simulatior Information Structure
#
# PERMANENT_SIMULATION_VARIABLE_NAMES = set()


# class Simulation(SimulatorVariableListener):
#     """Information container for some variables
#     Variables are filled if available, which is not always the case...
#     """

#     def __init__(self, owner) -> None:
#         SimulatorVariableListener.__init__(self, name=type(self).__name__)
#         self.owner = owner
#         self._permanent_variable_names = PERMANENT_SIMULATION_VARIABLE_NAMES
#         self._permanent_variables = {}

#     def init(self):
#         for v in self._permanent_variable_names:
#             intvar = self.owner.get_variable(name=SimulatorVariable.internal_variable_name(v), factory=self)
#             intvar.add_listener(self)
#             self._permanent_variables[v] = intvar
#         logger.info(f"permanent variables: {', '.join([SimulatorVariable.internal_variable_root_name(v) for v in self._permanent_variables.keys()])}")

#     def simulator_variable_changed(self, data: SimulatorVariable):
#         """
#         This gets called when dataref AIRCRAFT_CHANGE_MONITORING_DATAREF is changed, hence a new aircraft has been loaded.
#         """
#         name = data.name
#         if SimulatorVariable.is_internal_variable(name):
#             name = SimulatorVariable.internal_variable_root_name(name)
#         if name not in self._permanent_variables:
#             logger.warning(f"{data.name}({type(data)})={data.value} unhandled")
#             return


# #
