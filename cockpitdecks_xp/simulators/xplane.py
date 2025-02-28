# Class for interface with X-Plane using UDP protocol.
#
from __future__ import annotations

import socket
import threading
import logging
import json
import base64
import traceback

import requests
from simple_websocket import Client, ConnectionClosed

from datetime import datetime, timedelta, timezone

from cockpitdecks_xp import __version__
from cockpitdecks import CONFIG_KW, ENVIRON_KW, SPAM_LEVEL, MONITOR_DATAREF_USAGE
from cockpitdecks.strvar import Formula
from cockpitdecks.instruction import MacroInstruction

from cockpitdecks.simulator import Simulator, SimulatorEvent, SimulatorInstruction
from cockpitdecks.simulator import SimulatorVariable, SimulatorVariableListener
from cockpitdecks.resources.intvariables import COCKPITDECKS_INTVAR

from ..resources.stationobs import WeatherStationObservable
from ..resources.daytimeobs import DaytimeObservable

logger = logging.getLogger(__name__)
# logger.setLevel(SPAM_LEVEL)  # To see which dataref are requested
# logger.setLevel(logging.DEBUG)

# #############################################
# CONFIGURATION AND OPTIONS
#
# Data too delicate to be put in constant.py
# !! adjust with care !!
# UDP sends at most ~40 to ~50 dataref values per packet.
RECONNECT_TIMEOUT = 10  # seconds, times between attempts to reconnect to X-Plane when not connected
RECEIVE_TIMEOUT = 5  # seconds, assumes no awser if no message recevied withing that timeout

XP_MIN_VERSION = 121400
XP_MIN_VERSION_STR = "12.1.4"
XP_MAX_VERSION = 121499
XP_MAX_VERSION_STR = "12.1.4"

# #############################################
# PERMANENT DATAREFS
#
# Always requested datarefs (time and simulation speed)
#
ZULU_TIME_SEC = "sim/time/zulu_time_sec"
DATETIME_DATAREFS = [
    "sim/time/local_date_days",
    "sim/time/local_time_sec",
    ZULU_TIME_SEC,
    "sim/time/use_system_time",
]
REPLAY_DATAREFS = [
    "sim/time/is_in_replay",
    "sim/time/sim_speed",
    "sim/time/sim_speed_actual",
]

PERMANENT_SIMULATOR_DATA = {}  # set(DATETIME_DATAREFS + REPLAY_DATAREFS)


# #############################################
# REST OBJECTS
#

# REST API model keywords

REST_COMMANDS = "commands"
REST_DATA = "data"
REST_DATAREFS = "datarefs"
REST_DURATION = "duration"
REST_IDENT = "id"
REST_INDEX = "index"
REST_ISACTIVE = "is_active"
REST_NAME = "name"
REST_PARAMS = "params"
REST_REQID = "req_id"
REST_RESULT = "result"
REST_SUCCESS = "success"
REST_TYPE = "type"
REST_VALUE = "value"
REST_VALUE_TYPE = "value_type"
INDICES = "_index_list"

# Dataref object
class XPRESTObject:

    def __init__(self, path: str) -> None:
        self.path = path
        self.config = None
        self.api = None
        self.valid = False

    @property
    def ident(self) -> int | None:
        if not self.valid:
            return None
        return self.config[REST_IDENT]

    @property
    def value_type(self) -> int | None:
        if not self.valid:
            return None
        return self.config[REST_VALUE_TYPE]

    def init(self, cache):
        self.config = cache.get(self.path)
        self.api = None
        self.valid = False
        if self.config is None:
            logger.error(f"{type(self)} {self.path} not found")
        else:
            self.api = cache.api
            self.valid = True

# A value in X-Plane Simulator
# value_type: float, double, int, int_array, float_array, data
#
class Dataref(SimulatorVariable, XPRESTObject):
    """
    A Dataref is an internal value of the simulation software made accessible to outside modules,
    plugins, or other software in general.

    From Spring 2024 on, this is no longer inspired from Sandy Barbour and the like in Python 2.
    Most of the original code has been removed, because unused.
    This is a modern implementation, specific to Cockpitdecks. It even use X-Plane 12.1 REST/WebSocket API.
    """

    def __init__(self, path: str, simulator: XPlane, is_string: bool = False):
        # Data
        SimulatorVariable.__init__(self, name=path, simulator=simulator, data_type="string" if is_string else "float")
        XPRESTObject.__init__(self, path=path)

        if SimulatorVariable.is_state_variable(path):
            traceback.print_stack()

        self.index = 0  # 4
        if "[" in path:  # sim/some/values[4]
            self.root_name = self.name[: self.name.find("[")]
            self.index = int(self.name[self.name.find("[") + 1 : self.name.find("]")])

    # @property
    # def dataref(self) -> str:
    #     return self.name

    def __str__(self) -> str:
        return f"{self.path}={self.value()}"

    @property
    def rest_value(self):
        if not self.valid:
            logger.error(f"dataref {self.path} not valid")
            return None
        url = f"{self.api_url}/datarefs/{self.ident}/value"
        response = requests.get(url)
        data = response.json()
        logger.debug(f"result: {data}")
        if REST_DATA in data and type(data[REST_DATA]) in [bytes, str]:
            return base64.b64decode(data[REST_DATA]).decode("ascii").replace("\u0000", "")
        return data[REST_DATA]

    def rest_write(self) -> bool:
        if not self.valid:
            logger.error(f"dataref {self.path} not valid")
            return False
        value = self.value()
        if self.value_type == "data":
            value = str(value).encode('ascii')
            value = base64.b64encode(value).decode("ascii")
        payload = {REST_IDENT: self.ident, REST_DATA: value}
        url = f"{self.api_url}/datarefs/{self.ident}/value"
        print(f"writing dataref {self.path}: {url}, {payload}") # logger.debug
        response = requests.patch(url, json=payload)
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"result: {data}")
            return True
        logger.error(f"write: {response.reason}")
        return False

    def ws_write(self) -> int:
        return self.simulator.set_dataref_value(self.name, self.value())

    def save(self) -> bool:
        if self._writable:
            if not self.is_internal:
                return self.ws_write() != -1
            else:
                logger.warning(f"{self.name} is internal variable, not saved to simulator")
        else:
            logger.warning(f"{self.name} not writable")
        return False

# Events from simulator
#
class DatarefEvent(SimulatorEvent):
    """Dataref Update Event"""

    def __init__(self, sim: Simulator, dataref: str, value: float | str, cascade: bool, autorun: bool = True):
        """Dataref Update Event.

        Args:
        """
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
# The following command keywords are not executed, ignored with a warning
NOT_A_COMMAND = [
    "none",
    "noop",
    "no-operation",
    "no-command",
    "do-nothing",
]  # all forced to lower cases, -/:_ removed

# An instruction in X-Plane Simulator
#
class XPlaneInstruction(SimulatorInstruction, XPRESTObject):
    """An Instruction sent to the XPlane Simulator to execute some action.

    [description]
    """

    def __init__(self, name: str, simulator: XPlane, delay: float = 0.0, condition: str | None = None, button: "Button" = None) -> None:
        SimulatorInstruction.__init__(self, name=name, simulator=simulator, delay=delay, condition=condition)
        XPRESTObject.__init__(self, path=name)

    @classmethod
    def new(cls, name: str, simulator: XPlane, instruction_block: dict) -> XPlaneInstruction | None:
        INSTRUCTIONS = [CONFIG_KW.BEGIN_END.value, CONFIG_KW.SET_SIM_VARIABLE.value, CONFIG_KW.COMMAND.value, CONFIG_KW.VIEW.value]

        def try_keyword(keyw) -> XPlaneInstruction | None:
            # list of instructions (simple or block)
            if type(instruction_block) in [list, tuple]:
                return MacroInstruction(
                    name=name,
                    performer=simulator,
                    factory=simulator.cockpit,
                    instructions=instruction_block,
                    delay=instruction_block.get(CONFIG_KW.DELAY.value, 0.0),
                    condition=instruction_block.get(CONFIG_KW.CONDITION.value),
                )

            if type(instruction_block) is not dict:
                logger.warning(f"invalid instruction block {instruction_block} ({type(instruction_block)})")

            command_block = instruction_block.get(keyw)

            # single simple command to execute
            if type(command_block) is str:
                # Examples:
                #  command: AirbusFWB/SpeedSelPush
                #  long-press: AirbusFWB/SpeedSelPull
                #  view: AirbusFBW/PopUpSD
                #  set-dataref: toliss/dataref/to/set
                #  begin-end: sim/apu/fire_test
                match keyw:

                    case CONFIG_KW.BEGIN_END.value:
                        return BeginEndCommand(
                                name=name,
                                simulator=simulator,
                                path=command_block)

                    case CONFIG_KW.SET_SIM_VARIABLE.value:
                        return SetDataref(
                            name=name,
                            simulator=simulator,
                            path=command_block,
                        )

                    case CONFIG_KW.VIEW.value:
                        return Command(
                            name=name,
                            simulator=simulator,
                            path=command_block,
                        )

                    case CONFIG_KW.COMMAND.value:
                        return Command(
                            name=name,
                            simulator=simulator,
                            path=command_block,
                        )

                    case CONFIG_KW.LONG_PRESS.value:
                        return Command(
                            name=name,
                            simulator=simulator,
                            path=command_block,
                        )

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
                #  begin-end: airbus/fire_eng1/test
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

        # Each of the keyword below can be a single instruction or a block
        # If we find the keyword, we build the corresponding Instruction.
        # if we don't find the keyword, or if what the keyword points at it not
        # a string (single instruction), an instruction block, or a list of instructions,
        # we return None to signify "not found". Warning message also issued.
        for keyword in [CONFIG_KW.BEGIN_END.value, CONFIG_KW.SET_SIM_VARIABLE.value, CONFIG_KW.COMMAND.value, CONFIG_KW.VIEW.value]:
            attempt = try_keyword(keyword)
            if attempt is not None:
                # logger.debug(f"got {keyword} in {instruction_block}")
                return attempt

        logger.warning(f"could not find instruction in {instruction_block}")
        return None

    def ws_execute(self) -> int:
        return -1

# Instructions to simulator
#
class Command(XPlaneInstruction):
    """
    A Button activation will instruct the simulator software to perform an action.
    A Command is the message that the simulation sofware is expecting to perform that action.
    """

    def __init__(self, simulator: XPlane, path: str, name: str | None = None, delay: float = 0.0, condition: str | None = None):
        XPlaneInstruction.__init__(self, name=name if name is not None else path, simulator=simulator, delay=delay, condition=condition)
        self.path = path  # some/command

    def __str__(self) -> str:
        return f"{self.name} ({self.path})"

    def is_valid(self) -> bool:
        return self.path is not None and self.path.lower().replace("-", "") not in NOT_A_COMMAND

    def rest_execute(self) -> bool:
        if not self.valid:
            logger.error(f"command {self.path} not found")
            return False
        payload = {IDENT: self.ident, DURATION: 0.0}
        url = f"{self.api_url}/command/{self.ident}/activate"
        response = requests.post(url, json=payload)
        data = response.json()
        if response.status_code == 200:
            logger.debug(f"result: {data}")
            return True
        logger.error(f"execute: {response}, {data}")
        return False

    def ws_execute(self) -> int:
        return self.simulator.execute_ws_command(path=self.path)

    def _execute(self):
        self.ws_execute()
        self.clean_timer()


class BeginEndCommand(Command):
    """
    A Button activation will instruct the simulator software to perform an action.
    A Command is the message that the simulation sofware is expecting to perform that action.
    """

    DURATION = 5

    def __init__(self, simulator: XPlane, path: str, name: str | None = None, delay: float = 0.0, condition: str | None = None):
        Command.__init__(self, simulator=simulator, path=path, name=name, delay=0.0, condition=condition)  # force no delay for commandBegin/End
        self.is_on = False

    def rest_execute(self) -> bool:
        if not self.valid:
            logger.error(f"command {self.path} not found")
            return False
        payload = {REST_IDENT: self.ident, REST_DURATION: self.DURATION}
        url = f"{self.api_url}/command/{self.ident}/activate"
        response = requests.post(url, json=payload)
        data = response.json()
        if response.status_code == 200:
            logger.debug(f"result: {data}")
            return True
        logger.error(f"execute: {response}, {data}")
        return False

    def ws_execute(self) -> int:
        if not self.is_valid:
            logger.error(f"command {self.path} not found")
            return -1
        self.is_on = not self.is_on
        return self.simulator.execute_ws_long_command(path=self.path, active=self.is_on)

    def _execute(self):
        self.ws_execute()
        self.clean_timer()


class SetDataref(XPlaneInstruction):
    """
    A Button activation will instruct the simulator software to perform an action.
    A Command is the message that the simulation sofware is expecting to perform that action.
    """

    def __init__(self, simulator: XPlane, path: str, value=None, formula: str | None = None, delay: float = 0.0, condition: str | None = None):
        XPlaneInstruction.__init__(self, name=path, simulator=simulator, delay=delay, condition=condition)
        self.path = path  # some/command
        self._value = value
        self._formula = formula
        self.formula = None
        if self._formula is not None:
            self.formula = Formula(owner=simulator, formula=formula)  # no button, no formula?

    def __str__(self) -> str:
        return "set-dataref: " + self.name

    @property
    def value(self):
        if self.formula is not None:
            return self.formula.value()
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def ws_execute(self) -> int:
        if not self.valid:
            logger.error(f"set-dataref {self.path} invalid")
            return -1
        if Dataref.is_internal_variable(self.path):
            d = self.get_variable(self.path)
            d.update_value(new_value=self.value, cascade=True)
            logger.debug(f"written local dataref ({self.path}={self.value})")
            return -1
        return self.simulator.set_dataref_value(path=self.path, value=self.value)

    def _execute(self):
        self.ws_execute()


# Events from simulator
#
class CommandEvent(SimulatorEvent):
    """Data Update Event"""

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
        if just_do_it:
            logger.debug(f"event {self.name} occured in simulator")
        else:
            self.enqueue()
            logger.debug("enqueued")
        return True


# #############################################
# REST API
#
class Cache:
    def __init__(self, api: XPlaneREST) -> None:
        self.api = api
        self._data = dict()
        self._ids = dict()
        self._valid = set()

    def load(self, path):
        url = self.api.api_url + path
        response = requests.get(url)
        if response.status_code == 200:  # We have version 12.1.4 or above
            raw = response.json()
            data = raw[REST_DATA]
            self._data = {c[REST_NAME]: c for c in data}
            self._ids = {c[REST_IDENT]: c for c in data}
            self._valid = set()
            logger.debug(f"{path[1:]} cached")
            return
        logger.error(f"load: response={response.status_code}")

    @property
    def has_data(self) -> bool:
        return self._data is not None and len(self._data) > 0

    def get(self, name):
        return self.get_by_name(name=name)

    def get_by_name(self, name):
        r = self._data.get(name)
        if r is not None:
            self._valid.add(name)
            return r
        return None

    def get_by_id(self, ident: int):
        r = self._ids.get(ident)
        if r is not None:
            self._valid.add(r[REST_NAME])
            return r
        return None

    def is_valid(self, name):
        return name in self._valid


class XPlaneREST:

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.version = ""  # v1, v2, etc.
        self._api = ""  # /v1, /v2, to be appended to URL
        self._capabilities = {}

        self.all_datarefs: Cache | None = None
        self.all_commands: Cache | None = None

    @property
    # See https://stackoverflow.com/questions/7019643/overriding-properties-in-python
    # to overwrite @property definition
    def api_url(self) -> str | None:
        return f"http://{self.host}:{self.port}/api{self._api}"

    @property
    def ws_url(self) -> str | None:
        url = self.api_url
        if url is not None:
            return url.replace("http:", "ws:")
        return None

    @property
    def api_is_available(self) -> bool:
        try:
            response = requests.get(self.api_url + "/v1/datarefs/count")
            if response.status_code == 200:
                return True
        except:
            logger.error("is_available", exc_info=True)
        return False

    def capabilities(self) -> dict:
        # Guess capabilties and caches it
        if len(self._capabilities) > 0:
            return self._capabilities
        try:
            response = requests.get(self.api_url + "/capabilities")
            if response.status_code == 200:  # We have version 12.1.4 or above
                self._capabilities = response.json()
                logger.debug(f"capabilities: {self._capabilities}")
                return self._capabilities
            response = requests.get(self.api_url + "/v1/datarefs/count")
            if response.status_code == 200:  # OK, /api/v1 exists, we use it, we have version 12.1.1 or above
                self._capabilities = {"api": {"versions": ["v1"]}, "x-plane": {"version": "12"}}
                logger.debug(f"capabilities: {self._capabilities}")
                return self._capabilities
            logger.error(f"capabilities: response={response.status_code}")
        except:
            logger.error("capabilities", exc_info=True)
        return self._capabilities

    @property
    def xp_version(self) -> str | None:
        a = self._capabilities.get("x-plane")
        if a is None:
            return None
        return a.get("version")

    def set_api(self, api: str | None = None):
        api_details = self._capabilities.get("api")
        if api_details is not None:
            api_versions = api_details.get("versions")
            if api is None:
                if api_versions is None:
                    logger.error("cannot determine api")
                    return
                api = api_versions[-1]
            if api in api_versions:
                self.version = api
                self._api = "/" + api
                logger.info(f"set api {api}, xp {self.xp_version}")
            else:
                logger.warning(f"no api {api} in {api_versions}")
            return
        logger.warning(f"could not check api {api} in {self._capabilities}")

    def reload_caches(self):
        self.all_datarefs = Cache(self)
        self.all_datarefs.load("/datarefs")
        if self.version == "v2":  # >
            self.all_commands = Cache(self)
            self.all_commands.load("/commands")
        else:
            self.all_commands = {}

    def get_dataref_info_by_name(self, path: str):
        return self.all_datarefs.get_by_name(path) if self.all_datarefs is not None else None

    def get_command_info_by_name(self, path: str):
        return self.all_commands.get_by_name(path) if self.all_commands is not None else None

    def get_dataref_info_by_id(self, ident: int):
        return self.all_datarefs.get_by_id(ident) if self.all_datarefs is not None else None

    def get_command_info_by_id(self, ident: int):
        return self.all_commands.get_by_id(ident) if self.all_commands is not None else None

# #############################################
# WEBSOCKET API
#
class XPlaneWebSocket(XPlaneREST):
    """
    Get data from XPlane via network.
    Use a class to implement RAI Pattern for the UDP socket.
    """

    MAX_WARNING = 5

    def __init__(self, host: str, port: int):
        # Open a UDP Socket to receive on Port 49000
        XPlaneREST.__init__(self, host=host, port=port)
        hostname = socket.gethostname()
        self.local_ip = socket.gethostbyname(hostname)

        self.ws = None  # None = no connection
        self.req_number = 0
        self._requests = {}

        self.should_not_connect = None  # threading.Event(
        self.connect_thread = None  # threading.Thread()
        self._already_warned = 0

    def next_req(self):
        self.req_number = self.req_number + 1
        return self.req_number

    @property
    def connected(self) -> bool:
        res = self.ws is not None
        if not res and not self._already_warned > self.MAX_WARNING:
            if self._already_warned == self.MAX_WARNING:
                logger.warning("no connection (last warning)")
            else:
                logger.warning("no connection")
            self._already_warned = self._already_warned + 1
        return res

    def start(self):
        logger.warning("nothing to start")

    def stop(self):
        logger.warning("nothing to stop")

    def cleanup(self):
        self.disconnect_websocket()

    def connect_websocket(self):
        if self.ws is None:
            try:
                if self.api_is_available:
                    self.capabilities()
                    self.set_api("v2")
                    url = self.ws_url
                    if url is not None:
                        self.ws = Client.connect(url)
                        self.reload_caches()
                        logger.info(f"websocket opened at {url}")
                    else:
                        logger.warning(f"web socket url is none {url}")
            except:
                logger.error("connect", exc_info=True)

    def disconnect_websocket(self):
        if self.ws is not None:
            self.ws.close()
            self.ws = None
        logger.info("websocket closed")

    def connect_loop(self):
        """
        Trys to connect to X-Plane indefinitely until self.should_not_connect is set.
        If a connection fails, drops, disappears, will try periodically to restore it.
        """
        logger.debug("starting connection monitor..")
        MAX_TIMEOUT_COUNT = 5
        WARN_FREQ = 10
        number_of_timeouts = 0
        cnt = 0
        while self.should_not_connect is not None and not self.should_not_connect.is_set():
            if not self.connected:
                try:
                    self.connect_websocket()
                    if self.connected:
                        self._already_warned = 0
                        number_of_timeouts = 0
                        logger.info(f"beacon: {self.capabilities()}")
                        # if "XPlaneVersion" in self.beacon_data:
                        #     curr = self.beacon_data["XPlaneVersion"]
                        #     if curr < XP_MIN_VERSION:
                        #         logger.warning(f"X-Plane version {curr} detected, minimal version is {XP_MIN_VERSION}")
                        #         logger.warning(f"Some features in Cockpitdecks may not work properly")
                        #     elif curr > XP_MAX_VERSION:
                        #         logger.warning(f"X-Plane version {curr} detected, maximal version is {XP_MAX_VERSION}")
                        #         logger.warning(f"Some features in Cockpitdecks may not work properly")
                        #     else:
                        #         logger.info(f"X-Plane version meets current criteria ({XP_MIN_VERSION}<= {curr} <={XP_MAX_VERSION})")
                        #         logger.info(f"connected")
                        logger.debug("..connected, starting websocket listener..")
                        self.start()
                        self.inc(COCKPITDECKS_INTVAR.STARTS.value)
                        logger.info("..websocket listener started..")
                    else:
                        if self.ws_event is not None and not self.ws_event.is_set():
                            number_of_timeouts = number_of_timeouts + 1
                            if number_of_timeouts <= MAX_TIMEOUT_COUNT:  # attemps to reconnect
                                logger.info(f"timeout received ({number_of_timeouts}/{MAX_TIMEOUT_COUNT})")  # , exc_info=True
                            self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=2, cascade=True)
                            if number_of_timeouts >= MAX_TIMEOUT_COUNT:  # attemps to reconnect
                                logger.warning("too many times out, websocket listener terminated")  # ignore
                                self.ws_event.set()
                                self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=1, cascade=True)
                                self.inc(COCKPITDECKS_INTVAR.STOPS.value)

                        if number_of_timeouts >= MAX_TIMEOUT_COUNT and cnt % WARN_FREQ == 0:
                            logger.error(f"..X-Plane instance not found on local network.. ({datetime.now().strftime('%H:%M:%S')})")
                        cnt = cnt + 1
                except:
                    logger.error(f"..X-Plane instance not found on local network.. ({datetime.now().strftime('%H:%M:%S')})", exc_info=True)
                # If still no connection (above attempt failed)
                # we wait before trying again
                if not self.connected:
                    self.should_not_connect.wait(RECONNECT_TIMEOUT)
                    logger.debug("..no connection. trying to connect..")
            else:
                # Connection is OK, we wait before checking again
                self.should_not_connect.wait(RECONNECT_TIMEOUT)  # could be n * RECONNECT_TIMEOUT
                logger.debug("..monitoring connection..")
        logger.debug("..ended connection monitor")

    # ################################
    # Interface
    #
    def connect(self):
        """
        Starts connect loop.
        """
        if self.should_not_connect is None:
            self.should_not_connect = threading.Event()
            self.connect_thread = threading.Thread(target=self.connect_loop, name=f"{type(self).__name__}::Connection Monitor")
            self.connect_thread.start()
            logger.debug("connection monitor started")
        else:
            logger.debug("connection monitor started")

    def disconnect(self):
        """
        End connect loop and disconnect
        """
        if self.should_not_connect is not None:
            logger.debug("disconnecting..")
            self.cleanup()
            self.should_not_connect.set()
            wait = RECONNECT_TIMEOUT
            logger.debug(f"..asked to stop connection monitor.. (this may last {wait} secs.)")
            self.connect_thread.join(timeout=wait)
            if self.connect_thread.is_alive():
                logger.warning("..thread may hang..")
            self.should_not_connect = None
            logger.debug("..disconnected")
        else:
            if self.connected:
                self.cleanup()
                logger.debug("..connection monitor not running..disconnected")
            else:
                logger.debug("..not connected")

    def send(self, payload: dict) -> int:
        if self.connected:
            if payload is not None and len(payload) > 0:
                req_id = self.next_req()
                payload[REST_REQID] = req_id
                self._requests[req_id] = None
                self.ws.send(json.dumps(payload))
                # print(f"sent {payload}")
                return req_id
            else:
                logger.warning("no payload")
        logger.warning("not connected")
        return -1

    # Dataref operations
    #
    # It is not possible get the the value of a dataref just once
    # through web service.
    #
    # def get_dataref_value(self, path, on: bool = True) -> int:
    #     dref = self.get_dataref_info_by_name(path)
    #     if dref is not None:
    #         action = "dataref_subscribe_values" if on else "dataref_unsubscribe_values"
    #         return self.send({"type": action, "params": {REST_DATAREFS: [{"id": dref.ident}]}})
    #     logger.warning(f"dataref {path} not found in X-Plane datarefs database")
    #     return -1

    def split_dataref_path(self, path):
        name = path
        index = -1
        split = "[" in path and "]" in path
        if split:  # sim/some/values[4]
            name = path[: path.find("[")]
            index = int(path[path.find("[") + 1 : path.find("]")])  # 4
        dref = self.get_dataref_info_by_name(name)
        return split, dref, name, index

    def append_index(self, dref, i):
        if INDICES not in dref:
            dref[INDICES] = set()
        dref[INDICES].add(i)

    def remove_index(self, dref, i):
        if INDICES not in dref:
            logger.warning(f"{dref} has no index list")
            return
        dref[INDICES].remove(i)

    def set_dataref_value(self, path, value) -> int:
        if value is None:
            logger.warning(f"dataref {path} has no value to set")
            return -1
        split, dref, name, index = self.split_dataref_path(path)
        if dref is None:
            logger.warning(f"dataref {path} not found in X-Plane datarefs database")
            return -1
        payload = {
            REST_TYPE: "dataref_set_values",
            REST_PARAMS: {
                REST_DATAREFS: [{
                    REST_IDENT: dref[REST_IDENT],
                    REST_VALUE: value
                }]
            }
        }
        if split:
            payload[REST_PARAMS][REST_DATAREFS][0][REST_INDEX] = index
        return self.send(payload)

    def register_dataref_value_event(self, path: str, on: bool = True) -> int:
        split, dref, name, index = self.split_dataref_path(path)
        if dref is None:
            logger.warning(f"dataref {path} not found in X-Plane datarefs database")
            return -1
        payload = {
            REST_TYPE: "dataref_subscribe_values" if on else "dataref_unsubscribe_values",
            REST_PARAMS: {
                REST_DATAREFS: [{
                    REST_IDENT: dref[REST_IDENT],
                    REST_VALUE: value
                }]
            }
        }
        if split:
            payload[REST_PARAMS][REST_DATAREFS][0][REST_INDEX] = index
            if on:
                self.append_index(dref, index)
            else:
                self.remove_index(dref, index)
        return self.send(payload)

    def register_bulk_dataref_value_event(self, paths, on: bool = True) -> int:
        drefs = []
        for path in paths:
            split, dref, name, index = self.split_dataref_path(path)
            if dref is None:
                logger.warning(f"dataref {path} not found in X-Plane datarefs database")
                continue
            if split:
                drefs.append({REST_IDENT: dref[REST_IDENT], REST_INDEX: index})
                self.append_index(dref, index)
                if on:
                    self.append_index(dref, index)
                else:
                    self.remove_index(dref, index)
            else:
                drefs.append({REST_IDENT: dref[REST_IDENT]})

        if len(drefs) > 0:
            action = "dataref_subscribe_values" if on else "dataref_unsubscribe_values"
            return self.send({REST_TYPE: action, REST_PARAMS: {REST_DATAREFS: drefs}})
        logger.warning(f"no bulk datarefs to register")
        return -1

    # Command operations
    #
    def register_command_event(self, path: str, on: bool = True) -> int:
        cmd = self.get_command_info_by_name(path)
        if cmd is not None:
            action = "command_subscribe_is_active" if on else "command_unsubscribe_is_active"
            return self.send({REST_TYPE: action, REST_PARAMS: {REST_COMMANDS: [{REST_IDENT: cmd[REST_IDENT]}]}})
        logger.warning(f"command {path} not found in X-Plane commands database")
        return -1

    def execute_ws_command(self, path: str, duration: float = 0.0) -> int:
        cmd = self.get_command_info_by_name(path)
        if cmd is not None:
            return self.send({
                REST_TYPE: "command_set_is_active",
                REST_PARAMS: {
                    REST_COMMANDS: [
                        {REST_IDENT: cmd[REST_IDENT], REST_DURATION: duration, REST_ISACTIVE: True}
                    ]
                }
            })
        logger.warning(f"command {path} not found in X-Plane commands database")
        return -1

    def execute_ws_long_command(self, path: str, active: bool) -> int:
        cmd = self.get_command_info_by_name(path)
        if cmd is not None:
            return self.send({
                REST_TYPE: "command_set_is_active",
                REST_PARAMS: {
                    REST_COMMANDS: [
                        {REST_IDENT: cmd[REST_IDENT], REST_ISACTIVE: active}
                    ]
                }
            })
        logger.warning(f"command {path} not found in X-Plane commands database")
        return -1

    def execute_start_ws_long_command(self, path) -> int:
        return self.execute_ws_long_command(path=path, active=True)

    def execute_end_ws_long_command(self, path) -> int:
        return self.execute_ws_long_command(path=path, active=False)


# #############################################
# SIMULATOR
#
class XPlane(Simulator, SimulatorVariableListener, XPlaneWebSocket):
    """
    Get data from XPlane via network.
    Use a class to implement RAI Pattern for the UDP socket.
    """

    name = "X-Plane"

    # constants
    MCAST_GRP = "239.255.1.1"
    MCAST_PORT = 49707  # (MCAST_PORT was 49000 for XPlane10)
    BEACON_TIMEOUT = 3.0  # seconds
    TERMINATE_QUEUE = "quit"

    def __init__(self, cockpit, environ):
        self._inited = False
        # list of requested datarefs with index number
        self.datarefs = set()  # list of datarefs currently monitored
        self._max_monitored = 0

        self.ws_event = None  # thread to read X-Plane UDP port for datarefs
        self.ws_thread = None
        self._dref_cache = {}
        self.observables = []  # cannot create them here since XPlane does not exist yet...

        self.xp_home = environ.get(ENVIRON_KW.SIMULATOR_HOME.value)
        self.api_host = environ.get(ENVIRON_KW.API_HOST.value)
        self.api_port = environ.get(ENVIRON_KW.API_PORT.value)
        self.api_path = environ.get(ENVIRON_KW.API_PATH.value)
        self.api_version = environ.get(ENVIRON_KW.API_VERSION.value)

        Simulator.__init__(self, cockpit=cockpit, environ=environ)
        XPlaneWebSocket.__init__(self, host=self.api_host[0], port=self.api_host[1])
        SimulatorVariableListener.__init__(self, name=self.name)
        self.cockpit.set_logging_level(__name__)

        self.init()

    def init(self):
        if self._inited:
            return
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=0, cascade=True)
        self._inited = True

    def __del__(self):
        if not self._inited:
            return
        self.register_bulk_dataref_value_event(paths=self.datarefs, on=False)
        self.datarefs = set()
        self.disconnect()

    def get_version(self) -> list:
        return [f"{type(self).__name__} {__version__}"]

    # ################################
    # Factories
    #
    def instruction_factory(self, name: str, instruction_block: str | dict) -> XPlaneInstruction:
        # logger.debug(f"creating xplane instruction {name}")
        return XPlaneInstruction.new(name=name, simulator=self, instruction_block=instruction_block)

    def variable_factory(self, name: str, is_string: bool = False, creator: str = None) -> Dataref:
        # logger.debug(f"creating xplane dataref {name}")
        variable = Dataref(path=name, simulator= self, is_string=is_string)
        self.set_rounding(variable)
        self.set_frequency(variable)
        if creator is not None:
            variable._creator = creator
        return variable

    def replay_event_factory(self, name: str, value):
        logger.debug(f"creating replay event {name}")
        return DatarefEvent(sim=self, dataref=name, value=value, cascade=True, autorun=False)

    # ################################
    # Others
    #
    def runs_locally(self) -> bool:
        if self.connected:
            logger.debug(f"local ip {self.local_ip} vs beacon {self.host}")
        else:
            logger.debug(f"local ip {self.local_ip} but not connected to X-Plane")
        return False if not self.connected else self.local_ip == self.host

    #
    # Datarefs
    def get_variables(self) -> set:
        """Returns the list of datarefs for which the xplane simulator wants to be notified."""
        ret = set(PERMANENT_SIMULATOR_DATA)
        for obs in self.observables:
            ret = ret | obs.get_variables()
        return ret

    def simulator_variable_changed(self, data: SimulatorVariable):
        pass

    def create_local_observables(self):
        if len(self.observables) > 0:
            return
        self.observables = [WeatherStationObservable(simulator=self), DaytimeObservable(simulator=self)]

    def add_permanently_monitored_simulator_variables(self):
        """Add simulator variables coming from different sources (cockpit, simulator itself, etc.)
        that are always monitored (for all aircrafts)
        """
        self.create_local_observables()
        dtdrefs = self.get_permanently_monitored_simulator_variables()
        logger.info(f"monitoring {len(dtdrefs)} permanent simulator variables")
        self.add_simulator_variables_to_monitor(simulator_variables=dtdrefs, reason="permanent simulator variables")

    def datetime(self, zulu: bool = False, system: bool = False) -> datetime:
        """Returns the simulator date and time"""
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

    #
    # Instruction execution
    def execute_command(self, command: Command | None):
        if command is None:
            logger.warning("no command")
            return
        elif not command.is_valid():
            logger.warning(f"command '{command}' not sent (command placeholder, no command, do nothing)")
            return
        if not self.connected:
            logger.warning(f"no connection ({command})")
            return
        if command.path is not None:
            self.execute_ws_command(path=command.path)
            logger.log(SPAM_LEVEL, f"executed {command}")
        else:
            logger.warning("no command to execute")

    def ws_receiver(self):
        """Read and decode websocket messages and enqueue change events
        """
        def dref_round(local_path: str, local_value):
            local_r = self.get_rounding(simulator_variable_name=local_path)
            local_v = round(local_value, local_r) if local_r is not None and local_value is not None else local_value
            return 0.0 if local_v < 0.0 and local_v > -0.001 else local_v

        logger.debug("starting websocket listener..")
        total_reads = 0
        print_zulu = 0
        last_read_ts = datetime.now()
        total_read_time = 0.0
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=3, cascade=True)
        while self.ws_event is not None and not self.ws_event.is_set():
            try:
                message = self.ws.receive(timeout=RECEIVE_TIMEOUT)
                if message is None:
                    continue

                # Estimate response time
                self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=4, cascade=True)

                self.inc(COCKPITDECKS_INTVAR.UDP_READS.value)
                total_reads = total_reads + 1
                now = datetime.now()
                delta = now - last_read_ts
                self.set_internal_variable(
                    name=COCKPITDECKS_INTVAR.LAST_READ.value,
                    value=delta.microseconds,
                    cascade=False,
                )
                total_read_time = total_read_time + delta.microseconds / 1000000
                last_read_ts = now

                # Decode response
                data = {}
                resp_type = ""
                try:
                    data = json.loads(message)
                    resp_type = data[REST_TYPE]

                    if resp_type == REST_RESULT:

                        req_id = data.get(REST_REQID)
                        if req_id is not None:
                            self._requests[req_id] = data[REST_SUCCESS]
                        if not data[REST_SUCCESS]:
                            errmsg = REST_SUCCESS if data[REST_SUCCESS] else 'failed'
                            errmsg = errmsg + " " + data.get("error_message")
                            errmsg = errmsg + " (" + data.get("error_code") + ")"
                            logger.warning(f"req. {req_id}: {errmsg}")
                        else:
                            logger.debug(f"req. {req_id}: {REST_SUCCESS if data[REST_SUCCESS] else 'failed'}")

                    elif resp_type == "dataref_update_values":

                        if REST_DATA in data:
                            for didx, value in data[REST_DATA].items():
                                if didx in self._dref_cache and self._dref_cache[didx] == value:  # cached RAW value
                                    continue
                                self._dref_cache[didx] = value
                                dref = self.get_dataref_info_by_id(int(didx))
                                if dref is not None:
                                    d = dref[REST_NAME]
                                    if d == ZULU_TIME_SEC:
                                        now = datetime.now().astimezone(tz=timezone.utc)
                                        seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
                                        diff = value - seconds_since_midnight
                                        self.set_internal_variable(
                                            name=COCKPITDECKS_INTVAR.ZULU_DIFFERENCE.value,
                                            value=diff,
                                            cascade=(total_reads % 2 == 0),
                                        )
                                        print_zulu = print_zulu + 1
                                    if dref.get(REST_VALUE_TYPE) is not None and dref[REST_VALUE_TYPE] in ["int_array", "float_array"]:
                                        # we must check each individual value...
                                        if INDICES not in dref or len(value) != len(dref[INDICES]):
                                            logger.warning(f"dataref array {d} size mismatch ({len(value)}/{len(dref[INDICES])})")
                                        for v1, idx, cnt in zip(value, dref[INDICES], range(len(value))):
                                            d1 = f"{d}[{idx}]"
                                            v = dref_round(local_path=d1, local_value=v1)
                                            if d1 not in self._dref_cache or self._dref_cache[d1] != v:  # cached rounded value
                                                print("DREF ARRAY", cnt, d, idx, self._dref_cache.get(d1), " -> ", v)
                                                e = DatarefEvent(
                                                    sim=self,
                                                    dataref=d1,
                                                    value=v1,
                                                    cascade=d in self.simulator_variable_to_monitor.keys(),
                                                )
                                                self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)
                                                self._dref_cache[d1] = v
                                    else:
                                        v = value
                                        send_raw = True
                                        if dref.get(REST_VALUE_TYPE) is not None and dref[REST_VALUE_TYPE] == "data" and type(value) in [bytes, str]:  # float, double, int, int_array, float_array, data
                                            v = base64.b64decode(value).decode("ascii").replace("\u0000", "")
                                            send_raw = False
                                        elif type(v) in [int, float]:
                                            v = dref_round(local_path=d, local_value=value)
                                        if d not in self._dref_cache or self._dref_cache[d] != v:  # cached rounded value
                                            if d != ZULU_TIME_SEC or print_zulu % 120 == 0:
                                                print("DREF", d, self._dref_cache.get(d), " -> ", v)
                                            e = DatarefEvent(
                                                sim=self,
                                                dataref=d,
                                                value=value if send_raw else v,  # send raw value if possible
                                                cascade=d in self.simulator_variable_to_monitor.keys(),
                                            )
                                            self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)
                                            self._dref_cache[d] = v
                                else:
                                    logger.warning(f"dataref {didx} not found")
                        else:
                            logger.warning(f"no data: {data}")

                    elif resp_type == "command_update_is_active":

                        if REST_DATA in data:
                            for cidx, value in data[REST_DATA].items():
                                cref = self.get_command_info_by_id(int(cidx))
                                if cref is not None:
                                    c = cref[REST_NAME]
                                    v = value
                                    print("CMD", c, value)
                                    e = CommandEvent(sim=self, command=c, is_active=value, cascade=True)
                                    self.inc(COCKPITDECKS_INTVAR.COMMAND_ACTIVE_ENQUEUED.value)
                        else:
                            logger.warning(f"no data: {data}")

                    else:
                        logger.warning(f"invalid response type {resp_type}: {data}")
                except:
                    logger.warning(f"decode data {data} failed", exc_info=True)

            except ConnectionClosed:
                logger.warning("websocket connection closed")
                self.ws = None
                self.ws_event.set()

            except:
                logger.error(f"ws_receiver: other error", exc_info=True)

        self.ws_event = None
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=2, cascade=True)
        logger.info("..websocket listener terminated")

    # ################################
    # X-Plane Interface
    #
    def command_once(self, command: Command):
        self.execute_command(command)

    def command_begin(self, command: Command):
        if command.path is not None:
            self.execute_command(Command(path=command.path + "/begin", simulator=self, name="BeginCommand:" + command.path))
        else:
            logger.warning("no command")

    def command_end(self, command: Command):
        if command.path is not None:
            self.execute_command(Command(path=command.path + "/end", simulator=self, name="EndCommand:" + command.path))
        else:
            logger.warning("no command")

    def remove_local_datarefs(self, datarefs) -> list:
        return list(filter(lambda d: not Dataref.is_internal_variable(d), datarefs))

    def clean_datarefs_to_monitor(self):
        if not self.connected:
            return
        self.register_bulk_dataref_value_event(paths=self.datarefs, on=False)
        self.datarefs = set()
        super().clean_simulator_variable_to_monitor()
        self._strdref_cache = {}
        self._dref_cache = {}
        logger.debug("done")

    def add_simulator_variables_to_monitor(self, simulator_variables, reason: str | None = None):
        if not self.connected:
            logger.debug(f"would add {self.remove_local_datarefs(simulator_variables.keys())}")
            return
        # Add those to monitor
        super().add_simulator_variables_to_monitor(simulator_variables=simulator_variables)
        paths = set()
        for d in simulator_variables.values():
            if d.is_internal:
                logger.debug(f"local dataref {d.name} is not monitored")
                continue
            paths.add(d.name)
        self.register_bulk_dataref_value_event(paths=paths, on=True)
        self.datarefs = self.datarefs | paths
        self._max_monitored = max(self._max_monitored, len(self.datarefs))

        print(f"add_simulator_variable_to_monitor: added {paths}")
        if MONITOR_DATAREF_USAGE:
            logger.info(f">>>>> monitoring++{len(simulator_variables)}/{len(self.datarefs)}/{self._max_monitored} {reason if reason is not None else ''}")

    def remove_simulator_variables_to_monitor(self, simulator_variables: dict, reason: str | None = None):
        if not self.connected and len(self.simulator_variable_to_monitor) > 0:
            logger.debug(f"would remove {simulator_variables.keys()}/{self._max_monitored}")
            return
        # Add those to monitor
        paths = set()
        for d in simulator_variables.values():
            if d.is_internal:
                logger.debug(f"internal variable {d.name} is not monitored")
                continue
            if d.name in self.simulator_variable_to_monitor.keys():
                if self.simulator_variable_to_monitor[d.name] == 1:  # will be decreased by 1 in super().remove_simulator_variable_to_monitor()
                    paths.add(d.name)
                else:
                    logger.debug(f"{d.name} monitored {self.simulator_variable_to_monitor[d.name]} times")
            else:
                logger.debug(f"no need to remove {d.name}")

        self.register_bulk_dataref_value_event(paths=paths, on=False)
        self.datarefs = self.datarefs - paths
        logger.debug(f"removed {paths}")
        super().remove_simulator_variables_to_monitor(simulator_variables=simulator_variables)
        if MONITOR_DATAREF_USAGE:
            logger.info(f">>>>> monitoring--{len(simulator_variables)}/{len(self.datarefs)}/{self._max_monitored} {reason if reason is not None else ''}")

    def remove_all_datarefs(self):
        datarefs = [d for d in self.cockpit.variable_database.database.values() if type(d) is Dataref]
        if not self.connected and len(datarefs) > 0:
            logger.debug(f"would remove {', '.join([d.name for d in datarefs])}")
            return
        # This is not necessary:
        # self.remove_simulator_variable_to_monitor(datarefs)
        super().remove_all_simulator_variable()

    def add_all_datarefs_to_monitor(self):
        if not self.connected:
            return
        # Add permanently monitored drefs
        self.add_permanently_monitored_simulator_variables()
        # Add those to monitor
        paths = set()
        for path in self.simulator_variable_to_monitor.keys():
            d = self.cockpit.variable_database.get(path)
            if d is not None:
                paths.add(d.name)
            else:
                logger.warning(f"no dataref {path}")
        self.register_bulk_dataref_value_event(paths=paths, on=True)
        self.datarefs = self.datarefs | paths
        self._max_monitored = max(self._max_monitored, len(self.datarefs))
        logger.log(SPAM_LEVEL, f"added {paths}")

    def cleanup(self):
        """
        Called when before disconnecting.
        Just before disconnecting, we try to cancel dataref UDP reporting in X-Plane
        """
        self.clean_datarefs_to_monitor()
        super().cleanup()

    def start(self):
        if not self.connected:
            logger.warning("not connected. cannot not start.")
            return

        if self.ws_event is None:  # Thread for X-Plane datarefs
            self.ws_event = threading.Event()
            self.ws_thread = threading.Thread(target=self.ws_receiver, name="XPlane::WebSocket Listener")
            self.ws_thread.start()
            logger.info("websocket listener started")
        else:
            logger.info("websocket listener already running.")

        # When restarted after network failure, should clean all datarefs
        # then reload datarefs from current page of each deck
        self.clean_datarefs_to_monitor()
        self.add_all_datarefs_to_monitor()
        logger.info("reloading pages")
        self.cockpit.reload_pages()  # to take into account updated values

    def stop(self):
        if self.ws_event is not None:
            self.cleanup()
            self.ws_event.set()
            logger.debug("stopping websocket listener..")
            wait = RECEIVE_TIMEOUT
            logger.debug(f"..asked to stop websocket listener (this may last {wait} secs. for timeout)..")
            self.ws_thread.join(wait)
            if self.ws_thread.is_alive():
                logger.warning("..thread may hang in ws.receive()..")
            self.ws_event = None
            logger.debug("..websocket listener stopped")
        else:
            logger.debug("websocket listener not running")

    # ################################
    # Cockpit interface
    #
    def terminate(self):
        logger.debug(f"currently {'not ' if self.ws_event is None else ''}running. terminating..")
        logger.info("terminating..")
        logger.info("..requesting to stop websocket emission..")
        self.clean_datarefs_to_monitor()  # stop monitoring all datarefs
        logger.info("..stopping websocket listener..")
        self.stop()
        logger.info("..deleting datarefs..")
        self.remove_all_datarefs()
        logger.info("..disconnecting from simulator..")
        self.disconnect()
        logger.info("..terminated")

#
