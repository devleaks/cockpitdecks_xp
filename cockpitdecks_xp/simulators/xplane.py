# Class for interface with X-Plane using REST/WebSocket API.
# See https://developer.x-plane.com/article/x-plane-web-api
#
from __future__ import annotations

import os
import socket
import threading
import logging
import json
import base64
from abc import ABC, abstractmethod
from enum import Enum
from datetime import datetime, timedelta, timezone

import requests

from simple_websocket import Client, ConnectionClosed
from packaging.version import Version

from cockpitdecks_xp import __version__
from cockpitdecks import CONFIG_KW, ENVIRON_KW, SPAM_LEVEL, DEPRECATION_LEVEL, MONITOR_RESOURCE_USAGE, RESOURCES_FOLDER, OBSERVABLES_FILE, yaml
from cockpitdecks.strvar import Formula
from cockpitdecks.instruction import MacroInstruction

from cockpitdecks.simulator import Simulator, SimulatorEvent, SimulatorInstruction
from cockpitdecks.simulator import SimulatorVariable, SimulatorVariableListener
from cockpitdecks.resources.intvariables import COCKPITDECKS_INTVAR
from cockpitdecks.observable import Observables

from ..resources.stationobs import WeatherStationObservable
from ..resources.daytimeobs import DaytimeObservable
from ..resources.cmdlsnr import MapCommandObservable

from .beacon import XPlaneBeacon, BEACON_DATA_KW

logger = logging.getLogger(__name__)
logger.setLevel(SPAM_LEVEL)  # To see which dataref are requested
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
RECONNECT_TIMEOUT = 10  # seconds, times between attempts to reconnect to X-Plane when not connected
RECEIVE_TIMEOUT = 5  # seconds, assumes no awser if no message recevied withing that timeout

XP_MIN_VERSION = 121400
XP_MIN_VERSION_STR = "12.1.4"
XP_MAX_VERSION = 121499
XP_MAX_VERSION_STR = "12.1.4"

# /api/capabiltiies introduced in /api/v2. Here is a default one for v1.
DEFAULT_CAPABILITIES = {"api": {"versions": ["v1"]}, "x-plane": {"version": "12.1.1"}}
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
# (let's say time since plane was loaded, reloaded, or changed)
USEFUL_DATAREFS = [RUNNING_TIME, "sim/time/total_running_time_sec"]  # monitored to determine of cached data is valid  # Total time the sim has been up

PERMANENT_SIMULATOR_VARIABLES = set(USEFUL_DATAREFS)  # set(DATETIME_DATAREFS + REPLAY_DATAREFS + USEFUL_DATAREFS)
PERMANENT_SIMULATOR_EVENTS = {}  #


# #############################################
# REST OBJECTS
#
INDICES = "indices"
UPDATES = "updates"


# REST API model keywords
class REST_KW(Enum):
    COMMANDS = "commands"
    DATA = "data"
    DATAREFS = "datarefs"
    DESCRIPTION = "description"
    DURATION = "duration"
    IDENT = "id"
    INDEX = "index"
    ISACTIVE = "is_active"
    ISWRITABLE = "is_writable"
    NAME = "name"
    PARAMS = "params"
    REQID = "req_id"
    RESULT = "result"
    SUCCESS = "success"
    TYPE = "type"
    VALUE = "value"
    VALUE_TYPE = "value_type"


# value_type: float, double, int, int_array, float_array, data
class DATAREF_DATATYPE(Enum):
    INTEGER = "int"
    FLOAT = "float"
    DOUBLE = "double"
    INTARRAY = "int_array"
    FLOATARRAY = "float_array"
    DATA = "data"


# WEB API RETURN CODES
class REST_RESPONSE(Enum):
    RESULT = "result"
    COMMAND_ACTIVE = "command_update_is_active"
    DATAREF_UPDATE = "dataref_update_values"


# class obj(object):
#     def __init__(self, d):
#         for k, v in d.items():
#             if isinstance(k, (list, tuple)):
#                 setattr(self, k, [obj(x) if isinstance(x, dict) else x for x in v])
#             else:
#                 setattr(self, k, obj(v) if isinstance(v, dict) else v)


# Dataref object
class XPRESTObject:
    """Small structure to host a dataref or a command meta data: Id, value_type, etc."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.config = {}
        self.api = None
        self._valid = False
        self._cache = None
        self._last_updated = 0

    @property
    def ident(self) -> int | None:
        if not self.valid:
            return None
        return self.config.get(REST_KW.IDENT.value)

    @property
    def value_type(self) -> int | None:
        if not self.valid:
            return None
        return self.config.get(REST_KW.VALUE_TYPE.value)

    @property
    def valid(self) -> bool:
        if not self._valid:
            return False
        if self._cache is None:
            return False
        if self._last_updated != self._cache._last_updated:
            logger.warning(f"{self.path} description expired")
            self.init(self._cache)
        return self._valid

    def init(self, cache):
        if cache is None:
            return
        self.config = cache.get(self.path)
        self.api = None
        self._valid = False
        if self.config is None:
            logger.error(f"{type(self)} {self.path} not found")
        else:
            self.api = cache.api
            self._valid = True
            self._cache = cache
            self._last_updated = cache._last_updated


# A value in X-Plane Simulator
# value_type: float, double, int, int_array, float_array, data
#
class Dataref(SimulatorVariable, XPRESTObject):
    """
    A Dataref is an internal value of the simulation software made accessible to outside modules,
    plugins, or other software in general.
    """

    def __init__(self, path: str, simulator: XPlane, is_string: bool = False):
        # Data
        SimulatorVariable.__init__(self, name=path, simulator=simulator, data_type="string" if is_string else "float")
        XPRESTObject.__init__(self, path=path)

        self.index = None  # sign is it not an array
        if "[" in path:
            self.path = self.name[: self.name.find("[")]  # sim/some/values
            self.index = int(self.name[self.name.find("[") + 1 : self.name.find("]")])  # 4

    def __str__(self) -> str:
        if self.index is not None:
            return f"{self.path}[{self.index}]={self.value}"
        else:
            return f"{self.path}={self.value}"

    @property
    def is_writable(self) -> bool | None:
        if not self.valid:
            self.init(cache=self.simulator.all_datarefs)
            if not self.valid:
                logger.error(f"dataref {self.path} not valid")
                return False
        return self.config.get(REST_KW.ISWRITABLE.value)

    @property
    def use_rest(self):
        return USE_REST and (hasattr(self.simulator, "same_host") and not self.simulator.same_host())

    @property
    def rest_value(self):
        if not self.valid:
            self.init(cache=self.simulator.all_datarefs)
            if not self.valid:
                logger.error(f"dataref {self.path} not valid")
                return False
        url = f"{self.simulator.api_url}/datarefs/{self.ident}/value"
        response = requests.get(url)
        if response.status_code == 200:
            respjson = response.json()
            webapi_logger.info(f"GET {self.path}: {url} = {respjson}")
            if REST_KW.DATA.value in respjson and type(respjson[REST_KW.DATA.value]) in [bytes, str]:
                return base64.b64decode(respjson[REST_KW.DATA.value]).decode("ascii").replace("\u0000", "")
            return respjson[REST_KW.DATA.value]
        webapi_logger.info(f"ERROR {self.path}: {response} {response.reason} {response.text}")
        logger.error(f"rest_value: {response} {response.reason} {response.text}")
        return None

    def rest_write(self) -> bool:
        if not self.valid:
            self.init(cache=self.simulator.all_datarefs)
            if not self.valid:
                logger.error(f"dataref {self.path} not valid")
                return False
        if not self.is_writable:
            logger.warning(f"dataref {self.path} is not writable")
            return False
        value = self.value
        if self.value_type == DATAREF_DATATYPE.DATA.value:
            # Encode string
            value = str(value).encode("ascii")
            value = base64.b64encode(value).decode("ascii")
        payload = {REST_KW.DATA.value: value}
        url = f"{self.simulator.api_url}/datarefs/{self.ident}/value"
        if self.index is not None and self.value_type in [DATAREF_DATATYPE.INTARRAY.value, DATAREF_DATATYPE.FLOATARRAY.value]:
            # Update just one element of the array
            url = url + f"?index={self.index}"
        webapi_logger.info(f"PATCH {self.path}: {url}, {payload}")
        response = requests.patch(url, json=payload)
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"result: {data}")
            return True
        webapi_logger.info(f"ERROR {self.path}: {response} {response.reason} {response.text}")
        logger.error(f"write: {response} {response.reason} {response.text}")
        return False

    def ws_write(self) -> int:
        return self.simulator.set_dataref_value(self.name, self.value)

    def _write(self) -> bool:
        return self.rest_write() if self.use_rest else (self.ws_write() != -1)

    def save(self) -> bool:
        if not self.valid:
            self.init(cache=self.simulator.all_datarefs)
            if not self.valid:
                logger.error(f"dataref {self.path} not valid")
                return False
        return self._write()


# Events from simulator
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
# The following command keywords are not executed, ignored with a warning
NOT_A_COMMAND = [
    "none",
    "noop",
    "nooperation",
    "nocommand",
    "donothing",
]  # all forced to lower cases, -/:_ removed


# An instruction in X-Plane Simulator
#
class XPlaneInstruction(SimulatorInstruction, XPRESTObject):
    """An Instruction sent to the XPlane Simulator to execute some action.

    This is more an abstract base class, with a new() factory to handle instruction block.
    """

    def __init__(self, name: str, simulator: XPlane, delay: float = 0.0, condition: str | None = None, button: "Button" = None) -> None:
        SimulatorInstruction.__init__(self, name=name, simulator=simulator, delay=delay, condition=condition)
        XPRESTObject.__init__(self, path=name)

    @property
    def description(self) -> bool | None:
        if not self.valid:
            return None
        return self.config.get(REST_KW.DESCRIPTION.value)

    @property
    def use_rest(self):
        return USE_REST and (hasattr(self.simulator, "same_host") and not self.simulator.same_host())

    @property
    def is_no_operation(self) -> bool:
        return self.path is not None and self.path.lower().replace("-", "") in NOT_A_COMMAND

    @classmethod
    def new(cls, name: str, simulator: XPlane, instruction_block: dict) -> XPlaneInstruction | None:
        INSTRUCTIONS = [CONFIG_KW.BEGIN_END.value, CONFIG_KW.SET_SIM_VARIABLE.value, CONFIG_KW.COMMAND.value, CONFIG_KW.VIEW.value]

        def try_keyword(keyw) -> XPlaneInstruction | None:
            command_block = instruction_block.get(keyw)

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
                match keyw:

                    case CONFIG_KW.BEGIN_END.value:
                        return BeginEndCommand(name=name, simulator=simulator, path=command_block)

                    case CONFIG_KW.SET_SIM_VARIABLE.value:
                        return SetDataref(
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

                    case CONFIG_KW.VIEW.value:
                        logger.log(DEPRECATION_LEVEL, "«view» command no longer available, use regular command instead")
                        return Command(
                            name=name,
                            simulator=simulator,
                            path=command_block,
                        )

                    case CONFIG_KW.LONG_PRESS.value:
                        logger.log(DEPRECATION_LEVEL, "long press commands no longer available, use regular command instead")
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

        if len(instruction_block) == 0:
            logger.debug(f"{name}: instruction block is empty")
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

    def rest_execute(self) -> bool:  # ABC
        return False

    def ws_execute(self) -> int:  # ABC
        return -1

    def _execute(self):
        if self.use_rest:
            self.rest_execute()
        else:
            self.ws_execute()


# Instructions to simulator
#
class Command(XPlaneInstruction):
    """
    X-Plane simple Command, executed by CommandOnce API.
    """

    def __init__(self, simulator: XPlane, path: str, name: str | None = None, delay: float = 0.0, condition: str | None = None):
        XPlaneInstruction.__init__(self, name=name if name is not None else path, simulator=simulator, delay=delay, condition=condition)
        self.path = path  # some/command

    def __str__(self) -> str:
        return f"{self.name} ({self.path})"

    def is_valid(self) -> bool:
        return not self.is_no_operation

    def rest_execute(self) -> bool:
        if not self.is_valid():
            logger.error(f"command {self.path} is not valid")
            return False
        if not self.valid:
            self.init(cache=self.simulator.all_commands)
            if not self.valid:
                logger.error(f"command {self.path} is not valid")
                return False
        payload = {REST_KW.IDENT.value: self.ident, REST_KW.DURATION.value: 0.0}
        url = f"{self.simulator.api_url}/command/{self.ident}/activate"
        response = requests.post(url, json=payload)
        webapi_logger.info(f"POST {url} {payload} {response}")
        data = response.json()
        if response.status_code == 200:
            logger.debug(f"result: {data}")
            return True
        logger.error(f"execute: {response}, {data}")
        return False

    def ws_execute(self) -> int:
        return self.simulator.set_command_is_active_with_duration(path=self.path)


class BeginEndCommand(Command):
    """
    X-Plane long command, executed between CommandBegin/CommandEnd API.
    """

    DURATION = 5

    def __init__(self, simulator: XPlane, path: str, name: str | None = None, delay: float = 0.0, condition: str | None = None):
        Command.__init__(self, simulator=simulator, path=path, name=name, delay=0.0, condition=condition)  # force no delay for commandBegin/End
        self.is_on = False

    def rest_execute(self) -> bool:
        if not self.valid:
            self.init(cache=self.simulator.all_commands)
            if not self.valid:
                logger.error(f"command {self.path} is not valid")
                return False
        if not self.is_valid():
            logger.error(f"command {self.path} is not valid")
            return False
        payload = {REST_KW.IDENT.value: self.ident, REST_KW.DURATION.value: self.DURATION}
        url = f"{self.simulator.api_url}/command/{self.ident}/activate"
        response = requests.post(url, json=payload)
        webapi_logger.info(f"POST {url} {payload} {response}")
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
        return self.simulator.set_command_is_active_without_duration(path=self.path, active=self.is_on)


class SetDataref(XPlaneInstruction):
    """
    Instruction to update the value of a dataref in X-Plane simulator.
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
            return self.formula.value
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def rest_execute(self) -> bool:
        if not self.valid:
            self.init(cache=self.simulator.all_datarefs)
            if not self.valid:
                logger.error(f"dataref {self.path} is not valid")
                return False
        value = self.value
        if self.value_type == "data":
            value = str(value).encode("ascii")
            value = base64.b64encode(value).decode("ascii")
        payload = {REST_KW.DATA.value: self.value}
        url = f"{self.simulator.api_url}/datarefs/{self.ident}/value"
        response = requests.patch(url, json=payload)
        webapi_logger.info(f"PATCH {url} {payload} {response}")
        if response.status_code == 200:
            return True
        if response.status_code == 403:
            logger.warning(f"{self.name}: dataref not writable")
            return False
        logger.error(f"execute: {response}")
        return False

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
        if just_do_it:
            logger.info(f"event {self.name} occured in simulator with active={self.is_active}")
        else:
            self.enqueue()
            logger.debug("enqueued")
        return True


# #############################################
# REST API
#
class Cache:
    """Accessory structure to host datarref and command cache
    of current X-Plane instance.
    Must be "refreshed" each time a new connection is created.
    Must be refreshed each time a new aircraft is loaded (for new datarefs, commands, etc.)
    Reload_cache() is provided in XPlaneREST.

    There is no faster structure than a python dict() for (name,value) pair storage.
    """

    def __init__(self, api: XPlaneREST) -> None:
        self.api = api
        self._data = dict()
        self._ids = dict()
        self._valid = set()
        self._last_updated = 0

    def load(self, path):
        url = self.api.api_url + path
        response = requests.get(url)
        if response.status_code == 200:  # We have version 12.1.4 or above
            raw = response.json()
            data = raw[REST_KW.DATA.value]
            self._data = {c[REST_KW.NAME.value]: c for c in data}
            self._ids = {c[REST_KW.IDENT.value]: c for c in data}
            self._valid = set()
            logger.debug(f"{path[1:]} cached ({len(self._data)} entries)")
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
            self._valid.add(r[REST_KW.NAME.value])
            return r
        return None

    def is_valid(self, name):
        return name in self._valid

    def save(self, filename):
        with open(filename, "w") as fp:
            json.dump(self._data, fp)

    def equiv(self, ident) -> str | None:
        r = self._ids.get(ident)
        if r is not None:
            return f"{ident}({r[REST_KW.NAME.value]})"
        return None


class XPlaneREST:
    """Utility routines specific to REST API.
       Used by variables and instructions to execute their tasks.

    See https://developer.x-plane.com/article/x-plane-web-api/#REST_API.
    """

    def __init__(self, host: str, port: int, api: str, api_version: str) -> None:
        self.host = host
        self.port = port
        self._api_root_path = api
        if not self._api_root_path.startswith("/"):
            self._api_root_path = "/" + api
        self._api_version = api_version  # /v1, /v2, to be appended to URL
        if not self._api_version.startswith("/"):
            self._api_version = "/" + self._api_version
        self._first_try = True

        self.version = api_version  # v1, v2, etc.
        if self.version.startswith("/"):
            self.version = self.version[1:]

        self._capabilities = {}
        self._beacon = XPlaneBeacon()
        self._beacon.set_callback(self.beacon_callback)
        self._running_time = Dataref(path=RUNNING_TIME, simulator=self)  # cheating, side effect, works for rest api

        self.all_datarefs: Cache | None = None
        self.all_commands: Cache | None = None
        self._last_updated = 0

    @property
    # See https://stackoverflow.com/questions/7019643/overriding-properties-in-python
    # to overwrite @property definition
    def api_url(self) -> str:
        """URL for the REST API."""
        return f"http://{self.host}:{self.port}{self._api_root_path}{self._api_version}"

    @property
    def ws_url(self) -> str:
        """URL for the WebSocket API"""
        url = self.api_url
        if url is not None:
            return url.replace("http:", "ws:")
        return None

    @property
    def api_is_available(self) -> bool:
        """Important call that checks whether API is reachable
        API may not be reachable if:
         - X-Plane version before 12.1.4,
         - X-Plane is not running
        """
        CHECK_API_URL = f"http://{self.host}:{self.port}/api/v1/datarefs/count"
        response = None
        if self._first_try:
            logger.info(f"trying to connect to {CHECK_API_URL}..")
            self._first_try = False
        try:
            # Relies on the fact that first version is always provided.
            # Later verion offer alternative ot detect API
            response = requests.get(CHECK_API_URL)
            if response.status_code == 200:
                return True
        except requests.exceptions.ConnectionError:
            logger.warning(f"api unreachable, may be X-Plane is not running")
        except:
            logger.error(f"api unreachable, may be X-Plane is not running", exc_info=True)
        return False

    def beacon_callback(self, connected: bool):
        if connected:
            logger.info("X-Plane beacon connected")
            if self._beacon.connected:
                same_host = self._beacon.same_host()
                if same_host:
                    self.host = "127.0.0.1"
                    self.port = 8086
                else:
                    self.host = self._beacon.beacon_data[BEACON_DATA_KW.IP.value]
                    self.port = 8080
                xp_version = self._beacon.beacon_data.get(BEACON_DATA_KW.XPVERSION.value)
                if xp_version is not None:
                    use_rest = ", use REST" if USE_REST and not same_host else ""
                    if self._beacon.beacon_data[BEACON_DATA_KW.XPVERSION.value] >= 121400:
                        self._api_version = "/v2"
                        self._first_try = True
                        logger.info(f"XPlane API at {self.api_url} from UDP beacon data{use_rest}")
                    elif self._beacon.beacon_data[BEACON_DATA_KW.XPVERSION.value] >= 121100:
                        self._api_version = "/v1"
                        self._first_try = True
                        logger.info(f"XPlane API at {self.api_url} from UDP beacon data{use_rest}")
                    else:
                        logger.warning(f"could not set API version from {xp_version} ({self._beacon.beacon_data})")
                else:
                    logger.warning(f"could not get X-Plane version from {self._beacon.beacon_data}")
            else:
                logger.info("XPlane UDP beacon is not connected")
        else:
            logger.warning("X-Plane beacon disconnected")

    def capabilities(self) -> dict:
        # Guess capabilties and caches it
        if len(self._capabilities) > 0:
            return self._capabilities
        try:
            CAPABILITIES_API_URL = f"http://{self.host}:{self.port}/api/capabilities"  # independent from version
            response = requests.get(CAPABILITIES_API_URL)
            if response.status_code == 200:  # We have version 12.1.4 or above
                self._capabilities = response.json()
                logger.debug(f"capabilities: {self._capabilities}")
                return self._capabilities
            logger.info(f"capabilities at {self.api_url + '/capabilities'}: response={response.status_code}")
            response = requests.get(self.api_url + "/v1/datarefs/count")
            if response.status_code == 200:  # OK, /api/v1 exists, we use it, we have version 12.1.1 or above
                self._capabilities = DEFAULT_CAPABILITIES
                logger.debug(f"capabilities: {self._capabilities}")
                return self._capabilities
            logger.error(f"capabilities at {self.api_url + '/datarefs/count'}: response={response.status_code}")
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
        capabilities = self.capabilities()
        api_details = capabilities.get("api")
        if api_details is not None:
            api_versions = api_details.get("versions")
            if api is None:
                if api_versions is None:
                    logger.error("cannot determine api, api not set")
                    return
                api = sorted(api_versions)[-1]  # takes the latest one, hoping it is the latest in time...
                latest = ""
                try:
                    api = f"v{max([int(v.replace('v', '')) for v in api_versions])}"
                    latest = " latest"
                except:
                    pass
                logger.info(f"selected{latest} api {api} ({sorted(api_versions)})")
            if api in api_versions:
                self.version = api
                self._api_version = f"/{api}"
                logger.info(f"set api {api}, xp {self.xp_version}")
            else:
                logger.warning(f"no api {api} in {api_versions}")
            return
        logger.warning(f"could not check api {api} in {capabilities}")

    def reload_caches(self):
        MAX_TIME = 10
        if self._last_updated != 0:
            currtime = self._running_time.rest_value
            if currtime is not None:
                difftime = self._running_time.rest_value - self._last_updated
                if difftime < 10:
                    logger.info(f"dataref cache not updated, updated {round(difftime, 1)} secs. ago")
                    return
            else:
                logger.warning(f"no value for sim/time/total_running_time_sec")
        self.all_datarefs = Cache(self)
        self.all_datarefs.load("/datarefs")
        self.all_datarefs.save("webapi-datarefs.json")
        self.all_commands = Cache(self)
        if self.version == "v2":  # >
            self.all_commands.load("/commands")
            self.all_commands.save("webapi-commands.json")
        self._last_updated = self._running_time.rest_value
        logger.info(
            f"dataref cache ({len(self.all_datarefs._data)}) and command cache ({len(self.all_commands._data)}) reloaded ({round(self._last_updated, 1)})"
        )

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
class XPlaneWebSocket(XPlaneREST, ABC):
    """Utility routines specific to WebSocket API

    See https://developer.x-plane.com/article/x-plane-web-api/#Websockets_API.
    """

    MAX_WARNING = 5  # number of times it reports it cannot connect

    def __init__(self, host: str, port: int, api: str, api_version: str):
        # Open a UDP Socket to receive on Port 49000
        XPlaneREST.__init__(self, host=host, port=port, api=api, api_version=api_version)
        hostname = socket.gethostname()
        self.local_ip = socket.gethostbyname(hostname)

        self.ws = None  # None = no connection
        self.req_number = 0
        self._requests = {}

        self.should_not_connect = None  # threading.Event()
        self.connect_thread = None  # threading.Thread()
        self._already_warned = 0
        self._stats = {}

    @property
    def next_req(self) -> int:
        """Provides request number for WebSocket requests"""
        self.req_number = self.req_number + 1
        return self.req_number

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

    # ################################
    # Connection to web socket
    #
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

    def connect_websocket(self):
        if self.ws is None:
            try:
                if self.api_is_available:
                    self.set_api()  # attempt to get latest one
                    url = self.ws_url
                    if url is not None:
                        self.ws = Client.connect(url)
                        self.reload_caches()
                        logger.info(f"websocket opened at {url}")
                    else:
                        logger.warning(f"web socket url is none {url}")
            except:
                logger.error("cannot connect", exc_info=True)
        else:
            logger.warning("already connected")

    def disconnect_websocket(self):
        if self.ws is not None:
            self.ws.close()
            self.ws = None
            logger.info("websocket closed")
        else:
            logger.warning("already disconnected")

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
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=1, cascade=True)
        while self.should_not_connect is not None and not self.should_not_connect.is_set():
            if not self.connected:
                try:
                    logger.info("not connected, trying..")
                    self.connect_websocket()
                    if self.connected:
                        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=2, cascade=True)
                        self._already_warned = 0
                        number_of_timeouts = 0
                        logger.info(f"capabilities: {self.capabilities()}")
                        if self.xp_version is not None:
                            curr = Version(self.xp_version)
                            xpmin = Version(XP_MIN_VERSION_STR)
                            xpmax = Version(XP_MAX_VERSION_STR)
                            if curr < xpmin:
                                logger.warning(f"X-Plane version {curr} detected, minimal version is {xpmin}")
                                logger.warning("Some features in Cockpitdecks may not work properly")
                            elif curr > xpmax:
                                logger.warning(f"X-Plane version {curr} detected, maximal version is {xpmax}")
                                logger.warning("Some features in Cockpitdecks may not work properly")
                            else:
                                logger.info(f"X-Plane version meets current version criteria ({xpmin}<= {curr} <={xpmax})")
                        logger.debug("..connected, starting websocket listener..")
                        self.start()
                        # self.inc(COCKPITDECKS_INTVAR.STARTS.value)
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
                                # self.inc(COCKPITDECKS_INTVAR.STOPS.value)

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
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=0, cascade=True)
        logger.debug("..ended connection monitor")

    # ################################
    # Interface
    #
    def connect(self, reload_cache: bool = False):
        """
        Starts connect loop.
        """
        self._beacon.connect()
        if self.should_not_connect is None:
            self.should_not_connect = threading.Event()
            self.connect_thread = threading.Thread(target=self.connect_loop, name=f"{type(self).__name__}::Connection Monitor")
            self.connect_thread.start()
            logger.debug("connection monitor started")
        else:
            if reload_cache:
                self.reload_caches()
            logger.debug("connection monitor connected")

    def disconnect(self):
        """
        End connect loop and disconnect
        """
        if self.should_not_connect is not None:
            logger.debug("disconnecting..")
            self._beacon.disconnect()
            self.disconnect_websocket()
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
                self.disconnect_websocket()
                logger.debug("..connection monitor not running..disconnected")
            else:
                logger.debug("..not connected")

    # ################################
    # I/O
    #
    # Generic payload "send" function, unique
    def send(self, payload: dict, mapping: dict = {}) -> int:
        # Mapping is correspondance dataref_index=dataref_name
        if self.connected:
            if payload is not None and len(payload) > 0:
                req_id = self.next_req
                payload[REST_KW.REQID.value] = req_id
                self._requests[req_id] = None
                self.ws.send(json.dumps(payload))
                webapi_logger.info(f">>SENT {payload}")
                if len(mapping) > 0:
                    maps = [f"{k}={v}" for k, v in mapping.items()]
                    webapi_logger.info(f">> MAP {', '.join(maps)}")
                return req_id
            else:
                logger.warning("no payload")
        logger.warning("not connected")
        return -1

    # Dataref operations
    #
    # Note: It is not possible get the the value of a dataref just once
    # through web service. No get_dataref_value().
    #
    def split_dataref_path(self, path):
        name = path
        index = -1
        split = "[" in path and "]" in path
        if split:  # sim/some/values[4]
            name = path[: path.find("[")]
            index = int(path[path.find("[") + 1 : path.find("]")])  # 4
        dref = self.get_dataref_info_by_name(name)
        return split, dref, name, index

    def inc_dref(self, dref):
        if UPDATES not in dref:
            dref[UPDATES] = 0
        dref[UPDATES] = dref[UPDATES] + 1

    def append_index(self, dref, i):
        # see https://stackoverflow.com/questions/13694034/is-a-python-list-guaranteed-to-have-its-elements-stay-in-the-order-they-are-inse
        if INDICES not in dref:
            dref[INDICES] = list()  # set() do not preserve order of insertion
        if i not in dref[INDICES]:
            dref[INDICES].append(i)
        # webapi_logger.info(f"REG {dref[REST_KW.NAME.value]}: {i} ({dref[INDICES]})")

    def remove_index(self, dref, i):
        if INDICES not in dref:
            logger.warning(f"{dref} has no index list")
            return
        dref[INDICES].remove(i)
        # webapi_logger.info(f"DEREG {dref[REST_KW.NAME.value]}: {i} ({dref[INDICES]})")

    def set_dataref_value(self, path, value) -> int:
        if value is None:
            logger.warning(f"dataref {path} has no value to set")
            return -1
        split, dref, name, index = self.split_dataref_path(path)
        if dref is None:
            logger.warning(f"dataref {path} not found in X-Plane datarefs database")
            return -1
        payload = {
            REST_KW.TYPE.value: "dataref_set_values",
            REST_KW.PARAMS.value: {REST_KW.DATAREFS.value: [{REST_KW.IDENT.value: dref[REST_KW.IDENT.value], REST_KW.VALUE.value: value}]},
        }
        mapping = {dref[REST_KW.IDENT.value]: dref[REST_KW.NAME.value]}
        if split:
            payload[REST_KW.PARAMS.value][REST_KW.DATAREFS.value][0][REST_KW.INDEX.value] = index
        return self.send(payload, mapping)

    def register_dataref_value_event(self, path: str, on: bool = True) -> int:
        split, dref, name, index = self.split_dataref_path(path)
        if dref is None:
            logger.warning(f"dataref {path} not found in X-Plane datarefs database")
            return -1
        payload = {
            REST_KW.TYPE.value: "dataref_subscribe_values" if on else "dataref_unsubscribe_values",
            REST_KW.PARAMS.value: {REST_KW.DATAREFS.value: [{REST_KW.IDENT.value: dref[REST_KW.IDENT.value], REST_KW.VALUE.value: value}]},
        }
        mapping = {dref[REST_KW.IDENT.value]: dref[REST_KW.NAME.value]}
        if split:
            payload[REST_KW.PARAMS.value][REST_KW.DATAREFS.value][0][REST_KW.INDEX.value] = index
            if on:
                self.append_index(dref, index)
            else:
                self.remove_index(dref, index)
        return self.send(payload, mapping)

    def register_bulk_dataref_value_event(self, paths, on: bool = True) -> int:
        drefs = []
        mapping = {}
        for path in paths:
            split, dref, name, index = self.split_dataref_path(path)
            if dref is None:
                logger.warning(f"dataref {path} not found in X-Plane datarefs database")
                continue
            mapping[dref[REST_KW.IDENT.value]] = dref[REST_KW.NAME.value]
            if split:
                drefs.append({REST_KW.IDENT.value: dref[REST_KW.IDENT.value], REST_KW.INDEX.value: index})
                if on:
                    self.append_index(dref, index)
                else:
                    self.remove_index(dref, index)
            else:
                drefs.append({REST_KW.IDENT.value: dref[REST_KW.IDENT.value]})

        if len(drefs) > 0:
            action = "dataref_subscribe_values" if on else "dataref_unsubscribe_values"
            return self.send({REST_KW.TYPE.value: action, REST_KW.PARAMS.value: {REST_KW.DATAREFS.value: drefs}}, mapping)
        action = "register" if on else "unregister"
        logger.warning(f"no bulk datarefs to {action}")
        return -1

    # Command operations
    #
    def register_command_is_active_event(self, path: str, on: bool = True) -> int:
        cmdref = self.get_command_info_by_name(path)
        if cmdref is not None:
            mapping = {cmdref[REST_KW.IDENT.value]: cmdref[REST_KW.NAME.value]}
            action = "command_subscribe_is_active" if on else "command_unsubscribe_is_active"
            return self.send({REST_KW.TYPE.value: action, REST_KW.PARAMS.value: {REST_KW.COMMANDS.value: [{REST_KW.IDENT.value: cmdref[REST_KW.IDENT.value]}]}}, mapping)
        logger.warning(f"command {path} not found in X-Plane commands database")
        return -1

    def register_bulk_command_is_active_event(self, paths, on: bool = True) -> int:
        cmds = []
        mapping = {}
        for path in paths:
            cmdref = self.get_command_info_by_name(path=path)
            if cmdref is None:
                logger.warning(f"command {path} not found in X-Plane commands database")
                continue
            cmds.append({REST_KW.IDENT.value: cmdref[REST_KW.IDENT.value]})
            mapping[cmdref[REST_KW.IDENT.value]] = cmdref[REST_KW.NAME.value]

        if len(cmds) > 0:
            action = "command_subscribe_is_active" if on else "command_unsubscribe_is_active"
            return self.send({REST_KW.TYPE.value: action, REST_KW.PARAMS.value: {REST_KW.COMMANDS.value: cmds}}, mapping)
        action = "register" if on else "unregister"
        logger.warning(f"no bulk command active to {action}")
        return -1

    def set_command_is_active_with_duration(self, path: str, duration: float = 0.0) -> int:
        cmd = self.get_command_info_by_name(path)
        if cmd is not None:
            return self.send(
                {
                    REST_KW.TYPE.value: "command_set_is_active",
                    REST_KW.PARAMS.value: {
                        REST_KW.COMMANDS.value: [
                            {REST_KW.IDENT.value: cmd[REST_KW.IDENT.value], REST_KW.ISACTIVE.value: True, REST_KW.DURATION.value: duration}
                        ]
                    },
                }
            )
        logger.warning(f"command {path} not found in X-Plane commands database")
        return -1

    def set_command_is_active_without_duration(self, path: str, active: bool) -> int:
        cmd = self.get_command_info_by_name(path)
        if cmd is not None:
            return self.send(
                {
                    REST_KW.TYPE.value: "command_set_is_active",
                    REST_KW.PARAMS.value: {REST_KW.COMMANDS.value: [{REST_KW.IDENT.value: cmd[REST_KW.IDENT.value], REST_KW.ISACTIVE.value: active}]},
                }
            )
        logger.warning(f"command {path} not found in X-Plane commands database")
        return -1

    def set_command_is_active_true_without_duration(self, path) -> int:
        return self.set_command_is_active_without_duration(path=path, active=True)

    def set_command_is_active_false_without_duration(self, path) -> int:
        return self.set_command_is_active_without_duration(path=path, active=False)

    @abstractmethod
    def start(self):
        raise NotImplementedError


# Connector to X-Plane status (COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS)
# 0 = Connection monitor to X-Plane is not running
# 1 = Connection monitor to X-Plane running, not connected to websocket
# 2 = Connected to websocket, WS receiver not running
# 3 = Connected to websocket, WS receiver running
# 4 = WS receiver has received data from simulator


# #############################################
# SIMULATOR
#
class XPlane(Simulator, SimulatorVariableListener, XPlaneWebSocket):
    """
    Get data from XPlane via network.
    Use a class to implement RAI Pattern for the UDP socket.
    """

    name = "X-Plane"

    def __init__(self, cockpit, environ):
        self._inited = False
        # list of requested datarefs with index number
        self.datarefs = set()  # list of datarefs currently monitored
        self.cmdevents = set()  # list of command active events currently monitored
        self._max_datarefs_monitored = 0
        self._max_events_monitored = 0

        self.ws_event = threading.Event()
        self.ws_thread = None
        self._dref_cache = {}
        self._permanent_observables = []  # cannot create them now since XPlane does not exist yet
        self._observables = None  # local config observables for this simulator

        self.xp_home = environ.get(ENVIRON_KW.SIMULATOR_HOME.value)
        self.api_host = environ.get(ENVIRON_KW.API_HOST.value, "127.0.0.1")
        self.api_port = environ.get(ENVIRON_KW.API_PORT.value, 8086)
        self.api_path = environ.get(ENVIRON_KW.API_PATH.value, "/api")
        self.api_version = environ.get(ENVIRON_KW.API_VERSION.value, "v1")

        Simulator.__init__(self, cockpit=cockpit, environ=environ)
        XPlaneWebSocket.__init__(self, host=self.api_host[0], port=self.api_host[1], api=self.api_path, api_version=self.api_version)
        SimulatorVariableListener.__init__(self, name=self.name)
        self.cockpit.set_logging_level(__name__)

        self.init()

    def __del__(self):
        if not self._inited:
            return
        self.register_bulk_command_is_active_event(paths=self.cmdevents, on=False)
        self.cmdevents = set()
        self.register_bulk_dataref_value_event(paths=self.datarefs, on=False)
        self.datarefs = set()
        self.disconnect()

    def init(self):
        if self._inited:
            return
        # Create internal variable to hold the connection status
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=0, cascade=True)
        self._inited = True

    def get_version(self) -> list:
        return [f"{type(self).__name__} {__version__}"]

    # ################################
    # Factories
    #
    def variable_factory(self, name: str, is_string: bool = False, creator: str = None) -> Dataref:
        # logger.debug(f"creating xplane dataref {name}")
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
    def same_host(self) -> bool:
        return self._beacon.same_host() if self.connected else False

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

    # ################################
    # Observables
    #
    @property
    def observables(self) -> list:
        ret = self._permanent_observables
        if self._observables is not None:
            if hasattr(self._observables, "observables"):
                ret = ret + self._observables.observables
            elif type(self._observables) is list:
                ret = ret + self._observables
            else:
                logger.warning(f"observables: {type(self._observables)} unknown")
        return ret

    def load_observables(self):
        if self._observables is not None:
            return
        fn = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", RESOURCES_FOLDER, OBSERVABLES_FILE))
        if os.path.exists(fn):
            config = {}
            with open(fn, "r") as fp:
                config = yaml.load(fp)
            self._observables = Observables(config=config, simulator=self)
            logger.info(f"loaded {len(self._observables.observables)} simulator observables")
        else:
            logger.info("no simulator observables")

    def create_permanent_observables(self):
        # Permanent observables are "coded" observables
        # They are created the first time add_permanently_monitored_simulator_variables() or add_permanently_monitored_simulator_events() is called
        cd_obs = self.cockpit.get_permanent_observables()
        if len(self._permanent_observables) > 0 or len(cd_obs) == 0:
            return
        self._permanent_observables = [obs(simulator=self) for obs in cd_obs]
        logger.info(f"loaded {len(self._permanent_observables)} permanent simulator observables")
        self.load_observables()

    #
    # Datarefs
    def get_variables(self) -> set:
        """Returns the list of datarefs for which the xplane simulator wants to be notified."""
        ret = set(PERMANENT_SIMULATOR_VARIABLES)
        for obs in self.observables:
            ret = ret | obs.get_variables()
        more = self.cockpit.aircraft.get_variables()
        if len(more) > 0:
            ret = ret | more
        # The Cockpit is not aware of any simulator variable it could ask for
        return ret

    def simulator_variable_changed(self, data: SimulatorVariable):
        pass

    #
    # Events
    def get_events(self) -> set:
        """Returns the list of datarefs for which the xplane simulator wants to be notified."""
        ret = set(PERMANENT_SIMULATOR_EVENTS)
        for obs in self.observables:
            ret = ret | obs.get_events()
        more = self.cockpit.aircraft.get_events()
        if len(more) > 0:
            ret = ret | more
        # The Cockpit is not aware of any simulator event it could ask for
        return ret

    # ################################
    # X-Plane Interface
    #
    # Instruction execution
    #
    def execute_command(self, command: Command | None):
        self.command_once(command)

    def command_once(self, command: Command):
        if not command.is_valid():
            logger.warning(f"command '{command}' not sent (command placeholder, no command, do nothing)")
            return
        if not self.connected:
            logger.warning(f"no connection ({command})")
            return
        if command.path is not None:
            self.set_command_is_active_with_duration(path=command.path)
            logger.log(SPAM_LEVEL, f"executed {command}")
        else:
            logger.warning("no command to execute")

    def command_begin(self, command: Command):
        if not command.is_valid():
            logger.warning(f"command '{command}' not sent (command placeholder, no command, do nothing)")
            return
        if not self.connected:
            logger.warning(f"no connection ({command})")
            return
        if command.path is not None:
            self.set_command_is_active_true_without_duration(path=command.path)
            logger.log(SPAM_LEVEL, f"executing {command}..")
        else:
            logger.warning("no command to execute")

    def command_end(self, command: Command):
        if not command.is_valid():
            logger.warning(f"command '{command}' not sent (command placeholder, no command, do nothing)")
            return
        if not self.connected:
            logger.warning(f"no connection ({command})")
            return
        if command.path is not None:
            self.set_command_is_active_false_without_duration(path=command.path)
            logger.log(SPAM_LEVEL, f"..executed {command}")
        else:
            logger.warning("no command to execute")

    #
    # Variable management
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
                drefs[d] = dref
        logger.info(f"monitoring {len(drefs)} permanent simulator variables")
        if len(drefs) > 0:
            self.add_simulator_variables_to_monitor(simulator_variables=drefs, reason="permanent simulator variables")

    def clean_simulator_variables_to_monitor(self):
        if not self.connected:
            return
        self.register_bulk_dataref_value_event(paths=self.datarefs, on=False)
        self.datarefs = set()
        super().clean_simulator_variable_to_monitor()
        self._strdref_cache = {}
        self._dref_cache = {}
        logger.debug("done")

    def cleanup_monitored_simulator_variables(self):
        nolistener = []
        for d in self.datarefs:
            dref = self.get_variable(d)
            if len(dref.listeners) == 0:
                nolistener.append(d)
        logger.info(f"no listener for {', '.join(nolistener)}")
        # self.register_bulk_dataref_value_event(paths=remove, on=False)

    def print_currently_monitored_variables(self, with_value: bool = True):
        return
        logger.log(SPAM_LEVEL, ">>>>> currently monitored variables is disabled")
        self.cleanup_monitored_simulator_variables()
        return
        if with_value:
            values = [f"{d}={self.get_variable(d).value}" for d in self.datarefs]
            logger.log(SPAM_LEVEL, f">>>>> currently monitored variables:\n{'\n'.join(sorted(values))}")
            return
        logger.log(SPAM_LEVEL, f">>>>> currently monitored variables:\n{'\n'.join(sorted(self.datarefs))}")

    def add_simulator_variables_to_monitor(self, simulator_variables, reason: str | None = None):
        if not self.connected:
            logger.debug(f"would add {list(filter(lambda d: not Dataref.is_internal_variable(d), simulator_variables))}")
            return
        if len(simulator_variables) == 0:
            logger.debug("no variable to add")
            return
        # Add those to monitor
        super().add_simulator_variables_to_monitor(simulator_variables=simulator_variables)
        paths = set()
        for d in simulator_variables.values():
            if d.is_internal:
                logger.debug(f"local dataref {d.name} is not monitored")
                continue
            paths.add(d.name)
        if len(paths) > 0:
            self.register_bulk_dataref_value_event(paths=paths, on=True)
            self.datarefs = self.datarefs | paths
            self._max_datarefs_monitored = max(self._max_datarefs_monitored, len(self.datarefs))

            logger.log(SPAM_LEVEL, f">>>>> add_simulator_variable_to_monitor: {reason}: added {paths}")
        else:
            logger.debug("no variable to add")
        self.print_currently_monitored_variables()
        if MONITOR_RESOURCE_USAGE:
            logger.info(
                f">>>>> monitoring variables++{len(simulator_variables)}({len(paths)})/{len(self.datarefs)}/{self._max_datarefs_monitored} {reason if reason is not None else ''}"
            )

    def remove_simulator_variables_to_monitor(self, simulator_variables: dict, reason: str | None = None):
        if not self.connected and len(self.simulator_variable_to_monitor) > 0:
            logger.debug(f"would remove {simulator_variables.keys()}/{self._max_datarefs_monitored}")
            return
        if len(simulator_variables) == 0:
            logger.debug("no variable to remove")
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
        if len(paths) > 0:
            self.register_bulk_dataref_value_event(paths=paths, on=False)
            self.datarefs = self.datarefs - paths
            super().remove_simulator_variables_to_monitor(simulator_variables=simulator_variables)
            logger.log(SPAM_LEVEL, f">>>>> remove_simulator_variables_to_monitor: {reason}: removed {paths}")
        else:
            logger.debug("no variable to remove")
        self.print_currently_monitored_variables()
        if MONITOR_RESOURCE_USAGE:
            logger.info(
                f">>>>> monitoring variables--{len(simulator_variables)}({len(paths)})/{len(self.datarefs)}/{self._max_datarefs_monitored} {reason if reason is not None else ''}"
            )

    def remove_all_simulator_variables_to_monitor(self):
        datarefs = [d for d in self.cockpit.variable_database.database.values() if type(d) is Dataref]
        if not self.connected and len(datarefs) > 0:
            logger.debug(f"would remove {', '.join([d.name for d in datarefs])}")
            return
        # This is not necessary:
        # self.remove_simulator_variable_to_monitor(datarefs)
        super().remove_all_simulator_variable()

    def add_all_simulator_variables_to_monitor(self):
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
        if len(paths) > 0:
            self.register_bulk_dataref_value_event(paths=paths, on=True)
            self.datarefs = self.datarefs | paths
            self._max_datarefs_monitored = max(self._max_datarefs_monitored, len(self.datarefs))
            logger.log(SPAM_LEVEL, f">>>>> add_permanently_monitored_simulator_variables: added {paths}")
        logger.debug("no simulator variable to monitor")

    #
    # Event management
    def add_permanently_monitored_simulator_events(self):
        # self.create_permanent_observables() should be called before
        # like in add_permanently_monitored_simulator_variables()
        self.create_permanent_observables()
        cmds = self.get_events()
        logger.info(f"monitoring {len(cmds)} permanent simulator events")
        if len(cmds) > 0:
            self.add_simulator_events_to_monitor(simulator_events=cmds, reason="permanent simulator events")

    def clean_simulator_events_to_monitor(self):
        if not self.connected:
            return
        self.register_bulk_command_is_active_event(paths=self.cmdevents, on=False)
        self.cmdevents = set()
        super().clean_simulator_event_to_monitor()
        self._strdref_cache = {}
        self._dref_cache = {}
        logger.debug("done")

    def print_currently_monitored_events(self):
        logger.log(SPAM_LEVEL, f">>>>> currently monitored events:\n{'\n'.join(sorted(self.cmdevents))}")

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

    # ################################
    # Cockpit interface
    #
    def ws_receiver(self):
        """Read and decode websocket messages and enqueue events"""

        # WS_PCKT_RECV = "websockets_packets_received"
        # WS_RSLT_RECV = "websockets_result_received"
        # WS_VUPD_RECV = "websockets_value_update_received"
        # WS_DREF_RECV = "websockets_dataref_value_received"
        # WS_CMDS_RECV = "websockets_command_active_received"
        # COMMAND_ACTIVE_ENQUEUED: Command active enqueued
        # UPDATE_ENQUEUED = Value change enqueued
        # UPDATE_ENQUEUED = Value change enqueued

        def dref_round(local_path: str, local_value):
            local_r = self.get_rounding(simulator_variable_name=local_path)
            local_v = round(local_value, local_r) if local_r is not None and local_value is not None else local_value
            return 0.0 if local_v < 0.0 and local_v > -0.001 else local_v

        def dref_round_arr(local_path: str, local_value):
            local_r = self.get_rounding(simulator_variable_name=local_path)
            if local_r is not None:
                return [round(l, local_r) for l in local_value]
            return local_value

        logger.log(SPAM_LEVEL, "starting websocket listener..")
        RECEIVE_TIMEOUT = 1  # when not connected, checks often
        total_reads = 0
        print_zulu = 0
        cnt = 0
        cnt_mod = 10
        start_time = datetime.now()
        last_read_ts = start_time
        total_read_time = 0.0
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=3, cascade=True)
        while not self.ws_event.is_set():
            try:
                message = self.ws.receive(timeout=RECEIVE_TIMEOUT)
                if message is None:
                    cnt = cnt + 1
                    if cnt % cnt_mod == 0:
                        logger.log(SPAM_LEVEL, "waiting for data from simulator..")  # at {datetime.now()}")
                    continue

                now = datetime.now()
                if total_reads == 0:
                    logger.log(SPAM_LEVEL, f"..first message at {now} ({round((now - start_time).seconds, 2)} secs.)")
                    RECEIVE_TIMEOUT = 5  # when connected, check less often, message will arrive
                # Estimate response time
                self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=4, cascade=True)

                self.inc(COCKPITDECKS_INTVAR.WS_PCKT_RECV.value)
                total_reads = total_reads + 1
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
                    resp_type = data[REST_KW.TYPE.value]
                    #
                    #
                    if resp_type == REST_RESPONSE.RESULT.value:

                        self.inc(COCKPITDECKS_INTVAR.WS_RSLT_RECV.value)
                        webapi_logger.info(f"<<RCV  {data}")
                        req_id = data.get(REST_KW.REQID.value)
                        if req_id is not None:
                            self._requests[req_id] = data[REST_KW.SUCCESS.value]
                        if not data[REST_KW.SUCCESS.value]:
                            errmsg = REST_KW.SUCCESS.value if data[REST_KW.SUCCESS.value] else "failed"
                            errmsg = errmsg + " " + data.get("error_message")
                            errmsg = errmsg + " (" + data.get("error_code") + ")"
                            logger.warning(f"req. {req_id}: {errmsg}")
                        else:
                            logger.debug(f"req. {req_id}: {REST_KW.SUCCESS.value if data[REST_KW.SUCCESS.value] else 'failed'}")
                    #
                    #
                    elif resp_type == REST_RESPONSE.COMMAND_ACTIVE.value:

                        self.inc(COCKPITDECKS_INTVAR.WS_CMDS_RECV.value)
                        if REST_KW.DATA.value in data:
                            for cidx, value in data[REST_KW.DATA.value].items():
                                cref = self.get_command_info_by_id(int(cidx))
                                if cref is not None:
                                    c = cref[REST_KW.NAME.value]
                                    v = value
                                    webapi_logger.info(f"CMD : {c}={value}")
                                    e = CommandActiveEvent(sim=self, command=c, is_active=value, cascade=True)
                                    self.inc(COCKPITDECKS_INTVAR.COMMAND_ACTIVE_ENQUEUED.value)
                        else:
                            logger.warning(f"no data: {data}")
                    #
                    #
                    elif resp_type == REST_RESPONSE.DATAREF_UPDATE.value:

                        self.inc(COCKPITDECKS_INTVAR.WS_VUPD_RECV.value)
                        if REST_KW.DATA.value in data:
                            for didx, value in data[REST_KW.DATA.value].items():
                                if didx in self._dref_cache and self._dref_cache[didx] == value:  # cached RAW value
                                    continue
                                self._dref_cache[didx] = value
                                dref = self.get_dataref_info_by_id(int(didx))
                                if dref is not None:
                                    self.inc_dref(dref)
                                    d = dref[REST_KW.NAME.value]

                                    # Special treatment of ZULU time
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
                                        if print_zulu % 120 == 0:
                                            webapi_logger.info(f"DREF: {d} {self._dref_cache.get(d)} -> {v}")
                                            self.req_stats()
                                        e = DatarefEvent(sim=self, dataref=d, value=value, cascade=True)
                                        self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)
                                        continue

                                    if dref.get(REST_KW.VALUE_TYPE.value) is not None and dref[REST_KW.VALUE_TYPE.value] in ["int_array", "float_array"]:

                                        # Arrays
                                        # Whole array
                                        if len(dref[INDICES]) == 0:
                                            v = dref_round_arr(local_path=d, local_value=value)
                                            if d not in self._dref_cache or self._dref_cache[d] != v:  # cached rounded value
                                                cascade = d in self.simulator_variable_to_monitor.keys()
                                                webapi_logger.info(f"DREF WHOLE ARRAY: {d} {self._dref_cache.get(d)} -> {v} (cascade={cascade})")
                                                e = DatarefEvent(sim=self, dataref=d, value=v, cascade=cascade)  # send raw value if possible
                                                self._dref_cache[d] = value
                                                self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)
                                            continue

                                        # Single array element
                                        if INDICES not in dref or len(value) != len(dref[INDICES]):
                                            logger.warning(f"dataref array {d} size mismatch ({len(value)}/{len(dref[INDICES])})")
                                            logger.warning(f"dataref array {d}: value: {value}, indices: {dref[INDICES]})")
                                        for v1, idx in zip(value, dref[INDICES]):
                                            d1 = f"{d}[{idx}]"
                                            v = dref_round(local_path=d1, local_value=v1)
                                            if d1 not in self._dref_cache or self._dref_cache[d1] != v:  # cached rounded value
                                                cascade = d1 in self.simulator_variable_to_monitor.keys()
                                                webapi_logger.info(f"DREF ARRAY: {d}[{idx}] {self._dref_cache.get(d1)} -> {v} (cascade={cascade})")
                                                e = DatarefEvent(sim=self, dataref=d1, value=v1, cascade=cascade)
                                                self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)
                                                self._dref_cache[d1] = v

                                    else:
                                        # Scalar values
                                        v = value
                                        send_raw = True
                                        if (
                                            dref.get(REST_KW.VALUE_TYPE.value) is not None
                                            and dref[REST_KW.VALUE_TYPE.value] == "data"
                                            and type(value) in [bytes, str]
                                        ):  # data = string
                                            v = base64.b64decode(value).decode("ascii").replace("\u0000", "")
                                            send_raw = False
                                        elif type(v) in [int, float]:
                                            v = dref_round(local_path=d, local_value=value)

                                        if d not in self._dref_cache or self._dref_cache[d] != v:  # cached rounded value
                                            cascade = d in self.simulator_variable_to_monitor.keys()
                                            webapi_logger.info(f"DREF: {d} {self._dref_cache.get(d)} -> {v} (cascade={cascade})")
                                            e = DatarefEvent(sim=self, dataref=d, value=value if send_raw else v, cascade=cascade)  # send raw value if possible
                                            self._dref_cache[d] = v
                                            self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)

                                else:
                                    logger.warning(f"dataref {didx} not found")
                        else:
                            logger.warning(f"no data: {data}")
                    #
                    #
                    else:
                        logger.warning(f"invalid response type {resp_type}: {data}")

                except:
                    logger.warning(f"decode data {data} failed", exc_info=True)

            except ConnectionClosed:
                logger.warning("websocket connection closed")
                self.ws = None
                self.ws_event.set()

            except:
                logger.error("ws_receiver: other error", exc_info=True)

        if self.ws is not None:  # in case we did not receive a ConnectionClosed event
            self.ws.close()
            self.ws = None

        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=2, cascade=True)
        logger.info("..websocket listener terminated")

    def start(self):
        if not self.connected:
            logger.warning("not connected. cannot not start.")
            return

        if not self.ws_event.is_set():  # Thread for X-Plane datarefs
            self.ws_thread = threading.Thread(target=self.ws_receiver, name="XPlane::WebSocket Listener")
            self.ws_thread.start()
            logger.info("websocket listener started")
        else:
            logger.info("websocket listener already running.")

        # When restarted after network failure, should clean all datarefs
        # then reload datarefs from current page of each deck
        self.reload_caches()
        self.clean_simulator_variables_to_monitor()
        self.add_all_simulator_variables_to_monitor()
        self.clean_simulator_events_to_monitor()
        self.add_all_simulator_events_to_monitor()
        logger.info("reloading pages")
        self.cockpit.reload_pages()  # to request page variables and take into account updated values

    def stop(self):
        if not self.ws_event.is_set():
            if self.all_datarefs is not None:
                self.all_datarefs.save("datarefs.json")
            if self.all_commands is not None:
                self.all_commands.save("commands.json")
            self.cleanup()
            self.ws_event.set()
            if self.ws_thread is not None and self.ws_thread.is_alive():
                logger.debug("stopping websocket listener..")
                wait = RECEIVE_TIMEOUT
                logger.debug(f"..asked to stop websocket listener (this may last {wait} secs. for timeout)..")
                self.ws_thread.join(wait)
                if self.ws_thread.is_alive():
                    logger.warning("..thread may hang in ws.receive()..")
                logger.debug("..websocket listener stopped")
        else:
            logger.debug("websocket listener not running")

    def cleanup(self):
        """
        Called when before disconnecting.
        Just before disconnecting, we try to cancel dataref UDP reporting in X-Plane
        """
        logger.info("..requesting to stop websocket emission..")
        self.clean_simulator_variables_to_monitor()
        self.clean_simulator_events_to_monitor()

    def reset_connection(self):
        self.stop()
        self.disconnect()
        self.connect()
        self.start()

    def terminate(self):
        logger.debug(f"currently {'not ' if self.ws_event is None else ''}running. terminating..")
        logger.info("terminating..")
        logger.info("..stopping websocket listener..")
        self.stop()
        logger.info("..deleting datarefs..")
        self.remove_all_simulator_variables_to_monitor()
        self.remove_all_simulator_events_to_monitor()
        logger.info("..disconnecting from simulator..")
        self.disconnect()
        logger.info("..terminated")


#
