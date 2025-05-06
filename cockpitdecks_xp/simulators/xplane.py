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
from typing import List
from enum import Enum
from datetime import datetime, timedelta

# Packaging is used in Cockpit to check driver versions
from packaging.version import Version

# REST API
import requests

# WEBSOCKET API
from simple_websocket import Client, ConnectionClosed

from cockpitdecks_xp import __version__
from cockpitdecks import CONFIG_KW, ENVIRON_KW, SPAM_LEVEL, DEPRECATION_LEVEL, MONITOR_RESOURCE_USAGE, RESOURCES_FOLDER, OBSERVABLES_FILE, yaml
from cockpitdecks.strvar import StringWithVariables, Formula
from cockpitdecks.instruction import MacroInstruction

from cockpitdecks.simulator import Simulator, SimulatorEvent, SimulatorInstruction
from cockpitdecks.simulator import SimulatorVariable, SimulatorVariableListener
from cockpitdecks.resources.intvariables import COCKPITDECKS_INTVAR
from cockpitdecks.observable import Observables, Observable
from cockpitdecks.cockpit import CockpitInstruction
from ..resources.beacon import XPlaneBeacon, BEACON_DATA_KW

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

XP_MIN_VERSION = 121400
XP_MIN_VERSION_STR = "12.1.4"
XP_MAX_VERSION = 121499
XP_MAX_VERSION_STR = "12.1.4"

# /api/capabilities introduced in /api/v2. Here is a default one for v1.
V1_CAPABILITIES = {"api": {"versions": ["v1"]}, "x-plane": {"version": "12.1.1"}}
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


# #############################################
# REST OBJECTS
#
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


# DATAREF VALUE TYPES
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


# A value in X-Plane Simulator
#
class Dataref(SimulatorVariable):
    """
    A Dataref is an internal value of the simulation software made accessible to outside modules,
    plugins, or other software in general.
    """

    def __init__(self, path: str, simulator: XPlane, is_string: bool = False):
        # Data
        SimulatorVariable.__init__(self, name=path, simulator=simulator, data_type="string" if is_string else "float")
        self._monitored = 0

        # path with array index sim/some/values[4]
        self.path = path
        self.index = None  # sign is it not a selected array element
        if "[" in path:
            self.path = self.name[: self.name.find("[")]  # sim/some/values
            self.index = int(self.name[self.name.find("[") + 1 : self.name.find("]")])  # 4

    def __str__(self) -> str:
        if self.index is not None:
            return f"{self.path}[{self.index}]={self.value}"
        else:
            return f"{self.path}={self.value}"

    @property
    def meta(self) -> DatarefMeta | None:
        r = self.simulator.all_datarefs.get(self.path) if self.simulator.all_datarefs is not None else None
        if r is None:
            logger.error(f"dataref {self.path} has no api meta data")
        return r

    @property
    def valid(self) -> bool:
        return self.meta is not None

    @property
    def ident(self) -> int | None:
        if not self.valid:
            logger.error(f"dataref {self.path} not valid")
            return None
        return self.meta.ident

    @property
    def value_type(self) -> str | None:
        if not self.valid:
            logger.error(f"dataref {self.path} not valid")
            return None
        return self.meta.value_type

    @property
    def is_writable(self) -> bool:
        if not self.valid:
            logger.error(f"dataref {self.path} not valid")
            return False
        return self.meta.is_writable

    @property
    def is_array(self) -> bool:
        if not self.valid:
            logger.error(f"dataref {self.path} not valid")
            return False
        return self.value_type in [DATAREF_DATATYPE.INTARRAY.value, DATAREF_DATATYPE.FLOATARRAY.value]

    @property
    def selected_indices(self) -> bool:
        if not self.valid:
            logger.error(f"dataref {self.path} not valid")
            return False
        return len(self.meta.indices) > 0

    @property
    def use_rest(self):
        return USE_REST and (hasattr(self.simulator, "same_host") and not self.simulator.same_host())

    @property
    def rest_value(self):
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

    @property
    def is_monitored(self):
        return self._monitored > 0

    @property
    def monitored_count(self) -> int:
        return self._monitored

    def monitor(self):
        self._monitored = self._monitored + 1

    def unmonitor(self) -> bool:
        # IF returns False, no longer monitored
        if self._monitored > 0:
            self._monitored = self._monitored - 1
        else:
            logger.warning(f"{self.name} currently not monitored")
        return self._monitored > 0

    def rest_write(self) -> bool:
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
            logger.error(f"dataref {self.path} not valid")
            return False
        return self._write()

    def parse_raw_value(self, raw_value):
        if not self.valid:
            logger.error(f"dataref {self.path} not valid")
            return None

        if self.value_type in [DATAREF_DATATYPE.INTARRAY.value, DATAREF_DATATYPE.FLOATARRAY.value]:
            # 1. Arrays
            # 1.1 Whole array
            if type(raw_value) is not list:
                logger.warning(f"dataref array {self.name}: value: is not a list ({value}, {type(value)})")
                return None

            if len(self.meta.indices) == 0:
                logger.debug(f"dataref array {self.name}: no index, returning whole array")
                return raw_value

            # 1.2 Single array element
            if len(raw_value) != len(self.meta.indices):
                logger.warning(f"dataref array {self.name} size mismatch ({len(raw_value)}/{len(self.meta.indices)})")
                logger.warning(f"dataref array {self.name}: value: {raw_value}, indices: {self.meta.indices})")
                return None

            idx = self.meta.indices.index(self.index)
            if idx == -1:
                logger.warning(f"dataref index {self.index} not found in {self.meta.indices}")
                return None

            logger.debug(f"dataref array {self.name}: returning {self.name}[{idx}]={raw_value[idx]}")
            return raw_value[idx]

        else:
            # 2. Scalar values
            # 2.1  String
            if self.value_type == "data" and type(raw_value) in [bytes, str]:
                return base64.b64decode(raw_value).decode("ascii").replace("\u0000", "")

            # 2.1  Number (in python, float is double precision)
            elif type(raw_value) not in [int, float]:
                logger.warning(f"unknown value type for {self.name}: {type(raw_value)}, {raw_value}, expected {self.value_type}")

        return raw_value


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
class XPlaneInstruction(SimulatorInstruction, ABC):
    """An Instruction sent to the XPlane Simulator to execute some action.

    This is more an abstract base class, with a new() factory to handle instruction block.
    """

    def __init__(self, name: str, simulator: XPlane, delay: float = 0.0, condition: str | None = None, button: "Button" = None) -> None:
        SimulatorInstruction.__init__(self, name=name, simulator=simulator, delay=delay, condition=condition)

    @property
    def meta(self) -> CommandMeta | None:
        r = self.simulator.all_commands.get(self.path) if self.simulator.all_commands is not None else None
        if r is None:
            logger.error(f"command {self.path} has no api meta data")
        return r

    @property
    def valid(self) -> bool:
        return self.meta is not None

    @property
    def ident(self) -> int | None:
        if not self.valid:
            logger.error(f"command {self.path} not valid")
            return None
        return self.meta.ident

    @property
    def description(self) -> str | None:
        if not self.valid:
            return None
        return self.meta.description

    @property
    def use_rest(self):
        return USE_REST and (hasattr(self.simulator, "same_host") and not self.simulator.same_host())

    @property
    def is_no_operation(self) -> bool:
        return self.path is not None and self.path.lower().replace("-", "") in NOT_A_COMMAND

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

    @abstractmethod
    def rest_execute(self) -> bool:  # ABC
        return False

    @abstractmethod
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
            logger.error(f"command {self.path} is not an operation")
            return False
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
        if not self.is_valid():
            logger.error(f"command {self.path} is not an operation")
            return False
        if not self.valid:
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
        return self._variable.valid if isinstance(self._variable, Dataref) else True

    def rest_execute(self) -> bool:
        if not self.valid:
            logger.error(f"set-dataref: dataref {self._variable.name} is not valid")
            return False
        value = self.value
        if self.value_type == "data":
            value = str(value).encode("ascii")
            value = base64.b64encode(value).decode("ascii")
        ident = self._variable.ident
        payload = {REST_KW.DATA.value: self.value}
        url = f"{self.simulator.api_url}/datarefs/{ident}/value"
        response = requests.patch(url, json=payload)
        webapi_logger.info(f"PATCH {url} {payload} {response}")
        if response.status_code == 200:
            return True
        if response.status_code == 403:
            logger.warning(f"{self._variable.name}: dataref not writable")
            return False
        logger.error(f"execute: {response}")
        return False

    def ws_execute(self) -> int:
        if not self.valid:
            logger.error(f"set-dataref: dataref {self._variable.name} is not valid")
            return -1
        return self.simulator.set_dataref_value(path=self.path, value=self.value)

    def _execute(self):
        if isinstance(self._variable, Dataref):
            logger.debug(f"{self.name}: updating {self._variable.name}..")
            super()._execute()
            logger.debug(f"{self.name}: ..updated")
        else:
            self._variable.update_value(new_value=self.value, cascade=True)
            logger.debug(f"{self.name}: updated variable {self._variable.name} to {self.value}")


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


# #############################################
# WEB API
#
class DatarefMeta:

    def __init__(self, name: str, value_type: str, is_writable: bool, **kwargs) -> None:
        self.name = name
        self.ident = kwargs.get("id")
        self.value_type = value_type
        self.is_writable = is_writable
        self.indices = list()
        self.indices_history = []

        self.updates = 0
        self._last_req_number = 0
        self._previous_value = None
        self._current_value = None

    @property
    def value(self):
        return self._current_value

    @value.setter
    def value(self, value):
        self._previous_value = self._current_value
        self._current_value = value
        self.updates = self.updates + 1

    @property
    def is_array(self) -> bool:
        return self.value_type in [DATAREF_DATATYPE.INTARRAY.value, DATAREF_DATATYPE.FLOATARRAY.value]

    def save_indices(self):
        self.indices_history.append(self.indices.copy())

    def last_indices(self) -> list:
        if len(self.indices_history) > 0:
            return self.indices_history[-1]
        return []

    def append_index(self, i):
        if i not in self.indices:
            self.indices.append(i)
            """Note from Web API instruction/manual:
            If you subscribed to certain indexes of the dataref, they’ll be sent in the index order
            but no sparse arrays will be sent. For example if you subscribed to indexes [1, 5, 7] you’ll get
            a 3 item array like [200, 200, 200], meaning you need to remember that the first item of that response
            corresponds to index 1, the second to index 5 and the third to index 7 of the dataref.
            This also means that if you subscribe to index 2 and later to index 0 you’ll get them as [0,2].

            HENCE current_indices.sort()

            So bottom line is — keep it simple: either ask for a single index, or a range,
            or all; and if later your requirements change, unsubscribe, then subscribe again.
            """
            # self.indices.sort()

    def remove_index(self, i):
        # there is a problem if we remove a key here, and then still get
        # an array of values that contains the removed index
        if i in self.indices:
            self.indices.remove(i)
        else:
            logger.warning(f"{self.name} index {i} not in {self.indices}")


class CommandMeta:

    def __init__(self, name: str, description: str, **kwargs) -> None:
        self.name = name
        self.ident = kwargs.get("id")
        self.description = description


class APIMeta:

    @classmethod
    def new(cls, **kwargs):
        if "is_writable" in kwargs:
            return DatarefMeta(**kwargs)
        return CommandMeta(**kwargs)


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
        self._raw = {}
        self._by_name = dict()
        self._by_ids = dict()
        self._last_updated = 0

    def load(self, path):
        url = self.api.api_url + path
        response = requests.get(url)
        if response.status_code != 200:  # We have version 12.1.4 or above
            logger.error(f"load: response={response.status_code}")
            return
        raw = response.json()
        data = raw[REST_KW.DATA.value]
        self._raw = data

        metas = [APIMeta.new(**c) for c in data]
        self.last_cached = datetime.now().timestamp()
        self._by_name = {m.name: m for m in metas}
        self._by_ids = {m.ident: m for m in metas}

        logger.debug(f"{path[1:]} cached ({len(metas)} entries)")

    @property
    def count(self) -> int:
        return 0 if self._by_name is None else len(self._by_name)

    @property
    def has_data(self) -> bool:
        return self._by_name is not None and len(self._by_name) > 0

    def get(self, name) -> DatarefMeta | CommandMeta | None:
        return self.get_by_name(name=name)

    def get_by_name(self, name) -> DatarefMeta | CommandMeta | None:
        return self._by_name.get(name)

    def get_by_id(self, ident: int) -> DatarefMeta | CommandMeta | None:
        return self._by_ids.get(ident)

    def save(self, filename):
        with open(filename, "w") as fp:
            json.dump(self._raw, fp)

    def equiv(self, ident) -> str | None:
        r = self._by_ids.get(ident)
        if r is not None:
            return f"{ident}({r.name})"
        return f"no equivalence for {ident}"


# #############################################
# REST API
#
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
        self.dynamic_timeout = RECONNECT_TIMEOUT
        self._beacon.set_callback(self.beacon_callback)
        self._running_time = Dataref(path=RUNNING_TIME, simulator=self)  # cheating, side effect, works for rest api only, do not force!
        self._aircraft_path = Dataref(path=AIRCRAFT_LOADED, simulator=self)

        self.all_datarefs: Cache | None = None
        self.all_commands: Cache | None = None
        self._last_updated = 0
        self._warning_count = 0

    @property
    # See https://stackoverflow.com/questions/7019643/overriding-properties-in-python
    # to overwrite @property definition
    def api_url(self) -> str:
        """URL for the REST API"""
        return f"http://{self.host}:{self.port}{self._api_root_path}{self._api_version}"

    @property
    # See https://stackoverflow.com/questions/7019643/overriding-properties-in-python
    # to overwrite @property definition
    def is_valid(self) -> bool:
        """URL for the REST API"""
        d = self.all_datarefs is not None and self.all_datarefs.has_data
        c = self.all_commands is not None and self.all_commands.has_data
        a = self._aircraft_path.rest_value
        logger.debug(f"is_valid d={d}, c={c}, a={a is not None}")
        return a is not None and d and c

    @property
    def uptime(self) -> float:
        if self._running_time is not None:
            r = self._running_time.rest_value
            if r is not None:
                return float(r)
        return 0.0

    @property
    def ws_url(self) -> str:
        """URL for the WebSocket API"""
        url = self.api_url
        return url.replace("http:", "ws:")

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
            if self._warning_count % 20 == 0:
                logger.warning(f"api unreachable, may be X-Plane is not running")
                self._warning_count = self._warning_count + 1
        except:
            logger.error(f"api unreachable, may be X-Plane is not running", exc_info=True)
        return False

    def beacon_callback(self, connected: bool):
        if connected:
            logger.info("X-Plane beacon connected")
            if self._beacon.connected:
                self.dynamic_timeout = 0.5  # seconds
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
                self._capabilities = V1_CAPABILITIES
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
        MINTIME_BETWEEN_RELOAD = 10  # seconds
        if self._last_updated != 0:
            currtime = self._running_time.rest_value
            if currtime is not None:
                difftime = currtime - self._last_updated
                if difftime < MINTIME_BETWEEN_RELOAD:
                    logger.info(f"dataref cache not updated, updated {round(difftime, 1)} secs. ago")
                    return
            else:
                logger.warning("no value for sim/time/total_running_time_sec")
        self.all_datarefs = Cache(self)
        self.all_datarefs.load("/datarefs")
        self.all_datarefs.save("webapi-datarefs.json")
        self.all_commands = Cache(self)
        if self.version == "v2":  # >
            self.all_commands.load("/commands")
            self.all_commands.save("webapi-commands.json")
        currtime = self._running_time.rest_value
        if currtime is not None:
            self._last_updated = self._running_time.rest_value
        else:
            logger.warning("no value for sim/time/total_running_time_sec")
        logger.info(
            f"dataref cache ({self.all_datarefs.count}) and command cache ({self.all_commands.count}) reloaded, sim uptime {str(timedelta(seconds=int(self.uptime)))}"
        )

    def get_dataref_meta_by_name(self, path: str) -> DatarefMeta | None:
        return self.all_datarefs.get_by_name(path) if self.all_datarefs is not None else None

    def get_command_meta_by_name(self, path: str) -> CommandMeta | None:
        return self.all_commands.get_by_name(path) if self.all_commands is not None else None

    def get_dataref_meta_by_id(self, ident: int) -> DatarefMeta | None:
        return self.all_datarefs.get_by_id(ident) if self.all_datarefs is not None else None

    def get_command_meta_by_id(self, ident: int) -> CommandMeta | None:
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
        self.ws_event = None
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

    @property
    def connected_and_valid(self) -> bool:
        logger.info(f">>> is {'' if self.is_valid else 'in'}valid")
        return self.connected and self.is_valid

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
        to_count = 0
        noconn = 0
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=1, cascade=True)
        while self.should_not_connect is not None and not self.should_not_connect.is_set():
            if not self.connected:
                try:
                    if noconn % WARN_FREQ == 0:
                        logger.info("not connected, trying..")
                        noconn = noconn + 1
                    self.connect_websocket()
                    if self.connected:
                        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=2, cascade=True)
                        self._already_warned = 0
                        number_of_timeouts = 0
                        self.dynamic_timeout = RECONNECT_TIMEOUT
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
                                logger.info(f"X-Plane version requirements {xpmin}<= {curr} <={xpmax} satisfied")
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

                        if number_of_timeouts >= MAX_TIMEOUT_COUNT and to_count % WARN_FREQ == 0:
                            logger.error(f"..X-Plane instance not found on local network.. ({datetime.now().strftime('%H:%M:%S')})")
                        to_count = to_count + 1
                except:
                    logger.error(f"..X-Plane instance not found on local network.. ({datetime.now().strftime('%H:%M:%S')})", exc_info=True)
                # If still no connection (above attempt failed)
                # we wait before trying again
                if not self.connected:
                    self.dynamic_timeout = 1
                    self.should_not_connect.wait(self.dynamic_timeout)
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
                self._requests[req_id] = None  # may be should remember timestamp, etc. if necessary, create Request class.
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
    def set_dataref_value(self, path, value) -> int:
        def split_dataref_path(path):
            name = path
            index = -1
            split = "[" in path and "]" in path
            if split:  # sim/some/values[4]
                name = path[: path.find("[")]
                index = int(path[path.find("[") + 1 : path.find("]")])  # 4
            meta = self.get_dataref_meta_by_name(name)
            return split, meta, name, index

        if value is None:
            logger.warning(f"dataref {path} has no value to set")
            return -1
        split, meta, name, index = split_dataref_path(path)
        if meta is None:
            logger.warning(f"dataref {path} not found in X-Plane datarefs database")
            return -1
        payload = {
            REST_KW.TYPE.value: "dataref_set_values",
            REST_KW.PARAMS.value: {REST_KW.DATAREFS.value: [{REST_KW.IDENT.value: meta.ident, REST_KW.VALUE.value: value}]},
        }
        mapping = {meta.ident: meta.name}
        if split:
            payload[REST_KW.PARAMS.value][REST_KW.DATAREFS.value][0][REST_KW.INDEX.value] = index
        return self.send(payload, mapping)

    def register_bulk_dataref_value_event(self, datarefs, on: bool = True) -> bool:
        drefs = []
        for dataref in datarefs.values():
            if type(dataref) is list:
                meta = self.get_dataref_meta_by_id(dataref[0].ident)  # we modify the global source info, not the local copy in the Dataref()
                if meta is None:
                    logger.warning(f"{dataref[0].name}: cannot get meta information")
                    continue
                webapi_logger.info(f"INDICES bef: {dataref[0].ident} => {meta.indices}")
                meta.save_indices()  # indices of "current" requests
                ilist = []
                otext = "add"
                for d1 in dataref:
                    ilist.append(d1.index)
                    if on:
                        meta.append_index(d1.index)
                    else:
                        otext = "del"
                        meta.remove_index(d1.index)
                    meta._last_req_number = self.req_number  # not 100% correct, but sufficient
                drefs.append({REST_KW.IDENT.value: dataref[0].ident, REST_KW.INDEX.value: ilist})
                webapi_logger.info(f"INDICES {otext}: {dataref[0].ident} => {ilist}")
                webapi_logger.info(f"INDICES aft: {dataref[0].ident} => {meta.indices}")
            else:
                if dataref.is_array:
                    logger.debug(f"dataref {dataref.name}: collecting whole array")
                drefs.append({REST_KW.IDENT.value: dataref.ident})
        if len(datarefs) > 0:
            mapping = {}
            for d in datarefs.values():
                if type(d) is list:
                    for d1 in d:
                        mapping[d1.ident] = d1.name
                else:
                    mapping[d.ident] = d.name
            action = "dataref_subscribe_values" if on else "dataref_unsubscribe_values"
            err = self.send({REST_KW.TYPE.value: action, REST_KW.PARAMS.value: {REST_KW.DATAREFS.value: drefs}}, mapping)
            return err != -1
        if on:
            action = "register" if on else "unregister"
            logger.warning(f"no bulk datarefs to {action}")
        return False

    # Command operations
    #
    def register_command_is_active_event(self, path: str, on: bool = True) -> int:
        cmdref = self.get_command_meta_by_name(path)
        if cmdref is not None:
            mapping = {cmdref.ident: cmdref.name}
            action = "command_subscribe_is_active" if on else "command_unsubscribe_is_active"
            return self.send({REST_KW.TYPE.value: action, REST_KW.PARAMS.value: {REST_KW.COMMANDS.value: [{REST_KW.IDENT.value: cmdref.ident}]}}, mapping)
        logger.warning(f"command {path} not found in X-Plane commands database")
        return -1

    def register_bulk_command_is_active_event(self, paths, on: bool = True) -> int:
        cmds = []
        mapping = {}
        for path in paths:
            cmdref = self.get_command_meta_by_name(path=path)
            if cmdref is None:
                logger.warning(f"command {path} not found in X-Plane commands database")
                continue
            cmds.append({REST_KW.IDENT.value: cmdref.ident})
            mapping[cmdref.ident] = cmdref.name

        if len(cmds) > 0:
            action = "command_subscribe_is_active" if on else "command_unsubscribe_is_active"
            return self.send({REST_KW.TYPE.value: action, REST_KW.PARAMS.value: {REST_KW.COMMANDS.value: cmds}}, mapping)
        if on:
            action = "register" if on else "unregister"
            logger.warning(f"no bulk command active to {action}")
        return -1

    def set_command_is_active_with_duration(self, path: str, duration: float = 0.0) -> int:
        cmdref = self.get_command_meta_by_name(path)
        if cmdref is not None:
            return self.send(
                {
                    REST_KW.TYPE.value: "command_set_is_active",
                    REST_KW.PARAMS.value: {
                        REST_KW.COMMANDS.value: [{REST_KW.IDENT.value: cmdref.ident, REST_KW.ISACTIVE.value: True, REST_KW.DURATION.value: duration}]
                    },
                }
            )
        logger.warning(f"command {path} not found in X-Plane commands database")
        return -1

    def set_command_is_active_without_duration(self, path: str, active: bool) -> int:
        cmdref = self.get_command_meta_by_name(path)
        if cmdref is not None:
            return self.send(
                {
                    REST_KW.TYPE.value: "command_set_is_active",
                    REST_KW.PARAMS.value: {REST_KW.COMMANDS.value: [{REST_KW.IDENT.value: cmdref.ident, REST_KW.ISACTIVE.value: active}]},
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

        self._dataref_by_id = {}  # {dataref-id: Dataref}
        self._max_datarefs_monitored = 0  # max(len(self._dataref_by_id))

        self.cmdevents = set()  # list of command active events currently monitored
        self._max_events_monitored = 0

        self._permanent_observables: List[Observable] = []  # cannot create them now since XPlane does not exist yet (subclasses of Observable)
        self._observables: Observables | None = None  # local config observables for this simulator <sim>/resources/observables.yaml

        self.xp_home = environ.get(ENVIRON_KW.SIMULATOR_HOME.value)
        self.api_host = environ.get(ENVIRON_KW.API_HOST.value, "127.0.0.1")
        self.api_port = environ.get(ENVIRON_KW.API_PORT.value, 8086)
        self.api_path = environ.get(ENVIRON_KW.API_PATH.value, "/api")
        self.api_version = environ.get(ENVIRON_KW.API_VERSION.value, "v1")

        self.ws_thread: threading.Thread | None = None

        Simulator.__init__(self, cockpit=cockpit, environ=environ)
        XPlaneWebSocket.__init__(self, host=self.api_host[0], port=self.api_host[1], api=self.api_path, api_version=self.api_version)
        SimulatorVariableListener.__init__(self, name=self.name)
        self.cockpit.set_logging_level(__name__)

        self.ws_event = threading.Event()
        self.ws_event.set()  # means it is off

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

    def rebuild_dataref_ids(self):
        if self.all_datarefs.has_data and len(self._dataref_by_id) > 0:
            self._dataref_by_id = {d.ident: d for d in self._dataref_by_id}
            logger.info("dataref ids rebuilt")
            return
        logger.warning("no data to rebuild dataref ids")

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
        aircraft_vars = self.cockpit.aircraft.get_variables()
        if len(aircraft_vars) > 0:
            ret = ret | aircraft_vars
        return ret

    def simulator_variable_changed(self, data: SimulatorVariable):
        pass

    #
    # Events
    def get_activities(self) -> set:
        """Returns the list of commands for which cockpitdecks wants to be notified of activation."""
        ret = set(PERMANENT_SIMULATOR_EVENTS)
        for obs in self.observables:
            ret = ret | obs.get_activities()
        # Add cockpit's, which includes aircraft's
        more = self.cockpit.get_activities()
        if len(more) > 0:
            ret = ret | more
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
            d.monitor()
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
                if not d.unmonitor():  # will be decreased by 1 in super().remove_simulator_variable_to_monitor()
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
                    d.monitor()  # increases counter
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

    #
    # Event management
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
                return [round(v, local_r) for v in local_value]
            return local_value

        logger.info("starting websocket listener..")
        RECEIVE_TIMEOUT = 1  # when not connected, checks often
        total_reads = 0
        to_count = 0
        TO_COUNT_SPAM = 10
        TO_COUNT_INFO = 60  # ~ 1 min.
        start_time = datetime.now()
        last_read_ts = start_time
        total_read_time = 0.0
        self.set_internal_variable(name=COCKPITDECKS_INTVAR.INTDREF_CONNECTION_STATUS.value, value=3, cascade=True)
        while not self.ws_event.is_set():
            try:
                message = self.ws.receive(timeout=RECEIVE_TIMEOUT)
                if message is None:
                    if total_reads == 0:
                        if to_count % TO_COUNT_INFO == 0:
                            logger.info("..waiting for data from simulator..")  # at {datetime.now()}")
                        elif to_count % TO_COUNT_SPAM == 0:
                            logger.log(SPAM_LEVEL, "..waiting for data from simulator..")  # at {datetime.now()}")
                    to_count = to_count + 1
                    continue

                now = datetime.now()
                if total_reads == 0:
                    logger.log(logging.INFO, f"..first message after {(now - start_time).seconds} secs..")  # SPAM_LEVEL
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
                        if REST_KW.DATA.value not in data:
                            logger.warning(f"no data: {data}")
                            continue

                        for ident, value in data[REST_KW.DATA.value].items():
                            meta = self.get_command_meta_by_id(int(ident))
                            if meta is not None:
                                webapi_logger.info(f"CMD : {meta.name}={value}")
                                e = CommandActiveEvent(sim=self, command=meta.name, is_active=value, cascade=True)
                                self.inc(COCKPITDECKS_INTVAR.COMMAND_ACTIVE_ENQUEUED.value)
                            else:
                                logger.warning(f"no command for id={self.all_commands.equiv(ident=int(ident))}")
                    #
                    #
                    elif resp_type == REST_RESPONSE.DATAREF_UPDATE.value:

                        self.inc(COCKPITDECKS_INTVAR.WS_VUPD_RECV.value)
                        if REST_KW.DATA.value not in data:
                            logger.warning(f"no data: {data}")
                            continue

                        for ident, value in data[REST_KW.DATA.value].items():
                            ident = int(ident)
                            dataref = self._dataref_by_id.get(ident)
                            if dataref is None:
                                logger.debug(
                                    f"no dataref for id={self.all_datarefs.equiv(ident=int(ident))} (this may be a previously requested dataref arriving late..., safely ignore)"
                                )
                                continue

                            if type(dataref) is list:
                                #
                                # 1. One or more values from a dataref array (but not all values)
                                if type(value) is not list:
                                    logger.warning(f"dataref array {self.all_datarefs.equiv(ident=ident)} value is not a list ({value}, {type(value)})")
                                    continue
                                meta = dataref[0].meta
                                if meta is None:
                                    logger.warning(f"dataref array {self.all_datarefs.equiv(ident=ident)} meta data not found")
                                    continue
                                current_indices = meta.indices
                                if len(value) != len(current_indices):
                                    logger.warning(
                                        f"dataref array {self.all_datarefs.equiv(ident=ident)}: size mismatch ({len(value)} vs {len(current_indices)})"
                                    )
                                    logger.warning(f"dataref array {self.all_datarefs.equiv(ident=ident)}: value: {value}, indices: {current_indices})")
                                    # So! since we totally missed this set of data, we ask for the set again to refresh the data:
                                    # err = self.send({REST_KW.TYPE.value: "dataref_subscribe_values", REST_KW.PARAMS.value: {REST_KW.DATAREFS.value: meta.indices}}, {})
                                    last_indices = meta.last_indices()
                                    if len(value) != len(last_indices):
                                        logger.warning("no attempt with previously requested indices, no match")
                                        continue
                                    else:
                                        logger.warning("attempt with previously requested indices (we have a match)..")
                                        logger.warning(f"dataref array: current value: {value}, previous indices: {last_indices})")
                                        current_indices = last_indices

                                for idx, v1 in zip(current_indices, value):
                                    d1 = f"{meta.name}[{idx}]"
                                    cascade = d1 in self.simulator_variable_to_monitor
                                    webapi_logger.info(f"DREF ARRAY: {d1} = {v1} (cascade={cascade})")
                                    e = DatarefEvent(sim=self, dataref=d1, value=v1, cascade=cascade)
                                    self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)
                                # alternative:
                                # for d in dataref:
                                #     parsed_value = d.parse_raw_value(value)
                                #     cascade = d.name in self.simulator_variable_to_monitor
                                #     webapi_logger.info(f"DREF: {d.name} = {parsed_value} (cascade={cascade})")  # webapi_logger.info
                                #     e = DatarefEvent(sim=self, dataref=d.name, value=parsed_value, cascade=cascade)  # send raw value if possible
                                #     self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)
                            else:
                                #
                                # 2. Scalar value
                                parsed_value = dataref.parse_raw_value(value)
                                cascade = dataref.name in self.simulator_variable_to_monitor
                                if dataref.name not in BLACK_LIST:  # changes too often, clutters web api log.
                                    webapi_logger.info(f"DREF: {dataref.name} = {parsed_value} (cascade={cascade})")  # webapi_logger.info
                                e = DatarefEvent(sim=self, dataref=dataref.name, value=parsed_value, cascade=cascade)  # send raw value if possible
                                self.inc(COCKPITDECKS_INTVAR.UPDATE_ENQUEUED.value)
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

        if self.ws_event.is_set():  # Thread for X-Plane datarefs
            self.ws_event.clear()
            self.ws_thread = threading.Thread(target=self.ws_receiver, name="XPlane::WebSocket Listener")
            self.ws_thread.start()
            logger.info("websocket listener started")
        else:
            logger.info("websocket listener already running.")

        # When restarted after network failure, should clean all datarefs
        # then reload datarefs from current page of each deck
        self.reload_caches()
        self.rebuild_dataref_ids()
        self.clean_simulator_variables_to_monitor()
        self.add_all_simulator_variables_to_monitor()
        self.clean_simulator_events_to_monitor()
        self.add_all_simulator_events_to_monitor()
        logger.info("request to reload pages")
        self.cockpit.reload_pages()  # to request page variables and take into account updated values
        logger.info(f"{self.name} started")

    def stop(self):
        if not self.ws_event.is_set():
            # if self.all_datarefs is not None:
            #     self.all_datarefs.save("datarefs.json")
            # if self.all_commands is not None:
            #     self.all_commands.save("commands.json")
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
        logger.info("..terminated")


## #################################################@
#
# Simulatior Information Structure
#
PERMANENT_SIMULATION_VARIABLE_NAMES = set()


class Simulation(SimulatorVariableListener):
    """Information container for some variables
    Variables are filled if available, which is not always the case...
    """

    def __init__(self, owner) -> None:
        SimulatorVariableListener.__init__(self, name=type(self).__name__)
        self.owner = owner
        self._permanent_variable_names = PERMANENT_SIMULATION_VARIABLE_NAMES
        self._permanent_variables = {}

    def init(self):
        for v in self._permanent_variable_names:
            intvar = self.owner.get_variable(name=SimulatorVariable.internal_variable_name(v), factory=self)
            intvar.add_listener(self)
            self._permanent_variables[v] = intvar
        logger.info(f"permanent variables: {', '.join([SimulatorVariable.internal_variable_root_name(v) for v in self._permanent_variables.keys()])}")

    def simulator_variable_changed(self, data: SimulatorVariable):
        """
        This gets called when dataref AIRCRAFT_CHANGE_MONITORING_DATAREF is changed, hence a new aircraft has been loaded.
        """
        name = data.name
        if SimulatorVariable.is_internal_variable(name):
            name = SimulatorVariable.internal_variable_root_name(name)
        if name not in self._permanent_variables:
            logger.warning(f"{data.name}({type(data)})={data.value} unhandled")
            return


#
