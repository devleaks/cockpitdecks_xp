# Class for interface with X-Plane using UDP protocol.
#
from __future__ import annotations

import os
import socket
import struct
import binascii
import platform
import threading
import logging
import time
import json
import base64

import requests

from datetime import datetime, timedelta, timezone

from cockpitdecks_xp import __version__
from cockpitdecks import SPAM_LEVEL, CONFIG_KW, DEFAULT_FREQUENCY, MONITOR_DATAREF_USAGE
from cockpitdecks.data import InternalData
from cockpitdecks.simulator import Simulator, SimulatorEvent, SimulatorInstruction, SimulatorMacroInstruction
from cockpitdecks.simulator import SimulatorData, SimulatorDataListener, SimulatorDataConsumer
from cockpitdecks.resources.intdatarefs import INTERNAL_DATAREF

logger = logging.getLogger(__name__)
# logger.setLevel(SPAM_LEVEL)  # To see which dataref are requested
# logger.setLevel(logging.DEBUG)


# #############################################
# DATAREF
#
# A velue in X-Plane Simulator

# REST API model keywords
REST_DATA = "data"
REST_IDENT = "id"


class XPlaneData(SimulatorData):

    def __init__(self, path: str, is_string: bool = False):
        # Data
        SimulatorData.__init__(self, name=path, data_type="string" if is_string else "float")

    @classmethod
    def new(cls, name, **kwargs):
        is_string = kwargs.get("is_string", False)
        is_internal = kwargs.get("is_internal", False)

        if is_internal or SimulatorData.is_internal_data(name):
            return InternalData(name=name, is_string=is_string)

        return Dataref(path=name, is_string=is_string)


class Dataref(SimulatorData):
    """
    A Dataref is an internal value of the simulation software made accessible to outside modules,
    plugins, or other software in general.

    From Spring 2024 on, this is no longer inspired from Sandy Barbour and the like in Python 2.
    Most of the original code has been removed, because unused.
    This is a modern implementation, specific to Cockpitdecks. It even use X-Plane 12.1 REST/WebSocket API.
    """

    DEFAULT_REQ_FREQUENCY = DEFAULT_FREQUENCY

    def __init__(self, path: str, is_string: bool = False):
        # Data
        SimulatorData.__init__(self, name=path, data_type="string" if is_string else "float")

        self.dataref = path  # some/path/values
        self.index = 0  # 6
        if "[" in path:  # sim/some/values[4]
            self.dataref = self.name[: self.name.find("[")]
            self.index = int(self.name[self.name.find("[") + 1 : self.name.find("]")])

        self._xpindex = None
        self._req_id = 0

    # @property
    # def path(self) -> str:
    #     return self.name

    def save(self) -> bool:
        if self._writable:
            if not self.is_internal:
                return self._sim.write_dataref(dataref=self.name, value=self.value())
        else:
            logger.warning(f"{self.dataref} not writable")
        return False

    # ##############
    # REST API INTERFACE
    #
    # NOT GENERIC ONLY WORKS FOR SCALAR VALUES, NOT ARRAYS
    #
    def get_specs(self, simulator: Simulator) -> dict | None:
        api_url = simulator.api_url
        if api_url is None:
            logger.warning("no api url")
            return None
        payload = {"filter[name]": self.name}
        api_url = f"{api_url}/datarefs"
        try:
            response = requests.get(api_url, params=payload)
            resp = response.json()
            if REST_DATA in resp:
                return resp[REST_DATA][0]
            else:
                logger.error(resp)
        except:
            logger.error(f"no connection to {api_url}")
        return None

    def get_index(self, simulator: Simulator) -> int | None:
        if self._xpindex is not None:
            return self._xpindex
        data = self.get_specs(simulator=simulator)
        if data is not None and REST_IDENT in data:
            self._xpindex = int(data[REST_IDENT])
            return self._xpindex
        logger.error(f"could not get dataref specifications for {self.name} ({data})")
        return None

    def get_value(self, simulator: Simulator):
        api_url = simulator.api_url
        if api_url is None:
            logger.warning("no api url")
            return None
        if self._xpindex is None:
            idx = self.get_index(simulator=simulator)
            if idx is None:
                logger.error("could not get XP index")
                return None
        url = f"{api_url}/datarefs/{self._xpindex}/value"
        try:
            response = requests.get(url)
            data = response.json()
            if REST_DATA in data:
                # print(">>>>>>>", self.name, self.is_string)
                if self.is_string:
                    if type(data[REST_DATA]) in [str, bytes]:
                        return base64.b64decode(data[REST_DATA])[:-1].decode("ascii")
                    else:
                        logger.warning(f"value for {self.name} ({data}) is not a string")
                return data[REST_DATA]
        except:
            logger.error(f"could not get value for {self.name} ({data})")
        return None

    def set_value(self, simulator: Simulator):
        api_url = simulator.api_url
        if api_url is None:
            logger.warning("no api url")
            return None
        if self._xpindex is None:
            idx = self.get_index(simulator=simulator)
            if idx is None:
                logger.error("could not get XP index")
                return None
        url = f"{api_url}/datarefs/{self._xpindex}/value"
        value = self.current_value
        if value is not None and (self.is_string):
            value = base64.b64encode(bytes(str(self.current_value), "ascii")).decode("ascii")
        data = {"data": value}
        response = requests.patch(url=url, data=data)
        if response.status_code != 200:
            logger.error(f"could not set value for {self.name} ({data}, {response})")

    def ws_subscribe(self, ws):
        self._req_id = randint(100000, 1000000)
        request = {"req_id": self._req_id, "type": "dataref_subscribe_values", "params": {"datarefs": [{"id": self._xpindex}]}}
        ws.send(json.dumps(request))

    def ws_unsubscribe(self, ws):
        request = {"req_id": self._req_id, "type": "dataref_unsubscribe_values", "params": {"datarefs": [{"id": self._xpindex}]}}
        ws.send(json.dumps(request))

    def ws_callback(self, response) -> bool:
        # gets called by websocket onmessage on receipt.
        # 1. Ignore response with result unless error
        # 2. Get data
        # 3. Cascade if changed
        if "req_id" in response:
            if response.get("req_id") != self._req_id:
                return False
        # do something
        return True

    def ws_update(self, ws):
        request = {"req_id": 1, "type": "dataref_set_values", "params": {"datarefs": [{"id": self._xpindex, "value": self.current_value}]}}
        ws.send(json.dumps(request))

    def auto_collect(self, simulator: Simulator):
        if self.collector is None:
            e = DatarefEvent(sim=self, dataref=self.name, value=self.get_value(simulator=simulator), cascade=True)
            self.collector = threading.Timer(self.update_frequency, self.auto_collect, args=[simulator])

    def cancel_autocollect(self):
        if self.collector is not None:
            self.collector.cancel()
            self.collector = None


class StringDataref(Dataref):

    def __init__(self, path: str):
        Dataref.__init__(self, path=path, is_string=True)


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
        return super().info() | {"path": self.dataref_path, "value": self.value, "cascade": self.cascade}

    def run(self, just_do_it: bool = False) -> bool:
        if just_do_it:
            if self.sim is None:
                logger.warning("no simulator")
                return False
            dataref = self.sim.all_simulator_data.get(self.dataref_path)
            if dataref is None:
                logger.debug(f"dataref {self.dataref_path} not found in database")
                return

            try:
                logger.debug(f"updating {dataref.name}..")
                self.handling()
                dataref.update_value(self.value, cascade=self.cascade)
                self.handled()
                logger.debug(f"..updated")
            except:
                logger.warning(f"..updated with error", exc_info=True)
                return False
        else:
            self.enqueue()
            logger.debug(f"enqueued")
        return True


# #############################################
# COMMANDS
#
# The command keywords are not executed, ignored with a warning
NOT_A_COMMAND = [
    "none",
    "noop",
    "no-operation",
    "no-command",
    "do-nothing",
]  # all forced to lower cases


class XPlaneInstruction(SimulatorInstruction):
    """An Instruction is sent to the Simulator to execute some action.

    [description]
    """

    def __init__(self, name: str, simulator: XPlane, delay: float = 0.0, condition: str | None = None, button=None) -> None:
        SimulatorInstruction.__init__(self, name=name, simulator=simulator, delay=delay, condition=condition)

    @classmethod
    def new(cls, name: str, simulator: XPlane, **kwargs):
        for keyw in ["view", "command"]:
            if keyw in kwargs:
                cmdargs = kwargs.get(keyw)
                if type(cmdargs) is str:
                    if kwargs.get("longpress", False):
                        return BeginEndCommand(name=name, simulator=simulator, path=cmdargs, delay=kwargs.get("delay", 0.0), condition=kwargs.get("condition"))
                    else:
                        return Command(name=name, simulator=simulator, path=cmdargs, delay=kwargs.get("delay", 0.0), condition=kwargs.get("condition"))
                elif type(cmdargs) in [list, tuple]:
                    return SimulatorMacroInstruction(name=name, simulator=simulator, instructions=cmdargs)
        if "set_dataref" in kwargs:
            cmdargs = kwargs.get("set_dataref")
            if type(cmdargs) is str:
                return SetDataref(
                    name=name,
                    simulator=simulator,
                    path=cmdargs,
                    value=kwargs.get("value"),
                    formula=kwargs.get("formula"),
                    delay=kwargs.get("delay"),
                    condition=kwargs.get("condition"),
                )
        else:
            if not kwargs.get("silence", False):
                logger.warning(f"Instruction {name}: invalid argument {kwargs}")
        return None


class Command(XPlaneInstruction):
    """
    A Button activation will instruct the simulator software to perform an action.
    A Command is the message that the simulation sofware is expecting to perform that action.
    """

    def __init__(self, simulator: XPlane, path: str | None, name: str | None = None, delay: float = 0.0, condition: str | None = None):
        XPlaneInstruction.__init__(self, name=name, simulator=simulator, delay=delay, condition=condition)
        self.path = path  # some/command

    def __str__(self) -> str:
        return self.name + ":" + self.path if self.name is not None else (self.path if self.path is not None else "no command")

    def is_valid(self) -> bool:
        return self.path is not None and not self.path.lower() in NOT_A_COMMAND

    def _execute(self):
        self.simulator.execute_command(command=self)  # does not exist...
        self.clean_timer()


class BeginEndCommand(Command):
    """
    A Button activation will instruct the simulator software to perform an action.
    A Command is the message that the simulation sofware is expecting to perform that action.
    """

    def __init__(self, simulator: XPlane, path: str | None, name: str | None = None, delay: float = 0.0, condition: str | None = None):
        Command.__init__(self, simulator=simulator, path=path, name=name, delay=0.0, condition=condition)  # force no delay for commandBegin/End
        self.is_on = False

    def _execute(self):
        if self.is_on:
            self._simulator.command_end(command=self)
            self.is_on = False
        else:
            self._simulator.command_begin(command=self)
            self.is_on = True
        self.clean_timer()


class SetDataref(XPlaneInstruction):
    """
    A Button activation will instruct the simulator software to perform an action.
    A Command is the message that the simulation sofware is expecting to perform that action.
    """

    def __init__(self, simulator: XPlane, path: str, value=None, formula: str | None = None, delay: float = 0.0, condition: str | None = None):
        XPlaneInstruction.__init__(self, name=path, simulator=simulator, delay=delay, condition=condition)
        self.path = path  # some/command
        self.formula = None  # = formula: later, a set-dataref specific formula, different from the button one?
        self._value = value
        self._button = None

    def __str__(self) -> str:
        return "set-dataref: " + self.name

    @property
    def button(self):
        return self._button

    @button.setter
    def button(self, button):
        self._button = button

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def compute_value(self):
        if self._button is not None:
            return self._button.value.execute_formula(self.formula)
        logger.warning(f"SetDataref {self.name}: no button")
        return None

    def _execute(self):
        if self.formula is not None:
            self._value = self.compute_value()
        self._simulator.write_dataref(dataref=self.path, value=self.value)


# #############################################
# SIMULATOR
#
# A velue in X-Plane Simulatot

# Data too delicate to be put in constant.py
# !! adjust with care !!
# UDP sends at most ~40 to ~50 dataref values per packet.
LOOP_ALIVE = 100  # report loop activity every 1000 executions on DEBUG, set to None to suppress output
RECONNECT_TIMEOUT = 10  # seconds
SOCKET_TIMEOUT = 5  # seconds
MAX_TIMEOUT_COUNT = 5  # after x timeouts, assumes connection lost, disconnect, and restart later
MAX_DREF_COUNT = 80  # Maximum number of dataref that can be requested to X-Plane, CTD around ~100 datarefs

# String dataref listener
ANY = "0.0.0.0"
SDL_MCAST_PORT = 49505
SDL_MCAST_GRP = "239.255.1.1"

SDL_UPDATE_FREQ = 5.0  # same starting value as PI_string_datarefs_udp.FREQUENCY  (= 5.0 default)
SDL_SOCKET_TIMEOUT = SDL_UPDATE_FREQ + 1.0  # should be larger or equal to PI_string_datarefs_udp.FREQUENCY

XP_MIN_VERSION = 121100

# When this dataref changes, the loaded aircraft has changed
#
DATETIME_DATAREFS = [
    "sim/time/local_date_days",
    "sim/time/local_time_sec",
    "sim/time/zulu_time_sec",
    "sim/time/use_system_time",
]
REPLAY_DATAREFS = [
    "sim/time/is_in_replay",
    "sim/time/sim_speed",
    "sim/time/sim_speed_actual",
]

PERMANENT_SIMULATOR_DATA = DATETIME_DATAREFS + REPLAY_DATAREFS

INTDREF_CONNECTION_STATUS = "_connection_status"
# Status value:
# 0: Nothing running
# 1: Connection monitor running
# 2: Connection to X-Plane but no more
# 3: UDP listener running (no timeout)
# 4: Event forwarder running


# XPlaneBeacon
# Beacon-specific error classes
class XPlaneIpNotFound(Exception):
    args = "Could not find any running XPlane instance in network."


class XPlaneVersionNotSupported(Exception):
    args = "XPlane version not supported."


class XPlaneBeacon:
    """
    Get data from XPlane via network.
    Use a class to implement RAI Pattern for the UDP socket.
    """

    # constants
    MCAST_GRP = "239.255.1.1"
    MCAST_PORT = 49707  # (MCAST_PORT was 49000 for XPlane10)
    BEACON_TIMEOUT = 3.0  # seconds

    def __init__(self):
        # Open a UDP Socket to receive on Port 49000
        self.socket = None

        hostname = socket.gethostname()
        self.local_ip = socket.gethostbyname(hostname)

        self.beacon_data = {}

        self.should_not_connect = None  # threading.Event()
        self.connect_thread = None  # threading.Thread()

    @property
    def connected(self):
        return "IP" in self.beacon_data.keys()

    def FindIp(self):
        """
        Find the IP of XPlane Host in Network.
        It takes the first one it can find.
        """
        if self.socket is not None:
            self.socket.close()
            self.socket = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.beacon_data = {}

        # open socket for multicast group.
        # this socker is for getting the beacon, it can be closed when beacon is found.
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)  # SO_REUSEPORT?
        if platform.system() == "Windows":
            sock.bind(("", self.MCAST_PORT))
        else:
            sock.bind((self.MCAST_GRP, self.MCAST_PORT))
        mreq = struct.pack("=4sl", socket.inet_aton(self.MCAST_GRP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(XPlaneBeacon.BEACON_TIMEOUT)

        # receive data
        try:
            packet, sender = sock.recvfrom(1472)
            logger.debug(f"XPlane Beacon: {packet.hex()}")
            self.inc(INTERNAL_DATAREF.UDP_BEACON_RCV.value)

            # decode data
            # * Header
            header = packet[0:5]
            if header != b"BECN\x00":
                logger.warning(f"Unknown packet from {sender[0]}, {str(len(packet))} bytes:")
                logger.warning(packet)
                logger.warning(binascii.hexlify(packet))

            else:
                # * Data
                data = packet[5:21]
                # struct becn_struct
                # {
                #   uchar beacon_major_version;     // 1 at the time of X-Plane 10.40
                #   uchar beacon_minor_version;     // 1 at the time of X-Plane 10.40
                #   xint application_host_id;       // 1 for X-Plane, 2 for PlaneMaker
                #   xint version_number;            // 104014 for X-Plane 10.40b14
                #   uint role;                      // 1 for master, 2 for extern visual, 3 for IOS
                #   ushort port;                    // port number X-Plane is listening on
                #   xchr    computer_name[strDIM];  // the hostname of the computer
                # };
                beacon_major_version = 0
                beacon_minor_version = 0
                application_host_id = 0
                xplane_version_number = 0
                role = 0
                port = 0
                (
                    beacon_major_version,  # 1 at the time of X-Plane 10.40
                    beacon_minor_version,  # 1 at the time of X-Plane 10.40
                    application_host_id,  # 1 for X-Plane, 2 for PlaneMaker
                    xplane_version_number,  # 104014 for X-Plane 10.40b14
                    role,  # 1 for master, 2 for extern visual, 3 for IOS
                    port,  # port number X-Plane is listening on
                ) = struct.unpack("<BBiiIH", data)
                hostname = packet[21:-1]  # the hostname of the computer
                hostname = hostname[0 : hostname.find(0)]
                if beacon_major_version == 1 and beacon_minor_version <= 2 and application_host_id == 1:
                    self.beacon_data["IP"] = sender[0]
                    self.beacon_data["Port"] = port
                    self.beacon_data["hostname"] = hostname.decode()
                    self.beacon_data["XPlaneVersion"] = xplane_version_number
                    self.beacon_data["role"] = role
                    logger.info(f"XPlane Beacon Version: {beacon_major_version}.{beacon_minor_version}.{application_host_id}")
                    #
                    s = "does not appear"
                    if self.runs_locally():
                        s = "appears"
                        if self.xp_home is not None and os.path.isdir(self.xp_home):
                            logger.info(f"XPlane home directory {self.xp_home}")
                    logger.info(f"XPlane {s} to run locally ({self.local_ip}/{self.beacon_data['IP']})")
                    if self.runs_locally() and self.xp_home is not None and os.path.isdir(self.xp_home):
                        logger.info(f"XPlane home directory {self.xp_home}")
                    #
                else:
                    logger.warning(f"XPlane Beacon Version not supported: {beacon_major_version}.{beacon_minor_version}.{application_host_id}")
                    raise XPlaneVersionNotSupported()

        except socket.timeout:
            logger.debug("XPlane IP not found.")
            self.inc(INTERNAL_DATAREF.UDP_BEACON_TIMEOUT.value)
            raise XPlaneIpNotFound()
        finally:
            sock.close()

        return self.beacon_data

    def start(self):
        logger.warning("nothing to start")

    def stop(self):
        logger.warning("nothing to stop")

    def cleanup(self):
        logger.warning("nothing to clean up")

    def connect_loop(self):
        """
        Trys to connect to X-Plane indefinitely until self.should_not_connect is set.
        If a connection fails, drops, disappears, will try periodically to restore it.
        """
        logger.debug("starting..")
        WARN_FREQ = 10
        cnt = 0
        while self.should_not_connect is not None and not self.should_not_connect.is_set():
            if not self.connected:
                try:
                    self.FindIp()
                    if self.connected:
                        logger.info(f"beacon: {self.beacon_data}")
                        if "XPlaneVersion" in self.beacon_data:
                            curr = self.beacon_data["XPlaneVersion"]
                            if curr < XP_MIN_VERSION:
                                logger.warning(f"X-Plane version {curr} detected, minimal version is {XP_MIN_VERSION}")
                                logger.warning(f"Some features in Cockpitdecks may not work properly")
                            else:
                                logger.info(f"X-Plane version {curr} meets minima (>={XP_MIN_VERSION})")
                        logger.debug("..connected, starting dataref listener..")
                        self.start()
                        self.inc(INTERNAL_DATAREF.STARTS.value)
                        logger.info("..dataref listener started..")
                except XPlaneVersionNotSupported:
                    self.beacon_data = {}
                    logger.error("..X-Plane Version not supported..")
                except XPlaneIpNotFound:
                    self.beacon_data = {}
                    if cnt % WARN_FREQ == 0:
                        logger.error(f"..X-Plane instance not found on local network.. ({datetime.now().strftime('%H:%M:%S')})")
                    cnt = cnt + 1
                if not self.connected:
                    self.should_not_connect.wait(RECONNECT_TIMEOUT)
                    logger.debug("..trying..")
            else:
                self.should_not_connect.wait(RECONNECT_TIMEOUT)  # could be n * RECONNECT_TIMEOUT
                logger.debug("..monitoring connection..")
        logger.debug("..ended")

    # ################################
    # Interface
    #
    def connect(self):
        """
        Starts connect loop.
        """
        if self.should_not_connect is None:
            self.should_not_connect = threading.Event()
            self.connect_thread = threading.Thread(target=self.connect_loop, name="XPlaneBeacon::connect_loop")
            self.connect_thread.start()
            logger.debug("connect_loop started")
        else:
            logger.debug("connect_loop already started")

    def disconnect(self):
        """
        End connect loop and disconnect
        """
        if self.should_not_connect is not None:
            logger.debug("disconnecting..")
            self.cleanup()
            self.beacon_data = {}
            self.should_not_connect.set()
            wait = RECONNECT_TIMEOUT
            logger.debug(f"..asked to stop connect_loop.. (this may last {wait} secs.)")
            self.connect_thread.join(timeout=wait)
            if self.connect_thread.is_alive():
                logger.warning(f"..thread may hang..")
            self.should_not_connect = None
            logger.debug("..disconnected")
        else:
            if self.connected:
                self.beacon_data = {}
                logger.debug("..connect_loop not running..disconnected")
            else:
                logger.debug("..not connected")


class XPlane(Simulator, SimulatorDataListener, SimulatorDataConsumer, XPlaneBeacon):
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
        # list of requested datarefs with index number
        self.datarefidx = 0
        self.datarefs = {}  # key = idx, value = dataref path
        self._max_monitored = 0

        self.udp_event = None  # thread to read X-Plane UDP port for datarefs
        self.udp_thread = None
        self._dref_cache = {}

        self.dref_event = None  # thread to read XPPython3 PI_string_datarefs_udp alternate UDP port for string datarefs
        self.dref_thread = None
        self.dref_timeout = 1
        self._strdref_cache = {}

        self.xp_home = environ.get("XP_HOME")
        self.api_host = environ.get("API_HOST")
        self.api_port = environ.get("API_PORT")
        self.api_path = environ.get("API_PATH")

        Simulator.__init__(self, cockpit=cockpit, environ=environ)
        self.name = XPlane.name
        self.cockpit.set_logging_level(__name__)

        XPlaneBeacon.__init__(self)

        self.socket_strdref = None

        self.init()

    def init(self):
        if self._inited:
            return

        self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=0, cascade=True)

        # Setup socket reception for string-datarefs
        self.socket_strdref = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        # Allow multiple sockets to use the same PORT number
        self.socket_strdref.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)  # SO_REUSEPORT

        self.socket_strdref.bind((ANY, SDL_MCAST_PORT))
        # Tell the kernel that we want to add ourselves to a multicast group
        # The address for the multicast group is the third param
        status = self.socket_strdref.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton(SDL_MCAST_GRP) + socket.inet_aton(ANY),
        )

        self._inited = True

    def __del__(self):
        if not self._inited:
            return
        for i in range(len(self.datarefs)):
            self.add_dataref_to_monitor(next(iter(self.datarefs.values())), freq=0)
        self.disconnect()

    def get_version(self) -> list:
        return [f"{type(self).__name__} {__version__}"]

    # ################################
    # Factories
    #
    def instruction_factory(self, name, **kwargs):
        logger.debug(f"creating xplane instruction {name}")
        return XPlaneInstruction.new(name=name, simulator=self, **kwargs)

    def simulator_data_factory(self, name: str, data_type: str = "float", physical_unit: str = "") -> SimulatorData:
        logger.debug(f"creating xplane data {name}")
        return self.get_data(path=name, is_string=data_type in ["string", "str", str])

    def replay_event_factory(self, name: str, value):
        logger.debug(f"creating replay event {name}")
        return DatarefEvent(sim=self, dataref=name, value=value, cascade=True, autorun=False)

    # ################################
    # Others
    #
    @property
    def api_url(self) -> str | None:
        if self.connected:
            if self.api_path is None or self.api_port is None:
                logger.debug("no api connection information provided")
                return None
            host = self.api_host
            if host is None:
                host = self.beacon_data["IP"]
            url = f"http://{host}:{self.api_port}{self.api_path}"
            logger.debug(f"api reachable at {url}")
            return url
        logger.debug("no connection")
        return None

    def runs_locally(self) -> bool:
        if self.connected:
            logger.debug(f"local ip {self.local_ip} vs beacon {self.beacon_data['IP']}")
        else:
            logger.debug(f"local ip {self.local_ip} but not connected to X-Plane")
        return False if not self.connected else self.local_ip == self.beacon_data["IP"]

    #
    # Datarefs
    def get_simulator_data(self) -> set:
        """Returns the list of datarefs for which the xplane simulator wants to be notified."""
        ret = set(PERMANENT_SIMULATOR_DATA)
        return ret

    def simulator_data_changed(self, data: SimulatorData):
        pass

    def add_always_monitored_datarefs(self):
        self.add_cockpit_datarefs()
        self.add_simulator_datarefs()

    def add_cockpit_datarefs(self):
        """Cockpit datarefs are always requested and used internaly by the Cockpit"""
        dtdrefs = {}
        for d in self.cockpit.get_simulator_data():
            if d.startswith(CONFIG_KW.STRING_PREFIX.value):
                d = d.replace(CONFIG_KW.STRING_PREFIX.value, "")
                dtdrefs[d] = self.get_data(d, is_string=True)
            else:
                dtdrefs[d] = self.get_data(d)
            dtdrefs[d].add_listener(self.cockpit)
        self.add_simulator_data_to_monitor(dtdrefs)
        logger.info(f"monitoring {len(dtdrefs)} cockpit datarefs")

    def add_simulator_datarefs(self):
        """Simulator datarefs are always requested and used internaly by the Simulator"""
        dtdrefs = {}
        for d in self.get_simulator_data():
            if d.startswith(CONFIG_KW.STRING_PREFIX.value):
                d = d.replace(CONFIG_KW.STRING_PREFIX.value, "")
                dtdrefs[d] = self.get_data(d, is_string=True)
            else:
                dtdrefs[d] = self.get_data(d)
            dtdrefs[d].add_listener(self)
        self.add_simulator_data_to_monitor(dtdrefs)
        logger.info(f"monitoring {len(dtdrefs)} simulator datarefs")

    def get_data(self, name: str, is_string: bool = False) -> InternalData | Dataref:
        """Returns data or create a new one, internal if path requires it"""
        if name in self.all_simulator_data.keys():
            return self.all_simulator_data[name]
        if SimulatorData.is_internal_data(path=name):
            return self.register(simulator_data=InternalData(name=name, is_string=is_string))
        return self.register(simulator_data=Dataref(path=name, is_string=is_string))

    def datetime(self, zulu: bool = False, system: bool = False) -> datetime:
        """Returns the simulator date and time"""
        if DATETIME_DATAREFS[0] not in self.all_simulator_data.keys():  # hack, means dref not created yet
            return super().datetime(zulu=zulu, system=system)
        now = datetime.now().astimezone()
        days = self.get_simulation_data_value("sim/time/local_date_days")
        secs = self.get_simulation_data_value("sim/time/local_date_sec")
        if not system and days is not None and secs is not None:
            simnow = datetime(year=now.year, month=1, day=1, hour=0, minute=0, second=0, microsecond=0).astimezone()
            simnow = simnow + timedelta(days=days) + timedelta(days=secs)
            return simnow
        return now

    #
    # Commands
    def execute_command(self, command: Command):
        if command is None:
            logger.warning(f"no command")
            return
        elif not command.is_valid():
            logger.warning(f"command '{command}' not sent (command placeholder, no command, do nothing)")
            return
        if not self.connected:
            logger.warning(f"no connection ({command})")
            return
        if command.path is not None:
            message = "CMND0" + command.path
            self.socket.sendto(message.encode(), (self.beacon_data["IP"], self.beacon_data["Port"]))
            logger.log(SPAM_LEVEL, f"execute_command: executed {command}")
        else:
            logger.warning("execute_command: no command")

    def write_dataref(self, dataref: str, value: float | int | bool, vtype: str = "float") -> bool:
        """
        Write Dataref to XPlane
        DREF0+(4byte byte value)+dref_path+0+spaces to complete the whole message to 509 bytes
        DREF0+(4byte byte value of 1)+ sim/cockpit/switches/anti_ice_surf_heat_left+0+spaces to complete to 509 bytes
        """
        path = dataref
        if Dataref.is_internal_data(path):
            d = self.get_data(path)
            d.update_value(new_value=value, cascade=True)
            logger.debug(f"written local dataref ({path}={value})")
            return False

        if not self.connected:
            logger.warning(f"no connection ({path}={value})")
            return False

        cmd = b"DREF\x00"
        path = path + "\x00"
        string = path.ljust(500).encode()
        message = "".encode()
        if vtype == "float":
            message = struct.pack("<5sf500s", cmd, value, string)
        elif vtype == "int":
            message = struct.pack("<5si500s", cmd, value, string)
        elif vtype == "bool":
            message = struct.pack("<5sI500s", cmd, int(value), string)

        assert len(message) == 509
        logger.debug(f"sending ({self.beacon_data['IP']}, {self.beacon_data['Port']}): {path}={value} ..")
        logger.log(SPAM_LEVEL, f"write_dataref: {path}={value}")
        self.socket.sendto(message, (self.beacon_data["IP"], self.beacon_data["Port"]))
        # print(">>> written", dataref, value, (self.beacon_data["IP"], self.beacon_data["Port"]))
        logger.debug(".. sent")
        return True

    def add_dataref_to_monitor(self, path, freq=None):
        """
        Configure XPlane to send the dataref with a certain frequency.
        You can disable a dataref by setting freq to 0.
        """
        if Dataref.is_internal_data(path):
            logger.debug(f"{path} is local and does not need X-Plane monitoring")
            return False

        if not self.connected:
            logger.warning(f"no connection ({path}, {freq})")
            return False

        idx = -9999
        if freq is None:
            freq = self.DEFAULT_REQ_FREQUENCY

        if path in self.datarefs.values():
            idx = list(self.datarefs.keys())[list(self.datarefs.values()).index(path)]
            if freq == 0 and idx in self.datarefs.keys():
                # logger.debug(f">>>>>>>>>>>>>> {path} DELETING INDEX {idx}")
                del self.datarefs[idx]
        else:
            if freq != 0 and len(self.datarefs) > MAX_DREF_COUNT:
                # logger.warning(f"requesting too many datarefs ({len(self.datarefs)})")
                return False

            idx = self.datarefidx
            self.datarefs[self.datarefidx] = path
            self.datarefidx += 1

        self._max_monitored = max(self._max_monitored, len(self.datarefs))

        cmd = b"RREF\x00"
        string = path.encode()
        message = struct.pack("<5sii400s", cmd, freq, idx, string)
        assert len(message) == 413
        self.socket.sendto(message, (self.beacon_data["IP"], self.beacon_data["Port"]))
        if self.datarefidx % LOOP_ALIVE == 0:
            time.sleep(0.2)
        return True

    def remove_dataref_from_monitor(self, path):
        return self.add_dataref_to_monitor(path, freq=0)

    def udp_enqueue(self):
        """Read and decode socket messages and enqueue dataref values

        Terminates after 5 timeouts.
        """
        logger.debug("starting dataref listener..")
        number_of_timeouts = 0
        total_reads = 0
        total_values = 0
        last_read_ts = datetime.now()
        total_read_time = 0.0
        self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=3, cascade=True)
        while self.udp_event is not None and not self.udp_event.is_set():
            if len(self.datarefs) > 0:
                try:
                    # Receive packet
                    self.socket.settimeout(SOCKET_TIMEOUT)
                    data, addr = self.socket.recvfrom(1472)  # maximum bytes of an RREF answer X-Plane will send (Ethernet MTU - IP hdr - UDP hdr)
                    # Decode Packet
                    self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=4, cascade=True)
                    self.inc(INTERNAL_DATAREF.UDP_READS.value)
                    # Read the Header "RREF,".
                    number_of_timeouts = 0
                    total_reads = total_reads + 1
                    now = datetime.now()
                    delta = now - last_read_ts
                    self.set_internal_data(
                        name=INTERNAL_DATAREF.LAST_READ.value,
                        value=delta.microseconds,
                        cascade=True,
                    )
                    total_read_time = total_read_time + delta.microseconds / 1000000
                    last_read_ts = now
                    header = data[0:5]
                    if header == b"RREF,":  # (was b"RREFO" for XPlane10)
                        # We get 8 bytes for every dataref sent:
                        # An integer for idx and the float value.
                        values = data[5:]
                        lenvalue = 8
                        numvalues = int(len(values) / lenvalue)
                        self.inc(INTERNAL_DATAREF.VALUES.value, amount=numvalues)
                        total_values = total_values + numvalues
                        for i in range(0, numvalues):
                            singledata = data[(5 + lenvalue * i) : (5 + lenvalue * (i + 1))]
                            (idx, value) = struct.unpack("<if", singledata)

                            d = self.datarefs.get(idx)
                            if d is not None:
                                if value < 0.0 and value > -0.001:  # convert -0.0 values to positive 0.0
                                    value = 0.0
                                if d == DATETIME_DATAREFS[2]:  # zulu secs
                                    now = datetime.now().astimezone(tz=timezone.utc)
                                    seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
                                    diff = value - seconds_since_midnight
                                    self.set_internal_data(
                                        name=INTERNAL_DATAREF.ZULU_DIFFERENCE.value,
                                        value=diff,
                                        cascade=(total_reads % 2 == 0),
                                    )

                                v = value
                                r = self.get_rounding(simulator_data_name=d)
                                if r is not None and value is not None:
                                    v = round(value, r)
                                if d not in self._dref_cache or (d in self._dref_cache and self._dref_cache[d] != v):
                                    e = DatarefEvent(
                                        sim=self,
                                        dataref=d,
                                        value=value,
                                        cascade=d in self.simulator_data_to_monitor.keys(),
                                    )
                                    self.inc(INTERNAL_DATAREF.UPDATE_ENQUEUED.value)
                                    self._dref_cache[d] = v
                            else:
                                logger.debug(f"no dataref at index {idx}, probably no longer monitored")
                    else:
                        logger.warning(f"{binascii.hexlify(data)}")
                    if total_reads % 10 == 0:
                        logger.debug(
                            f"average socket time between reads {round(total_read_time / total_reads, 3)} ({total_reads} reads; {total_values} values sent)"
                        )  # ignore
                except TimeoutError:  # socket timeout
                    number_of_timeouts = number_of_timeouts + 1
                    logger.info(f"socket timeout received ({number_of_timeouts}/{MAX_TIMEOUT_COUNT})")  # , exc_info=True
                    self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=2, cascade=True)
                    if number_of_timeouts >= MAX_TIMEOUT_COUNT:  # attemps to reconnect
                        logger.warning("too many times out, disconnecting, udp_enqueue terminated")  # ignore
                        self.beacon_data = {}
                        if self.udp_event is not None and not self.udp_event.is_set():
                            self.udp_event.set()
                        self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=1, cascade=True)
                        self.inc(INTERNAL_DATAREF.STOPS.value)
                except:
                    logger.error(f"udp_enqueue", exc_info=True)
        self.udp_event = None
        self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=2, cascade=True)
        logger.info("..dataref listener terminated")

    def strdref_enqueue(self):
        logger.info("starting string dataref listener..")
        self.dref_timeout = max(SDL_SOCKET_TIMEOUT, SDL_UPDATE_FREQ)
        total_to = 0
        tot_items = 0
        total_reads = 0
        last_read_ts = datetime.now()
        total_read_time = 0.0
        src_last_ts = 0
        src_cnt = 0
        src_tot = 0

        while self.dref_event is not None and not self.dref_event.is_set():
            try:
                self.socket_strdref.settimeout(self.dref_timeout)
                data, addr = self.socket_strdref.recvfrom(1472)
                self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=4, cascade=True)
                total_to = 0
                total_reads = total_reads + 1
                now = datetime.now()
                delta = now - last_read_ts
                total_read_time = total_read_time + delta.microseconds / 1000000
                last_read_ts = now
                logger.debug("string dataref listener: got data")
                data_decoded = data.decode("utf-8")
                data = {}
                try:  # \n({json.dumps(json.loads(data.decode('utf-8')), indent=2)})
                    data = json.loads(data_decoded)
                except:
                    logger.warning(f"string dataref listener: could not decode {data_decoded}")

                meta = data  # older version carried meta data directly in message
                if "meta" in data:  # some meta data in string values message
                    meta = data["meta"]
                    del data["meta"]

                ts = 0
                if "ts" in meta:
                    ts = meta["ts"]
                    del meta["ts"]
                    if src_last_ts > 0:
                        src_tot = src_tot + (ts - src_last_ts)
                        src_cnt = src_cnt + 1
                        self.collector_avgtime = src_tot / src_cnt
                        if src_cnt % 100 == 0:
                            logger.debug(
                                f"string dataref listener: average time between reads {round(self.collector_avgtime, 4)} ({round(tot_items/total_reads,0)})"
                            )
                    src_last_ts = ts

                freq = None
                oldf = self.dref_timeout
                if "f" in meta:
                    freq = meta["f"]
                    del meta["f"]
                    if freq is not None and (oldf != (freq + 1)):
                        self.dref_timeout = freq + 1
                        logger.info(f"string dataref listener: {len(data)} strings, adjusted frequency to {self.dref_timeout} secs")
                for k, v in data.items():  # simple cache mechanism
                    tot_items = tot_items + 1
                    if k not in self._strdref_cache or (k in self._strdref_cache and self._strdref_cache[k] != v):
                        e = DatarefEvent(sim=self, dataref=k, value=v, cascade=True)
                        self._strdref_cache[k] = v
            except TimeoutError:  # socket timeout
                total_to = total_to + 1
                logger.debug(f"string dataref listener: socket timeout ({self.dref_timeout} secs.) received ({total_to})")
                self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=2, cascade=True)
                self.dref_timeout = self.dref_timeout + 1  # may be we are too fast to ask, let's slow down a bit next time...
            except:
                logger.warning(f"strdref_enqueue", exc_info=True)

        self.dref_event = None
        # Bind to the port that we know will receive multicast data
        # self.socket_strdref.shutdown()
        # self.socket_strdref.close()
        # logger.info("..strdref socket closed..")
        self.set_internal_data(name=INTDREF_CONNECTION_STATUS, value=3, cascade=True)
        logger.info("..string dataref listener terminated")

    # ################################
    # X-Plane Interface
    #
    def command_once(self, command: Command):
        self.execute_command(command)

    def command_begin(self, command: Command):
        if command.path is not None:
            self.execute_command(Command(path=command.path + "/begin", simulator=self, name="BeginCommand:" + command.path))
        else:
            logger.warning(f"no command")

    def command_end(self, command: Command):
        if command.path is not None:
            self.execute_command(Command(path=command.path + "/end", simulator=self, name="EndCommand:" + command.path))
        else:
            logger.warning(f"no command")

    def remove_local_datarefs(self, datarefs) -> list:
        return list(filter(lambda d: not Dataref.is_internal_data(d), datarefs))

    def clean_datarefs_to_monitor(self):
        if not self.connected:
            logger.warning("no connection")
            return
        for i in range(len(self.datarefs)):
            self.add_dataref_to_monitor(next(iter(self.datarefs.values())), freq=0)
        super().clean_simulator_data_to_monitor()
        self._strdref_cache = {}
        self._dref_cache = {}
        logger.debug("done")

    def add_simulator_data_to_monitor(self, datarefs):
        if not self.connected:
            logger.warning("no connection")
            logger.debug(f"would add {self.remove_local_datarefs(datarefs.keys())}")
            return
        # Add those to monitor
        super().add_simulator_data_to_monitor(datarefs)
        prnt = []
        for d in datarefs.values():
            if d.is_internal:
                logger.debug(f"local dataref {d.name} is not monitored")
                continue
            if d.is_string:
                logger.debug(f"string dataref {d.name} is not monitored")
                continue
            if self.add_dataref_to_monitor(d.name, freq=d.update_frequency):
                prnt.append(d.name)

        logger.log(SPAM_LEVEL, f"add_simulator_data_to_monitor: added {prnt}")
        if MONITOR_DATAREF_USAGE:
            logger.info(f">>>>> monitoring++{len(datarefs)}/{len(self.datarefs)}/{self._max_monitored}")

    def remove_simulator_data_to_monitor(self, datarefs):
        if not self.connected and len(self.simulator_data_to_monitor) > 0:
            logger.warning("no connection")
            logger.debug(f"would remove {datarefs.keys()}/{self._max_monitored}")
            return
        # Add those to monitor
        prnt = []
        for d in datarefs.values():
            if d.is_internal:
                logger.debug(f"local dataref {d.name} is not monitored")
                continue
            if d.name in self.simulator_data_to_monitor.keys():
                if self.simulator_data_to_monitor[d.name] == 1:  # will be decreased by 1 in super().remove_simulator_data_to_monitor()
                    if self.add_dataref_to_monitor(d.name, freq=0):
                        prnt.append(d.name)
                else:
                    logger.debug(f"{d.name} monitored {self.simulator_data_to_monitor[d.name]} times")
            else:
                logger.debug(f"no need to remove {d.name}")

        logger.debug(f"removed {prnt}")
        super().remove_simulator_data_to_monitor(datarefs)
        if MONITOR_DATAREF_USAGE:
            logger.info(f">>>>> monitoring--{len(datarefs)}/{len(self.datarefs)}/{self._max_monitored}")

    def remove_all_datarefs(self):
        if not self.connected and len(self.all_simulator_data) > 0:
            logger.warning("no connection")
            logger.debug(f"would remove {self.all_simulator_data.keys()}")
            return
        # Not necessary:
        # self.remove_simulator_data_to_monitor(self.all_simulator_data)
        super().remove_all_simulator_data()

    def add_all_datarefs_to_monitor(self):
        if not self.connected:
            logger.warning("no connection")
            return
        # Add always monitored drefs
        self.add_always_monitored_datarefs()
        # Add those to monitor
        prnt = []
        for path in self.simulator_data_to_monitor.keys():
            d = self.all_simulator_data.get(path)
            if d is not None:
                if not d.is_string:
                    if self.add_dataref_to_monitor(d.name, freq=d.update_frequency):
                        prnt.append(d.name)
                else:
                    logger.debug(f"dataref {path} is string dataref, not requested")
            else:
                logger.warning(f"no dataref {path}")
        logger.log(SPAM_LEVEL, f"added {prnt}")

        # Add collector ticker
        # self.collector.add_ticker()
        # logger.info("..dataref sets collector ticking..")

    def cleanup(self):
        """
        Called when before disconnecting.
        Just before disconnecting, we try to cancel dataref UDP reporting in X-Plane
        """
        self.clean_datarefs_to_monitor()

    def start(self):
        if not self.connected:
            logger.warning("no IP address. could not start.")
            return

        if self.udp_event is None:  # Thread for X-Plane datarefs
            self.udp_event = threading.Event()
            self.udp_thread = threading.Thread(target=self.udp_enqueue, name="XPlaneUDP::udp_enqueue")
            self.udp_thread.start()
            logger.info("dataref listener started")
        else:
            logger.info("dataref listener already running.")

        if self.dref_thread is None:  # Thread for string datarefs
            self.dref_event = threading.Event()
            self.dref_thread = threading.Thread(target=self.strdref_enqueue, name="XPlaneUDP::strdref_enqueue")
            self.dref_thread.start()
            logger.info("string dataref listener started")
        else:
            logger.info("string dataref listener running.")

        # When restarted after network failure, should clean all datarefs
        # then reload datarefs from current page of each deck
        self.clean_datarefs_to_monitor()
        self.add_all_datarefs_to_monitor()
        logger.info("reloading pages")
        self.cockpit.reload_pages()  # to take into account updated values

    def stop(self):
        if self.udp_event is not None:
            self.udp_event.set()
            logger.debug("stopping dataref listener..")
            wait = SOCKET_TIMEOUT
            logger.debug(f"..asked to stop dataref listener (this may last {wait} secs. for UDP socket to timeout)..")
            self.udp_thread.join(wait)
            if self.udp_thread.is_alive():
                logger.warning("..thread may hang in socket.recvfrom()..")
            self.udp_event = None
            logger.debug("..dataref listener stopped")
        else:
            logger.debug("dataref listener not running")

        if self.dref_event is not None and self.dref_thread is not None:
            self.dref_event.set()
            logger.debug("stopping string dataref listener..")
            timeout = self.dref_timeout
            logger.debug(f"..asked to stop string dataref listener (this may last {timeout} secs. for UDP socket to timeout)..")
            self.dref_thread.join(timeout)
            if self.dref_thread.is_alive():
                logger.warning("..thread may hang in socket.recvfrom()..")
            else:
                self.dref_event = None
            logger.debug("..string dataref listener stopped")
        else:
            logger.debug("string dataref listener not running")

    # ################################
    # Cockpit interface
    #
    def terminate(self):
        logger.debug(f"currently {'not ' if self.udp_event is None else ''}running. terminating..")
        logger.info("terminating..")
        logger.info("..requesting to stop dataref emission..")
        self.clean_datarefs_to_monitor()  # stop monitoring all datarefs
        logger.info("..stopping dataref receptions..")
        self.stop()
        logger.info("..deleting datarefs..")
        self.remove_all_datarefs()
        logger.info("..disconnecting from simulator..")
        self.disconnect()
        logger.info("..terminated")
