import argparse
import sys
import logging
from datetime import datetime, timedelta

import signal, grp, os

from pwd import getpwnam
from daemonize import Daemonize

import json

from pathlib import Path
from time import sleep

import numpy as np
import paho.mqtt.client as mqtt

# ========== Main logging daemon ==========

class MQTTLoggerDaemon:
    def __init__(self, args, logger):
        self._args = args
        self._log = logger
        self._terminate = False
        self._reloadConfig = True

        self._mqtt = []

    def _signal_sig_hup(self, *args):
        self._reloadConfig = True
    def _signal_sig_term(self, *args):
        self._terminate = True

    def __enter__(self):
        return self
    def __exit__(self, type, value, tb):
        pass

    def _load_config(self):
        # Drop existing connection (if any)
        for mq in self._mqtt:
            mq.close()
        self._mqtt = []

        # Read our configuration file
        newCfg = None
        try:
            with open(self._args.cfg) as cfgfile:
                newCfg = json.load(cfgfile)
        except Exception as e:
            self._log.error(f"Failed to read configuration file:\n{str(e)}")
            return False

        # Parse configuration and create subscriptions
        try:
            if "datasources" not in newCfg:
                raise ValueError("Configuration invalid, does not contain datasources")
            for ds in newCfg["datasources"]:
                if "broker" not in ds:
                    raise ValueError("Configuration invalid. Broker configuration missing in datasource")
                broker = ds["broker"]
                if ("broker" not in broker) or ("user" not in broker) or ("password" not in broker) or ("basetopic" not in broker):
                    raise ValueError("Broker configuration invalid, missing broker, user, password or basetopic")

                port = 1883
                if "port" in broker:
                    port = int(broker["port"])
                    if (port < 1) or (port > 65535):
                        raise ValueError(f"Broker configuration invalid, port number {broker['port']} out of valid range")

                # We got valid broker configuration (most likely). Create our handler configuration
                handlers = []
                for quant in ds["quantities"]:
                    if ("name" not in quant) or ("topic" not in quant) or ("field" not in quant):
                        raise ValueError("Configuration of quantity invalid, missing name, topic or field")

                    handlers.append({
                        'topic' : quant["topic"],
                        'handler' : [ lambda topic, msg, fields=quant["field"], qname=quant["name"] : self._receive_message(topic, msg, qname, fields) ]
                    })
                    self._log.debug(f"Registering handler for {quant['topic']}: sensor {quant['name']}, field {quant['field']}")

                self._mqtt.append(MQTTPublisher(
                    self._log,
                    broker["broker"],
                    port,
                    broker["user"],
                    broker["password"],
                    broker["basetopic"],
                    handlers
                ))
            return True
        except Exception as e:
            self._log.error("Failed to read configuration file")
            self._log.error(str(e))
            return False



    def _receive_message(self, topic, msg, sensorname, field):
        # Handle our logging ...
        if isinstance(field, list):
            for f in field:
                self._receive_message(topic, msg, sensorname, f)
            return

        try:
            self._log.debug(f"Received {field}={msg[field]} by {sensorname}")
        except:
            self._log.error(f"Failed to write field {field} from message {msg} for sensor {sensorname}")
            return
        # Write into our logfile ...

        fnameprefix = datetime.today().strftime('%Y-%m-%d')
        fname = os.path.join(self._args.datadir, f"{fnameprefix}.npz")
        olddata = {}

        try:
            od = np.load(fname)
            for k in od.keys():
                olddata[k] = list(od[k])
        except FileNotFoundError:
            pass
        except Exception as e:
            self._log.error(f"Failed to load old data\n{str(e)}")

        # Update _our_ data
        if f"{sensorname}_{field}" not in olddata:
            olddata[f"{sensorname}_{field}"] = []
            olddata[f"{sensorname}_{field}__ts"] = []
        olddata[f"{sensorname}_{field}"].append(msg[field])
        olddata[f"{sensorname}_{field}__ts"].append(int(datetime.now().timestamp()))

        # And write out again ...
        for k in olddata:
            olddata[k] = np.asarray(olddata[k])
        try:
            np.savez(fname, **olddata)
        except FileNotFoundError:
            self._log.error(f"Failed to write file {fname}")

    def run(self):
        signal.signal(signal.SIGHUP, self._signal_sig_hup)
        signal.signal(signal.SIGTERM, self._signal_sig_term)
        signal.signal(signal.SIGINT, self._signal_sig_term)

        self._log.info("Service running")

        while True:
            if self._terminate:
                for mq in self._mqtt:
                    mq.close()
                self._mqtt = False
                break
            if self._reloadConfig:
                if self._load_config():
                    self._reloadConfig = False
                else:
                    # We try to reload config in 10 seconds again
                    self._log.error("Failed to load configuration, retrying in 10 seconds again")
                    sleep(10)
                    continue
            sleep(5)

        self._log.info("Terminating logger on user request")
 

# ========== MQTT helper ==========

class MQTTPatternMatcher:
    def __init__(self):
        self._handlers = []
        self._idcounter = 0

    def registerHandler(self, pattern, handler):
        self._idcounter = self._idcounter + 1
        self._handlers.append({ 'id' : self._idcounter, 'pattern' : pattern, 'handler' : handler })
        return self._idcounter

    def removeHandler(self, handlerId):
        newHandlerList = []
        for entry in self._handlers:
            if entry['id'] == handlerId:
                continue
            newHandlerList.append(entry)
        self._handlers = newHandlerList

    def _checkTopicMatch(self, filter, topic):
        filterparts = filter.split("/")
        topicparts = topic.split("/")

        # If last part of topic or filter is empty - drop ...
        if topicparts[-1] == "":
            del topicparts[-1]
        if filterparts[-1] == "":
            del filterparts[-1]

        # If filter is longer than topics we cannot have a match
        if len(filterparts) > len(topicparts):
            return False

        # Check all levels till we have a mistmatch or a multi level wildcard match,
        # continue scanning while we have a correct filter and no multi level match
        for i in range(len(filterparts)):
            if filterparts[i] == '+':
                continue
            if filterparts[i] == '#':
                return True
            if filterparts[i] != topicparts[i]:
                return False

        if len(topicparts) != len(filterparts):
            return False

        # Topic applies
        return True

    def callHandlers(self, topic, message, basetopic = "", stripBaseTopic = True):
        topic_stripped = topic
        if basetopic != "":
            if topic.startswith(basetopic) and stripBaseTopic:
                topic_stripped = topic[len(basetopic):]

        for regHandler in self._handlers:
            if self._checkTopicMatch(regHandler['pattern'], topic):
                if isinstance(regHandler['handler'], list):
                    for handler in regHandler['handler']:
                        handler(topic_stripped, message)
                elif callable(regHandler['handler']):
                    regHandler['handler'](topic_stripped, message)

class MQTTPublisher:
    def __init__(self, logger, broker, port, username, password, basetopic, topichandlers=None):
        self._logger = logger
        logger.debug('MQTT: Starting up')

        self._topicHandlers = topichandlers

        self._config = {
            'broker'    : broker,
            'port'      : port,
            'username'  : username,
            'password'  : password,
            'basetopic' : basetopic
        }

        logger.debug("MQTT: Configured broker {} : {} (user: {})".format(broker, port, username))

        self._shuttingDown = False

        self._mqtt = mqtt.Client(reconnect_on_failure=True)
        self._mqtt.on_connect = self._mqtt_on_connect
        self._mqtt.on_message = self._mqtt_on_message
        self._mqtt.on_disconnect = self._mqtt_on_disconnect

        if username:
            self._mqtt.username_pw_set(self._config['username'], self._config['password'])
        try:
            self._mqtt.connect(self._config['broker'], self._config['port'])
        except:
            logger.error("Failed to connect to MQTT broker ...")
            self._mqtt = None
            return

#        atexit.register(self._shutdown)

        # Start asynchronous loop ...
        self._mqtt.loop_start()

    def _mqtt_on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._logger.debug("MQTT: Connected to broker")
#            self.publish_event("mqtt/connect")
        else:
            self._logger.error("MQTT: Failed to connect to broker ({})".format(rc))

        # Here one could also subscribe to various topics ...
        # We use the self._topicHandlers to create our MQTT matcher and
        # then subscribe to the topics requested. These can then be used
        # to react to MQTT messages from the outside
        self._mqttHandler = MQTTPatternMatcher()
        if self._topicHandlers is not None:
            for handler in self._topicHandlers:
                self._mqttHandler.registerHandler(f"{self._config['basetopic'] + handler['topic']}", handler['handler'])
                client.subscribe(self._config['basetopic'] + handler['topic'])
                self._logger.debug(f"Subscribing to {self._config['basetopic'] + handler['topic']}")

    def _mqtt_on_message(self, client, userdata, msg):
        self._logger.debug("MQTT: Received message on {}".format(msg.topic))

        # First try to decode object (if it's JSON)
        try:
            msg.payload = json.loads(str(msg.payload.decode('utf-8', 'ignore')))
        except json.JSONDecodeError:
            pass

        # One could handle messages here (or raise event handlers, etc.) ...
        if self._mqttHandler is not None:
            self._mqttHandler.callHandlers(msg.topic, msg.payload, self._config['basetopic'])

    def _mqtt_on_disconnect(self, client, userdata, rc=0):
        self._logger.error("MQTT: Disconnected ({})".format(rc))
        if self._shuttingDown:
            client.loop_stop()

    def _shutdown(self):
        if self._mqtt:
            try:
                self._shuttingDown = True
                self._mqtt.disconnect()
            except:
                pass
        self._mqtt = None

    def _publish_raw(self, topic, message=None, prependBasetopic=True):
        if self._mqtt:
            if isinstance(message, dict):
                message = json.dumps(message, cls=NumpyArrayEncoder)

            realtopic = self._config['basetopic']+topic

            try:
                if not (message is None):
                    self._mqtt.publish(realtopic, payload=message, qos=0, retain=False)
                else:
                    self._mqtt.publish(realtopic, qos=0, retain=False)

                self._logger.debug("MQTT: Published to {}".format(realtopic))
            except Exception as e:
                self._logger.error("MQTT: Publish failed ({})".format(e))

    def publish_event(self, eventtype, payload=None):
        self._publish_raw(eventtype, payload)

    def close(self):
        self._shutdown()
        sleep(5)

class NumpyArrayEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, datetime):
            return obj.__str__()
        if isinstance(obj, timedelta):
            return obj.__str__()
        return json.JSONEncoder.default(self, obj)

# ========== Startup logic: Argument parsing and daemonization ==========

def parseArguments():
    defaultCfgPath = str(os.path.join(Path.home(), ".config/mqttserieslogger.cfg"))
    defaultDataPath = str(os.path.join(Path.home(), "mqttserieslog"))

    # Parse arguments ...
    ap = argparse.ArgumentParser(description = "MQTT time series logger")

    ap.add_argument("-f", "--foreground", action="store_true", help="Stay in foreground, do not daemonize")

    ap.add_argument('--uid', type=str, required=False, default=None, help="User ID to impersonate when launching as root")
    ap.add_argument('--gid', type=str, required=False, default=None, help="Group ID to impersonate when launching as root")
    ap.add_argument('--chroot', type=str, required=False, default=None, help="Chroot directory that should be switched into")
    ap.add_argument('--pidfile', type=str, required=False, default="/var/run/mqtttimeserieslog.pid", help="PID file to keep only one daemon instance running")
    ap.add_argument('--loglevel', type=str, required=False, default="error", help="Loglevel to use (debug, info, warning, error, critical). Default: error")
    ap.add_argument('--logfile', type=str, required=False, default="/var/log/mqtttimeserieslog.log", help="Logfile that should be used as target for log messages")

    ap.add_argument("--cfg", type=str, required=False, default=defaultCfgPath, help=f"Configuration file path (default: {defaultCfgPath})")
    ap.add_argument("--datadir", type=str, required=False, default=defaultDataPath, help=f"Default data directory (default: {defaultDataPath})")
    ap.add_argument("--singlefiles", action='store_true', help="Instead of writing into a single NPZ write into one NPZ per sensor")

    args = ap.parse_args()
    loglvls = {
        "DEBUG"     : logging.DEBUG,
        "INFO"      : logging.INFO,
        "WARNING"   : logging.WARNING,
        "ERROR"     : logging.ERROR,
        "CRITICAL"  : logging.CRITICAL
    }
    if not args.loglevel.upper() in loglvls:
        print("Unknown log level {}".format(args.loglevel.upper()))
        sys.exit(1)

    logger = logging.getLogger()
    logger.setLevel(loglvls[args.loglevel.upper()])
    if args.logfile:
        fileHandleLog = logging.FileHandler(args.logfile)
        logger.addHandler(fileHandleLog)
    if args.foreground:
        stderrHandler = logging.StreamHandler(sys.stderr)
        logger.addHandler(stderrHandler)

    return args, logger

def mainDaemon():
    args, logger = parseArguments()
    logger.debug("Daemon starting")
    with MQTTLoggerDaemon(args, logger) as daemon:
        daemon.run()

def main():
    args, logger = parseArguments()

    daemonPidfile = args.pidfile
    daemonUid = None
    daemonGid = None
    daemonChroot = "/"

    if args.uid:
        try:
            args.uid = int(args.uid)
        except ValueError:
            try:
                args.uid = getpwnam(args.uid).pw_uid
            except KeyError:
                logger.critical("Unknown user {}".format(args.uid))
                print("Unknown user {}".format(args.uid))
                sys.exit(1)
        daemonUid = args.uid
    if args.gid:
        try:
            args.gid = int(args.gid)
        except ValueError:
            try:
                args.gid = grp.getgrnam(args.gid)[2]
            except KeyError:
                logger.critical("Unknown group {}".format(args.gid))
                print("Unknown group {}".format(args.gid))
                sys.exit(1)

        daemonGid = args.gid

    if args.chroot:
        if not os.path.isdir(args.chroot):
            logger.critical("Non existing chroot directors {}".format(args.chroot))
            print("Non existing chroot directors {}".format(args.chroot))
            sys.exit(1)
        daemonChroot = args.chroot

    if args.foreground:
        logger.debug("Launching in foreground")
        with MQTTLoggerDaemon(args, logger) as daemon:
            daemon.run()
    else:
        logger.debug("Daemonizing ...")
        daemon = Daemonize(
            app="MQTT time series logger",
            action=mainDaemon,
            pid=daemonPidfile,
            user=daemonUid,
            group=daemonGid,
            chdir=daemonChroot
        )
        daemon.start()


if __name__ == "__main__":
    main()

