# Simple MQTT time series logger

__Work in progress, not production ready__

This is a very simple (Python) logging daemon that subscribes
to multiple MQTT topics and logs received messages including
timestamps into daily ```npz``` files. The way it currently works
is really crude so it's only useable for low rate measurements
like minutely temperatures or alike. All data is read and written
for each measurement at the moment (this will change in future).

The service is configured by a configuration file (by default
at ```~/.config/mqttserieslogger.cfg```.

## Installation

```
pip install mqtttimeserieslogger-tspspi
```

## Sample configuration

```
{
        "datasources" : [
                {
                        "broker" : {
                                "broker" : "10.4.1.1",
                                "user" : "quasem",
                                "password" : "XXXX",
                                "basetopic" : "quasem/experiment/"
                        },

                        "quantities" : [
                                {
                                        "name": "pt1000pcb",
                                        "topic" : "cryo/temperature/pt1000pcb",
                                        "field" : [ "temperature", "temperature_C" ]
                                }
                        ]
                }
        ]
}
```

## Automatic launching with ```rc.init``` script

```
#!/bin/sh

# PROVIDE: mqtttimeserieslogger
# REQUIRE: NETWORKING SERVERS

# Simple MQTT time series logger
# 
# RC configuration variables:
#  mqtttimeserieslogger_enable:="NO"
#   Enables the logging service

. /etc/rc.subr

name="mqtttimeserieslogger"
rcvar=mqtttimeserieslogger_enable

load_rc_config $name
: ${mqtttimeserieslogger_enable:="NO"}

command=/usr/local/bin/mqtttimeserieslogger

load_rc_config $name
run_rc_command "$1"
```
