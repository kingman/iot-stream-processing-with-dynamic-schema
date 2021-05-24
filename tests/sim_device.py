# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from core import CloudIot
from time import sleep
from datetime import datetime

import itertools
import logging
import math
import uuid

logger = logging.getLogger(__name__)

def main():
    with CloudIot() as cloud:
        print("Sending simulated event message to Cloud IoT Core.")
        for counter in itertools.count():
            cloud.publish_message(generate_payload(counter))
            sleep(1)
            if counter > 1000:
                exit(0)

def generate_sin_val(max, min, cycle, counter):
    return str(int((max+min)/2+(max-min)/2*math.sin(2*(counter%cycle)/cycle*math.pi)))

def generate_line_val(max, min, cycle, counter):
    return str(int(min+(max-min)*(counter%cycle)/cycle))

def generate_single_measurement(id, timestamp, device_name, measurement_type, measurement_val, value_type_str):
    return {
        "id":id,
        "origin":timestamp,
        "device":device_name,
        "name":measurement_type,
        "value":measurement_val,
        "valueType":value_type_str
    }

def generate_payload(counter):
    now = int(datetime.now().timestamp()*1000000)
    id = str(uuid.uuid1())
    device_name = "Sim Gateway"
    value_type_str = "Int16"
    pressure_val = generate_sin_val(85, 5, 120, counter)
    temperature_val = generate_sin_val(99, 15, 90, counter)
    level_val = generate_line_val(52, 0, 30, counter)

    return {
        'id':id,
        "device": device_name,
        "created":now,
        "origin":now,
        "readings":[
            generate_single_measurement(id, now, device_name, "Pressure", pressure_val, value_type_str),
            generate_single_measurement(id, now, device_name, "Temperature", temperature_val, value_type_str),
            generate_single_measurement(id, now, device_name, "Level", level_val, value_type_str)
        ]
    }

if __name__ == '__main__':
    main()
