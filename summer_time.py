import datetime
import logging
import minimalmodbus
import pandas as pd
import serial
import sys
import typer

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Resource:
    register_address: int
    description: str
    length: int
    data_mode: type 
    function_code: int = 3


resources: List[Resource] = [
    Resource(register_address=0x3c, description="Time", length=4, data_mode=datetime.datetime),
]

def read_all_data(meter: minimalmodbus.Instrument) -> None:
    current_datetime = datetime.datetime.now()
    for resource in resources:
        if resource.data_mode == float:
            logger.info(f"Quering {resource.description} at address {resource.register_address:x}")
            data: float = meter.read_float(
                registeraddress=resource.register_address,
                functioncode=resource.function_code,
            )
            logger.info(f"For {resource.description} got following value: {data:.2f}")
            result.append(f"{data:.2f}")
        else:
            datetime_to_string_repr = f"\
            10\ # Those are seconds
            {current_datetime.time().minute:02}\
            {current_datetime.time().hour:02}\
            05\
            {current_datetime.date().day:02}\
            {current_datetime.date().month:02}\
            {current_datetime.date().year % 100}\
            00\
            "
            meter.write_long(registeraddress=resource.register_address, value=int("1040180506042400", 16), number_of_registers=4)

def setup_meter(device_path: Path) -> minimalmodbus.Instrument:
    smartmeter = minimalmodbus.Instrument(str(device_path), 1) # port name, slave address (in decimal)
    smartmeter.serial.baudrate = 9600         # Baud
    smartmeter.serial.bytesize = 8
    smartmeter.serial.parity   = serial.PARITY_EVEN # vendor default is EVEN
    smartmeter.serial.stopbits = 1
    smartmeter.serial.timeout  = 0.5           # seconds
    smartmeter.mode = minimalmodbus.MODE_RTU   # rtu or ascii mode
    smartmeter.clear_buffers_before_each_transaction = True
    smartmeter.debug = True # set to "True" for debug mode
    
    return smartmeter

app = typer.Typer()

@app.command()
def measure(
    device: Annotated[Path, typer.Option(help="Path to device which perform connection to power meter.")],
):
    meter = setup_meter(device)
    read_all_data(meter)


if __name__ == "__main__":
    app()
