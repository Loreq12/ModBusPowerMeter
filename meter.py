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
        logging.StreamHandler(),
        logging.FileHandler(f'debug_output/{datetime.datetime.now().isoformat()}.log')
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
    # Resource(register_address=0x0d, description="Cycle time"),
    Resource(register_address=0x0e, description="L1 Voltage", length=4, data_mode=float),
    Resource(register_address=0x10, description="L2 Voltage", length=4, data_mode=float),
    Resource(register_address=0x12, description="L3 Voltage", length=4, data_mode=float),
    Resource(register_address=0x14, description="Grid Frequency", length=4, data_mode=float),
    Resource(register_address=0x16, description="L1 Current", length=4, data_mode=float),
    Resource(register_address=0x18, description="L2 Current", length=4, data_mode=float),
    Resource(register_address=0x1a, description="L3 Current", length=4, data_mode=float),
    Resource(register_address=0x1c, description="Total Active Power", length=4, data_mode=float),
    Resource(register_address=0x1e, description="L1 Active Power", length=4, data_mode=float),
    Resource(register_address=0x20, description="L2 Active Power", length=4, data_mode=float),
    Resource(register_address=0x22, description="L3 Active Power", length=4, data_mode=float),
    Resource(register_address=0x24, description="Total Reactive Power", length=4, data_mode=float),
    Resource(register_address=0x26, description="L1 Reactive Power", length=4, data_mode=float),
    Resource(register_address=0x28, description="L2 Reactive Power", length=4, data_mode=float),
    Resource(register_address=0x2a, description="L3 Reactive Power", length=4, data_mode=float),
    Resource(register_address=0x2c, description="Total Apparent Power", length=4, data_mode=float),
    Resource(register_address=0x2e, description="L1 Apparent Power", length=4, data_mode=float),
    Resource(register_address=0x30, description="L2 Apparent Power", length=4, data_mode=float),
    Resource(register_address=0x32, description="L3 Apparent Power", length=4, data_mode=float),
    Resource(register_address=0x34, description="Total Power Factor", length=4, data_mode=float),

    Resource(register_address=0x100, description="Total Active Energy", length=4, data_mode=float),
    Resource(register_address=0x102, description="L1 Total Active Energy", length=4, data_mode=float),
    Resource(register_address=0x104, description="L2 Total Active Energy", length=4, data_mode=float),
    Resource(register_address=0x106, description="L3 Total Active Energy", length=4, data_mode=float),
    Resource(register_address=0x108, description="Forward Active Energy", length=4, data_mode=float),
    Resource(register_address=0x10a, description="L1 Forward Active Energy", length=4, data_mode=float),
    Resource(register_address=0x10c, description="L2 Forward Active Energy", length=4, data_mode=float),
    Resource(register_address=0x10e, description="L3 Forward Active Energy", length=4, data_mode=float),
    Resource(register_address=0x110, description="Reverse Active Energy", length=4, data_mode=float),
    Resource(register_address=0x112, description="L1 Reverse Active Energy", length=4, data_mode=float),
    Resource(register_address=0x114, description="L2 Reverse Active Energy", length=4, data_mode=float),
    Resource(register_address=0x116, description="L3 Reverse Active Energy", length=4, data_mode=float),
    Resource(register_address=0x118, description="Total Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x11a, description="L1 Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x11c, description="L2 Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x11e, description="L3 Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x120, description="Forward Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x122, description="L1 Forward Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x124, description="L2 Forward Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x126, description="L3 Forward Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x128, description="Reverse Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x12a, description="L1 Reverse Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x12c, description="L2 Reverse Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x12e, description="L3 Reverse Reactive Energy", length=4, data_mode=float),

    Resource(register_address=0x130, description="T1 Total Active Energy", length=4, data_mode=float),
    Resource(register_address=0x132, description="T1 Forward Active Energy", length=4, data_mode=float),
    Resource(register_address=0x134, description="T1 Reverse Active Energy", length=4, data_mode=float),
    Resource(register_address=0x136, description="T1 Total Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x138, description="T1 Forward Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x13a, description="T1 Reverse Reactive Energy", length=4, data_mode=float),

    Resource(register_address=0x13c, description="T2 Total Active Energy", length=4, data_mode=float),
    Resource(register_address=0x13e, description="T2 Forward Active Energy", length=4, data_mode=float),
    Resource(register_address=0x140, description="T2 Reverse Active Energy", length=4, data_mode=float),
    Resource(register_address=0x142, description="T2 Total Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x144, description="T2 Forward Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x146, description="T2 Reverse Reactive Energy", length=4, data_mode=float),

    Resource(register_address=0x148, description="T3 Total Active Energy", length=4, data_mode=float),
    Resource(register_address=0x14a, description="T3 Forward Active Energy", length=4, data_mode=float),
    Resource(register_address=0x14c, description="T3 Reverse Active Energy", length=4, data_mode=float),
    Resource(register_address=0x14e, description="T3 Total Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x150, description="T3 Forward Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x152, description="T3 Reverse Reactive Energy", length=4, data_mode=float),

    Resource(register_address=0x154, description="T4 Total Active Energy", length=4, data_mode=float),
    Resource(register_address=0x156, description="T4 Forward Active Energy", length=4, data_mode=float),
    Resource(register_address=0x158, description="T4 Reverse Active Energy", length=4, data_mode=float),
    Resource(register_address=0x15a, description="T4 Total Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x15c, description="T4 Forward Reactive Energy", length=4, data_mode=float),
    Resource(register_address=0x15e, description="T4 Reverse Reactive Energy", length=4, data_mode=float),
]

def _check_if_db_exists(database_path: Path) -> pd.DataFrame:
    if database_path.exists():
        logger.info(f"Database found")
        return pd.read_excel(database_path, index_col=0)
    else:
        logger.info(f"Creating new database")
        return pd.DataFrame(columns=["Datetime", "Date", "Time"] + [r.description for r in resources])

def read_all_data(meter: minimalmodbus.Instrument, database_path: Path) -> None:
    db: pd.DataFrame = _check_if_db_exists(database_path)
    current_datetime: datetime.datetime = datetime.datetime.now()
    result = []

    for resource in resources:
        if resource.data_mode == float:
            logger.info(f"Quering {resource.description} at address {resource.register_address:x}")
            try:
                data: float = meter.read_float(
                    registeraddress=resource.register_address,
                    functioncode=resource.function_code,
                )
            except Exception as e:
                logger.error(f"Could not get data: {e}")
                # Doesn't affect end result when plotting
                data = 0
            else:
                logger.info(f"For {resource.description} got following value: {data:.2f}")
            finally:
                result.append(f"{data:.2f}")

    db.loc[len(db.index)] = [current_datetime.isoformat(), current_datetime.date().isoformat(), current_datetime.time().isoformat(), *result]

    db.to_excel(database_path, sheet_name=str(current_datetime.year))

def setup_meter(device_path: Path) -> minimalmodbus.Instrument:
    smartmeter = minimalmodbus.Instrument(str(device_path), 1) # port name, slave address (in decimal)
    smartmeter.serial.baudrate = 9600         # Baud
    smartmeter.serial.bytesize = 8
    smartmeter.serial.parity   = serial.PARITY_EVEN # vendor default is EVEN
    smartmeter.serial.stopbits = 1
    smartmeter.serial.timeout  = 0.5           # seconds
    smartmeter.mode = minimalmodbus.MODE_RTU   # rtu or ascii mode
    smartmeter.clear_buffers_before_each_transaction = True
    smartmeter.debug = False # set to "True" for debug mode
    
    return smartmeter

app = typer.Typer()

@app.command()
def measure(
    device: Annotated[Path, typer.Option(help="Path to device which perform connection to power meter.")],
    database: Annotated[Path, typer.Option(help="Path which contains/will contain read data (Excel sheet). If file doesn't exist new Excel will be created.")],
):
    meter = setup_meter(device)
    read_all_data(meter, database)


if __name__ == "__main__":
    app()
