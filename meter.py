import datetime
import minimalmodbus
import pandas as pd
import serial
import typer

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, List


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
]

def _check_if_db_exists(database_path: Path) -> pd.DataFrame:
    if database_path.exists():
        return pd.read_excel(database_path)
    else:
        return pd.DataFrame(columns=["Datetime", "Date", "Time"] + [r.description for r in resources])

def read_all_data(meter: minimalmodbus.Instrument, database_path: Path) -> None:
    db: pd.DataFrame = _check_if_db_exists(database_path)
    current_datetime: datetime.datetime = datetime.datetime.now()
    result = []

    for resource in resources:
        if resource.data_mode == float:
            data: float = meter.read_float(
                registeraddress=resource.register_address,
                functioncode=resource.function_code,
            )
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
