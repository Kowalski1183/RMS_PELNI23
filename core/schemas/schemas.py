from typing import Union
from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import List, Dict


class MargeData(BaseModel):
    departure: Union[datetime, None] = None
    departure_date: Union[datetime, None] = None
    departure_time: Union[datetime, None] = None
    destination_port: str
    origin_port: str
    revenue: float
    jumlah_penumpang: int
    voyage: Union[str, None] = None
    total_pax: Union[int, None] = None
    type_rev: int
    revenue_cargo: float


class RouteCostRequest(BaseModel):
    idShip: str
    idPort: List[str]
    season: str = Field(default="PEAK")


class EstCostRevenueFactorRequest(BaseModel):
    idShip: str
    idPort: List[str]
    season: str = Field(default='LOW')


class EstRevenueFactorRequest(BaseModel):
    idShip: str
    idPort: List[str]
    season: str = Field(default='LOW')


class ListMapsRequest(BaseModel):
    idShip: str
    idPort: List[str]


class GenRouteOptimizer(BaseModel):
    class ParameterItem(BaseModel):
        class ConfigRouteItem(BaseModel):
            idConfRoute: str
            forbiddenPort: str
            status: str
            runningTime: int
            accuracy: int
            method: str
            trial: int

            class RouteDetailItem(BaseModel):
                class idPort(BaseModel):
                    idPort: str
                idConfRouteDetail: str
                idShip: str
                lastLocationPort: str
                originPort: str
                destinationPort: str
                mustVisitPort: str
                minPort: int
                maxPort: int
                minComDays: int
                maxComDays: int
                regionRule: str
                roundTrip: int
                homeBase: str

            routeDetail: List[RouteDetailItem]
        data: ConfigRouteItem
    parameter: ParameterItem


class ForecastHistPax(BaseModel):
    period_start: str
    period_end: str


class ForecastHistPro(BaseModel):
    period_start: str
    period_end: str


class GenerateSchedule(BaseModel):
    class Parameter(BaseModel):
        class Data(BaseModel):
            periodStart: str
            periodEnd: str
            idTaskManagement: str = Field(default="")
        data: Data

    parameter: Parameter
