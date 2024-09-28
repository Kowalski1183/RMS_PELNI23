from fastapi import FastAPI

from app.services import CostOfRoute, HistPaxRevenue, MapsOfPortConnectivity, RetrieveCargo, RetrievePax, EstCostRevenueFactor, EstRevenueFactor, ForecastHistPax, ForecastHistPro, GenRouteOptimizer, GenerateSchedule
from fastapi.middleware.cors import CORSMiddleware

tags_metadata = [
    {"name": "Retrieve Service", "description": ""},
    {"name": "Calculator Service", "description": ""},
    {"name": "Maps Service", "description": ""},
    {"name": "Insert Service", "description": ""},
    {"name": "Forecast HistPro Service", "description": ""},
    {"name": "Route Optimizer Service", "description": ""},
    {"name": "Generate Schedule Service", "description": ""},
]

app = FastAPI(openapi_tags=tags_metadata)

app.include_router(RetrieveCargo.router)
app.include_router(RetrievePax.router)
# app.include_router(MapsOfPortConnectivity.router)
app.include_router(HistPaxRevenue.router)
# app.include_router(CostOfRoute.router)
# app.include_router(EstRevenueFactor.router)
app.include_router(EstCostRevenueFactor.router)
app.include_router(GenRouteOptimizer.router)
# app.include_router(ForecastHistPax.router)
app.include_router(ForecastHistPro.router)
app.include_router(GenerateSchedule.router)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
