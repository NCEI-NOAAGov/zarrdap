from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from .api_catalog.catalog import router as catalog_router
from .api_dap2.dap2 import router as dap2_router
from .api_dap2.ncss import router as ncss_router
from .routes.views import router as view_router
from .api_intake.intake import router as intake_router

description = """
>*OPeNDAP for Zarr!*

ZarrDAP is an OPeNDAP API for NetCDF3, NetCDF4, and Zarr data in S3 buckets.

## Catalog

An **HTML catalog** provides a discovery UI for datasets in S3.

A **Search API** returns a list of datasets that match *keyword* or *filetype* parameters.

## OPeNDAP

DAP2-compliant **DAS**, **DDS**, and **DODS** URLs are available for OPeNDAP connectivity. Use the **DAP URL** with your favorite OPeNDAP client to get started.

## NetCDF Subsetting

Retrieve subsets of data files in NetCDF or CSV format.

## Intake

An **Intake** catalog is automatically generated from the ZarrDAP catalog.
"""

tag_metadata = [
    {
        "name": "catalog",
        "description": "Navigate or search the ZarrDAP catalog"
    },
    {
        "name": "dap",
        "description": "Request data from ZarrDAP"
    },
    {
        "name": "intake",
        "description": "Request data through Intake"
    },
    {
        "name": "debug",
        "description": "You know, for debugging"
    }
]

app = FastAPI(
    title="ZarrDAP", 
    description=description, 
    contact={
        "name": "Mark Capece",
        "email": "mark.capece@noaa.gov"
    },
    openapi_tags=tag_metadata,
    openapi_url="/zarrdap/api/openapi.json",
    version="0.9.1",
    docs_url="/zarrdap/docs", 
    redoc_url="/zarrdap/redoc")

app.include_router(view_router, prefix="/zarrdap/catalog")
app.include_router(catalog_router, prefix="/zarrdap/api/catalog")
app.include_router(dap2_router, prefix="/zarrdap/api/dap2")
app.include_router(ncss_router, prefix="/zarrdap/api/ncss")
app.include_router(intake_router, prefix="/zarrdap/api/intake")
app.mount("/zarrdap/static", StaticFiles(directory="app/static"), name="static")
