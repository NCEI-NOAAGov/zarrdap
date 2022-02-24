from cachetools import cached, TTLCache
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import json
from pydantic import BaseModel
import s3fs

cache = TTLCache(maxsize=8192, ttl=3600)
router = APIRouter()
s3 = s3fs.S3FileSystem(anon=False, client_kwargs={"region_name": "us-east-1"})
templates = Jinja2Templates(directory="app/templates")

with open("app/catalog.json", "r") as fp:
    master_catalog = json.load(fp)
datasets = master_catalog.get("datasets")


class HelloResponseModel(BaseModel):
    message: str = "Hello World!"


@cached(cache)
@router.get("/hello/", tags=["debug"], description="Hello World!", 
            summary="hello", response_model=HelloResponseModel, 
            responses={200: {"content": {"application/json": {}}, "description": "Successful Response"}})
async def hello() -> dict:
    return {"message": "Hello World!"}


class CatalogHtmlResponseModel(BaseModel):
    pass


@cached(cache)
@router.get("/", tags=["catalog"], description="HTML view of the root catalog", 
            summary="catalog root", response_model=CatalogHtmlResponseModel, 
            responses={200: {"content": {"html" : {}}, "description": "Successful Response"}})
async def root(request: Request) -> HTMLResponse:
    root_categories = set([d.split("/")[0] for d in datasets])
    listing = sorted(root_categories)
    return templates.TemplateResponse(
        "catalog.html", 
        {
            "request": request,
            "categories": listing, 
            "title": "Catalog"}
    )


@cached(cache)
@router.get("/{path:path}/", tags=["catalog"], description="HTML view of the nonroot catalog", 
            summary="inner catalog", response_model=CatalogHtmlResponseModel, 
            responses={200: {"content": {"html": {}}, "description": "Successful Response"}})
async def path(path: str, request: Request) -> HTMLResponse:
    categories = set([])
    categories.update([d.split(path)[-1].split("/")[1] for d in datasets if d.startswith(path)])
    if "" in categories:
        categories.remove("")
    if "*" in categories:
        categories.remove("*")
        keys_in_cat = set([])
        keys_in_cat.update(
            [p.split(path)[-1].split("/")[1] for p in s3.ls(path) if all((s3.isdir(p), not p.endswith(".zarr")))]
        )
        categories.update(keys_in_cat)
    files = [
        x.split("/")[-1] for x in s3.ls(path) if x.lower().endswith((
            ".nc",
            ".nc.gz",
            ".zarr",
            # ".grb",
            # ".grib",
            # ".grb2",
            # ".grib2"
        ))
    ]
    listing = sorted(categories)
    listing.extend(files)
    return templates.TemplateResponse(
        "catalog.html", 
        {
            "request": request,
            "categories": listing, 
            "title": "Catalog"}
    )
