from fastapi import APIRouter, Query, Response, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import json
from pydantic import BaseModel
import s3fs
from typing import Optional, TypedDict

from .backend import *

router = APIRouter()
s3 = s3fs.S3FileSystem(anon=False, client_kwargs={"region_name": "us-east-1"})
HEADERS = {
    "XDODS-Server": "opendap/3.7",
    "Accept-Ranges": "bytes",
    "Connection": "close"
}


class DapParameterResponseTypedDict(TypedDict):
    path: str
    variables: dict
    attributes: dict


class DapParameterResponse(BaseModel):
    parameters: DapParameterResponseTypedDict


@router.get("/parameters", tags=["dap"], description="Query a dataset's properties",
            summary="parameters", response_model=DapParameterResponse,
            responses={200: {"content": {"application/json": {}}, "description": "Successful Response"}})
async def parameters(p: str) -> JSONResponse:
    ds = load_dataset(p)
    properties = {
        "path": p,
        "variables": {v: [c for c in ds[v].coords] for v in ds.variables },
        "attributes": {k: v for k, v in ds.attrs.items()}
    }
    results = {"parameters": properties}
    return JSONResponse(content=jsonable_encoder(results))


@router.get("/{path:path}.das", tags=["dap"], description="Request a DAS response", summary="DAS")
async def opendap_das(path: str, request: Request) -> Response:
    ds = load_dataset(path)
    parsed_args = parse_args(str(request.query_params))
    das = create_das(ds, parsed_args)
    headers = HEADERS.copy()
    headers.update({"Content-Description": "dods-das"})
    return Response(content=das, media_type="text/plain", status_code=200, headers=headers)


@router.get("/{path:path}.dds", tags=["dap"], description="Request a DDS response", summary="DDS")
async def opendap_dds(path: str, request: Request) -> Response:
    ds = load_dataset(path)
    name = request.path_params["path"].split("/")[-1]
    parsed_args = parse_args(str(request.query_params))
    dds = create_dds(ds, parsed_args, name)
    headers = HEADERS.copy()
    headers.update({"Content-Description": "dods-dds"})
    return Response(content=dds, media_type="text/plain", status_code=200, headers=headers)


@router.get("/{path:path}.dods", tags=["dap"], description="Request a binary response", summary="DODS")
async def opendap_dods(path: str, request: Request) -> Response:
    ds = load_dataset(path)
    name = request.path_params["path"].split("/")[-1]
    parsed_args = parse_args(str(request.query_params))
    dods = create_dods(ds, parsed_args, name)
    headers = HEADERS.copy()
    headers.update({"Content-Description": "dods-data"})
    return Response(content=dods, media_type="application/octet-stream", status_code=200, headers=headers)
