from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import json
from pydantic import BaseModel
import s3fs
from typing import Optional, TypedDict, List

router = APIRouter()
s3 = s3fs.S3FileSystem(anon=False, client_kwargs={"region_name": "us-east-1"})

with open("app/catalog.json", "r") as fp:
    master_catalog = json.load(fp)
datasets = master_catalog.get("datasets")


class CatalogSearchResponseTypedDict(TypedDict):
    filepath: str
    dap_url: str
    timestamp: str


class CatalogSearchResponse(BaseModel):
    matches: List[CatalogSearchResponseTypedDict]


@router.get("/search", tags=["catalog"], description="Query the catalog for matching files",
            summary="search", response_model=CatalogSearchResponse,
            responses={200: {"content": {"application/json": {}}, "description": "Successful Response"}})
async def search(
    request: Request,
    keyword: Optional[str] = None, 
    filetype: Optional[str] = None
) -> JSONResponse:
    folders = []
    matching_files = []
    for d in datasets:
        query = d
        if keyword:
            query += f"*{keyword}"
        if filetype:
            query += f"*.{filetype}"
        else:
            query += "*"
        print(query)
        all_files = s3.glob(query)
        matching_files.extend([x for x in all_files if any([s3.isfile(x), x.endswith(".zarr")])])
    match_dict = [{"filepath": x,
                   "dap_url": request.url_for("opendap_dods", path=x)[:-5].replace("http", "https"),
                   "timestamp": s3.modified(x) if s3.isfile(x) else ""}
                  for x in matching_files]
    results = {"matches": match_dict}
    return JSONResponse(content=jsonable_encoder(results))
