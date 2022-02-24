from fastapi import APIRouter, Query, Response, Request
from fastapi.responses import FileResponse
from typing import List, Literal, Optional

from .backend import *

router = APIRouter()
s3 = s3fs.S3FileSystem(anon=False, client_kwargs={"region_name": "us-east-1"})


@router.get("/subset", tags=["dap"], description="Request pieces of data in a variety of formats",
            summary="subset")
async def subset(path: str, format: Literal["csv", "nc"], vars: Optional[List[str]] = None) -> FileResponse:
    ds = load_dataset(path)
    name = path.split("/")[-1]
    name_root, _ = os.path.splitext(name)
    new_name = ".".join((name_root, format))
    tmp_file = create_subset(ds, new_name, format, vars)
    return FileResponse(tmp_file, status_code=200, 
                        media_type="application/octet-stream", 
                        filename=new_name)
