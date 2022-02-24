from fastapi import APIRouter, Response, Request
import json
import s3fs
import urllib.request

router = APIRouter()
s3 = s3fs.S3FileSystem(anon=False, client_kwargs={"region_name": "us-east-1"})
indent = "    "

with open("app/catalog.json", "r") as fp:
    master_catalog = json.load(fp)
datasets = master_catalog.get("datasets")


@router.get("/data_catalog.yaml", tags=["intake"], description="Download an Intake catalog",
            summary="intake")
async def intake(request: Request) -> Response:
    all_matching_files = []
    for d in datasets:
        query = d + "*.zarr"
        all_matching_files.extend(s3.glob(query))
    catalog = f"metadata:\n{indent}version: 0.9.1\n\nsources:\n"
    for z in all_matching_files:
        catalog += f"{indent}{z.split('/')[-1].replace('.zarr', '')}\n" + \
                   f"{indent}{indent}description: {z.split('/')[-1]} as zarr from S3\n" + \
                   f"{indent}{indent}metadata:\n" + \
                   f"{indent}{indent}{indent}tags:\n" + \
                   f"{indent}{indent}{indent}- zarr\n" + \
                   f"{indent}{indent}driver: zarr\n" + \
                   f"{indent}{indent}args:\n" + \
                   f"{indent}{indent}{indent}urlpath: {z}\n" + \
                   f"{indent}{indent}{indent}consolidated: True\n\n"
    return Response(content=catalog, media_type="plain/text", status_code=200)
