import warnings
warnings.filterwarnings("ignore")
from cachetools import TTLCache, cached
# import cfgrib
from collections import OrderedDict
import dask
from dask.distributed import Client, LocalCluster
import gzip
import math
import netCDF4
import numpy as np
import os
import re
import s3fs
from typing import List, Literal, Optional
import urllib.parse
import xarray as xr
import xdrlib
import zarr as zr

cache = TTLCache(maxsize=8192, ttl=36000)
s3 = s3fs.S3FileSystem(anon=False, client_kwargs={"region_name": "us-east-1"})
open_bracket = "{"
close_bracket = "}"
indent = "    "
regex_triplet = "\[(?P<start>\d+)\:(?P<step>\d+)\:(?P<stop>-?\d+)\]"
regex_doublet = "\[(?P<start>\d+)\:(?P<stop>-?\d+)\]"
regex_singlet = "\[(?P<start>\d+)\]"

cluster = LocalCluster(n_workers=4)
client = Client(cluster)


@cached(cache)
def get_opendap_type(xr_type) -> str:
    if xr_type in (str, np.dtype("S1"), np.dtype("S2"), np.dtype("S4"), np.dtype("S8"), np.dtype("S16"), np.dtype("S32"), np.dtype("S64"), list, np.array, np.ndarray):
        return "String"
    elif xr_type in (np.int8,):
        # raise ValueError("8-bit signed char not supported in DAP2")
        return "Byte"  # Try it anyway
    elif xr_type in (np.uint8,):
        return "Byte"
    elif xr_type in (np.int16,):
        return "Int16"
    elif xr_type in (np.uint16,):
        return "UInt16"
    elif xr_type in (int, np.int32, np.int64):  # Note Int64 -> Int32 conversions below
        return "Int32"
    elif xr_type in (np.uint32,):
        return "UInt32"
    elif xr_type in (np.float16,):
        raise ValueError("16-bit float not supported in DAP2")
    elif xr_type in (np.float32,):
        return "Float32"
    elif xr_type in (float, np.float64):
        return "Float64"
    raise ValueError(f"Cannot interpret type {xr_type} as DAP2 type")

@cached(cache)
def get_xdr_type(packer: xdrlib.Packer, xr_type):
    if xr_type in (str, np.dtype("S1"), np.dtype("S2"), np.dtype("S4"), np.dtype("S8"), np.dtype("S16"), np.dtype("S32"), np.dtype("S64")):
        return packer.pack_string
    elif xr_type in (np.int8,):
        # raise ValueError("8-bit signed char not supported in DAP2")
        return packer.pack_int  # Try it anyway
    elif xr_type in (np.uint8,):
        return packer.pack_int
    elif xr_type in (int, np.int16, np.int32):
        return packer.pack_int
    elif xr_type in (np.uint16, np.uint32):
        return packer.pack_uint
    elif xr_type in (np.float16,):
        raise ValueError("16-bit float not supported in DAP2")
    elif xr_type in (np.float32,):
        return packer.pack_float
    elif xr_type in (float, np.float64):
        return packer.pack_double
    raise ValueError(f"Cannot pack type {xr_type}")


@cached(cache)
def load_dataset(path: str) -> xr.Dataset:
    if path.endswith(".nc"):
        ds = load_nc(path)
    elif path.endswith(".nc.gz"):
        ds = load_nc_gz(path)
    elif path.endswith(".zarr"):
        ds = load_zarr(path)
    # elif path.lower().endswith((".grb", ".grib", ".grb2", ".grib2")):
    #     ds = load_grb(path)
    else:
        raise ValueError(f"File type for {path} not recognized.")
    return ds


def load_nc(path: str) -> xr.Dataset:
    with s3.open(path, "rb") as fp:
        ncds = netCDF4.Dataset("data", memory=fp.read())
        xrds = xr.backends.NetCDF4DataStore(ncds)
        ds = xr.open_dataset(xrds, decode_cf=False)
    return ds


def load_nc_gz(path: str) -> xr.Dataset:
    with s3.open(path, "rb") as fp:
        with gzip.open(fp) as gp:
            ncds = netCDF4.Dataset("data", memory=gp.read())
            xrds = xr.backends.NetCDF4DataStore(ncds)
            ds = xr.open_dataset(xrds, decode_cf=False)
    return ds


def load_zarr(path: str) -> xr.Dataset:
    store = s3fs.S3Map(root=path, s3=s3, check=False)
    cache = zr.LRUStoreCache(store=store, max_size=2**28)
    ds = xr.open_zarr(cache, decode_cf=False)
    return ds


# def load_grb(path: str) -> xr.Dataset:
#     with s3.open(path, "rb") as fp:
#         ds = cfgrib.open_datasets(fp.read())
#         ds_map = []
#         total_coords = {}
#         for d in ds:
#             v_map = {}
#             for v in d.data_vars:
#                 level = d[v].attrs.get("GRIB_typeOfLevel")
#                 step = d[v].attrs.get("GRIB_stepType")
#                 name = v
#                 if level:
#                     name += f"_{level}"
#                 if step:
#                     name += f"_{step}"
#                 v_map.update({v: name})
#             for c in d.coords:
#                 if c in total_coords.keys():
#                     if (d[c].values == total_coords[c]).all():
#                         continue
#                     else:
#                         if c + "1" in total_coords.keys():
#                             name = c + "2"
#                         else:
#                             name = c + "1"
#                         v_map.update({c: name})
#                 else:
#                     total_coords[c] = d[c].values
#             d_map = v_map.copy()
#             for x in v_map.keys():
#                 if x not in d.dims:
#                     del d_map[x]
#             new_d = d.rename(v_map)
#             new_d = new_d.rename_dims(d_map)
#             new_d = new_d.expand_dims([x for x in new_d.coords if x not in new_d.dims])
#             ds_map.append(new_d)
#         dx = xr.merge(ds_map)
#         return dx


def parse_args(query_args: str) -> Optional[dict]:
    if query_args == "":
        return None
    cleaned = query_args.replace("=", "")
    decoded = urllib.parse.unquote(cleaned)
    string_to_list = decoded.split(",")
    parsed_args = OrderedDict([])
    for a in string_to_list:
        if "[" in a:  # anom[0:1:10][0:1:0][0:1:100][0:2:20]
            variable, bracket, values = a.partition("[")
            if "." in variable:
                before, after = variable.split(".")
                if before == after:
                    variable = before  # anom.anom -> anom
            parsed_args[variable] = bracket + values
        else:  # time
            if "." in a:
                before, after = a.split(".")
                if before == after:
                    a = before  # time.time -> time
            parsed_args[a] = "[0:1:-1]"
    return parsed_args  # {anom: [0:1:10][0:1:0][0:1:100][0:2:20], zlev: [0:1:0]}


def create_das(ds: xr.Dataset, parsed_args: dict) -> str:
    variable_attrs = OrderedDict([])
    global_attrs = {"NC_GLOBAL": OrderedDict([])}
    if parsed_args is None:
        for k in ds.variables:
            variable_attrs[k] = OrderedDict([(k, v) for k,v in ds[k].attrs.items()])
            for attrkey, attrval in variable_attrs[k].items():
                try:
                    if math.isnan(attrval):
                        del variable_attrs[k][attrkey]
                except TypeError:
                    pass
    else:
        for k in parsed_args.keys():
            variable_attrs[k] = OrderedDict([(k, v) for k,v in ds[k].attrs.items()])
            for attrkey, attrval in variable_attrs[k].items():
                try:
                    if math.isnan(attrval):
                        del variable_attrs[k][attrkey]
                except TypeError:
                    pass
    for k, v in ds.attrs.items():
        global_attrs["NC_GLOBAL"][k] = v
    master_dict = OrderedDict([])
    master_dict.update(variable_attrs)
    master_dict.update(global_attrs)

    das = "Attributes {\n"
    for k, v in master_dict.items():
        das += f"{indent}{k} {open_bracket}\n"
        for attrkey, attrval in v.items():
            if k == "NC_GLOBAL":
                dtype = get_opendap_type(type(ds.attrs[attrkey]))
                if dtype != "String":
                    das += f"{indent}{indent}{dtype} {attrkey} {attrval};\n"
                else:
                    das += f"{indent}{indent}{dtype} {attrkey} \"{attrval}\";\n"
            else:
                dtype = get_opendap_type(type(ds[k].attrs[attrkey]))
                if dtype != "String":
                    das += f"{indent}{indent}{dtype} {attrkey} {attrval};\n"
                else:
                    das += f"{indent}{indent}{dtype} {attrkey} \"{attrval}\";\n"
        das += f"{indent}{close_bracket}\n"
    das += f"{close_bracket}"
    return das


def create_dds(ds: xr.Dataset, parsed_args: dict, name: str) -> str:
    variable_ranges = OrderedDict([])
    if parsed_args is None:
        for k in ds.variables:
            dimensions = ds[k].dims
            variable_ranges[k] = OrderedDict([])
            for d in dimensions:
                variable_ranges[k][d] = ds[d].shape[0]
    else:
        for k, v in parsed_args.items():
            dimensions = ds[k].dims
            values = ["[" + x for x in v.split("[") if x]
            variable_ranges[k] = OrderedDict([])
            for i, d in enumerate(dimensions):
                try:
                    d_range = values[i]
                except IndexError:
                    d_range = "[0:1:-1]"  # dimension not specified -> assume all
                try:  # Most common [start:step:stop] pattern
                    d_range_dict = re.search(regex_triplet, d_range).groupdict()
                except AttributeError:
                    try:  # [start:stop] pattern with implied step=1
                        d_range_dict = re.search(regex_doublet, d_range).groupdict()
                        d_range_dict.update({"step": 1})
                    except AttributeError:  # [exact] pattern
                        d_range_dict = re.search(regex_singlet, d_range).groupdict()
                        d_range_dict.update({"step": 1, "stop": int(d_range_dict["start"])})
                if int(d_range_dict["stop"]) == -1:
                    d_range_dict["stop"] = ds[d].shape[0]
                else:
                    d_range_dict["stop"] = int(d_range_dict["stop"]) + 1
                d_size = int(np.ceil((int(d_range_dict["stop"]) - int(d_range_dict["start"]) / int(d_range_dict["step"]))))
                variable_ranges[k][d] = d_size
    
    dds = f"Dataset {open_bracket}\n"
    for variable, dim_size_dict in variable_ranges.items():
        if len(dim_size_dict) == 1 and variable == list(dim_size_dict.keys())[0]:
            # coordinate array
            for dim_name, dim_size in dim_size_dict.items():
                dtype = get_opendap_type(ds[dim_name].dtype)
                dds += f"{indent}{dtype} {dim_name}[{dim_name} = {dim_size}];\n"
        else:
            # variable grid
            vtype = get_opendap_type(ds[variable].dtype)
            dds += f"{indent}Grid {open_bracket}\n{indent} ARRAY:\n"
            dds += f"{indent}{indent}{vtype} {variable}"
            for dim_name, dim_size in dim_size_dict.items():
                dds += f"[{dim_name} = {dim_size}]"
            dds += f";\n{indent} MAPS:\n"
            for dim_name, dim_size in dim_size_dict.items():
                dtype = get_opendap_type(ds[dim_name].dtype)
                dds += f"{indent}{indent}{dtype} {dim_name}[{dim_name} = {dim_size}];\n"
            dds += f"{indent}{close_bracket} {variable};\n"
    dds += f"{close_bracket} {name};"
    return dds


def create_dods(ds: xr.Dataset, parsed_args: dict, name: str) -> bytes:
    packer = xdrlib.Packer()
    dds = create_dds(ds, parsed_args, name) + "\n\nData:\n"
    
    variable_ranges = OrderedDict([])
    if parsed_args is None:
        for k in ds.variables:
            dimensions = ds[k].dims
            variable_ranges[k] = OrderedDict([])
            for d in dimensions:
                variable_ranges[k][d] = {"start": 0, "step": 1, "stop": ds[d].shape[0]}
    else:
        for k, v in parsed_args.items():
            dimensions = ds[k].dims
            values = ["[" + x for x in v.split("[") if x]
            variable_ranges[k] = OrderedDict([])
            for i, d in enumerate(dimensions):
                try:
                    d_range = values[i]
                except IndexError:
                    d_range = "[0:1:-1]"  # dimension not specified -> assume all
                try:  # Most common [start:step:stop] pattern
                    d_range_dict = re.search(regex_triplet, d_range).groupdict()
                except AttributeError:
                    try:  # [start:stop] pattern with implied step=1
                        d_range_dict = re.search(regex_doublet, d_range).groupdict()
                        d_range_dict.update({"step": 1})
                    except AttributeError:  # [exact] pattern
                        d_range_dict = re.search(regex_singlet, d_range).groupdict()
                        d_range_dict.update({"step": 1, "stop": int(d_range_dict["start"])})
                if int(d_range_dict["stop"]) == -1:
                    d_range_dict["stop"] = ds[d].shape[0]
                else:
                    d_range_dict["stop"] = int(d_range_dict["stop"]) + 1
                variable_ranges[k][d] = d_range_dict
    for variable, data_dict in variable_ranges.items():
        if len(data_dict) == 1 and variable == list(data_dict.keys())[0]:
            for dim_name, dim_range_dict in data_dict.items():
                size = int(np.ceil((int(dim_range_dict["stop"]) - int(dim_range_dict["start"]) / int(dim_range_dict["step"]))))
                d_values = ds[dim_name][int(dim_range_dict["start"]):int(dim_range_dict["stop"]):int(dim_range_dict["step"])].values[:]
                packer.pack_uint(size)
                if d_values.dtype == np.int64:
                    d_values = d_values.astype('int32')
                    packer.pack_array(d_values, get_xdr_type(packer, np.int32))
                else:
                    packer.pack_array(d_values, get_xdr_type(packer, ds[dim_name].dtype))
        else:
            cdr = [(int(data_dict[d]["start"]), int(data_dict[d]["stop"]), int(data_dict[d]["step"])) for d in data_dict.keys()]
            if len(cdr) == 4:
                variable_array = ds[variable][
                    cdr[0][0]:cdr[0][1]:cdr[0][2],
                    cdr[1][0]:cdr[1][1]:cdr[1][2],
                    cdr[2][0]:cdr[2][1]:cdr[2][2],
                    cdr[3][0]:cdr[3][1]:cdr[3][2]
                    ].compute()
            elif len(cdr) == 3:
                variable_array = ds[variable][
                    cdr[0][0]:cdr[0][1]:cdr[0][2],
                    cdr[1][0]:cdr[1][1]:cdr[1][2],
                    cdr[2][0]:cdr[2][1]:cdr[2][2]
                    ].compute()
            elif len(cdr) == 2:
                variable_array = ds[variable][
                    cdr[0][0]:cdr[0][1]:cdr[0][2],
                    cdr[1][0]:cdr[1][1]:cdr[1][2]
                    ].compute()
            elif len(cdr) == 1:
                variable_array = ds[variable][
                    cdr[0][0]:cdr[0][1]:cdr[0][2]
                    ].compute()
            elif len(cdr) == 0:
                variable_array = ds[variable].compute()
            else:
                raise IndexError(f"Too many dimensions. There are {len(cdr)} dimensions, but only up to 4 are supported")
            variable_array_flat = np.array(variable_array).ravel()
            packer.pack_uint(variable_array_flat.shape[0])
            if variable_array_flat.dtype == np.int64:
                variable_array_flat = variable_array_flat.astype('int32')
                print(variable_array_flat.dtype)
                packer.pack_array(variable_array_flat, np.int32)
            else:
                packer.pack_array(variable_array_flat, get_xdr_type(packer, ds[variable].dtype))

            for dim_name, dim_range_dict in data_dict.items():
                size = int(np.ceil((int(dim_range_dict["stop"]) - int(dim_range_dict["start"]) / int(dim_range_dict["step"]))))
                d_values = ds[dim_name][int(dim_range_dict["start"]):int(dim_range_dict["stop"]):int(dim_range_dict["step"])].values[:]
                packer.pack_uint(size)
                if d_values.dtype == np.int64:
                    d_values = d_values.astype('int32')
                    packer.pack_array(d_values, get_xdr_type(packer, np.int32))
                else:
                    packer.pack_array(d_values, get_xdr_type(packer, ds[dim_name].dtype))
    dods = dds.encode() + packer.get_buffer()
    return dods


def create_subset(ds: xr.Dataset, name: str, format: Literal["csv", "nc"], vars: Optional[List[str]]) -> str:
    path = os.path.join("app/tmp", name)
    if not vars:
        new_ds = ds
    else:
        new_ds = ds[vars]
    if format == "nc":
        new_ds.to_netcdf(path, mode="w")
    elif format == "csv":
        new_ds.to_dataframe().to_csv(path, mode="w")
    return path
