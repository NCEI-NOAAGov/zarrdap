# ZarrDAP
>*OPeNDAP for Zarr!*

ZarrDAP is a FastAPI project that provides access to Zarr and NetCDF data in remote object storage using the Open-source Project for a Network Data Access Protocol (OPeNDAP).

## Table of Contents
* [Getting Started](#getting-started)
* [Developing](#developing)
* [Maintainers](#maintainers)
* [Contributing](#contributing)
* [License](#license)

## Getting Started
To run ZarrDAP directly:
1. Clone this repository: `git clone <URL>`
1. Create a Python virtual environment: `python3 -m venv .venv`
1. Activate the virtual environment: `source .venv/bin/activate` (Linux) or `.venv\Scripts\activate.bat` (Windows)
1. Install required libraries: `pip install -r requirements.txt`
1. Start the server: `gunicorn -k uvicorn.workers.UvicornH1Worker app.zarrdap:app -b 0.0.0.0:8000`

To run ZarrDAP in Docker:
1. Clone this repository: `git clone <URL>`
1. Modify the `Dockerfile` with any additional parameters
1. Create a Docker image: `docker build -t zarrdap .`
1. Start a Docker container: `docker run -d -p 80:8000 zarrdap`

## Developing
ZarrDAP is composed of a frontend UI and four APIs. The APIs are documented at the `/zarrdap/docs` endpoint.

### Catalog
>`/zarrdap/catalog`
>`/zarrdap/api/catalog`

An **HTML catalog** provides a discovery UI for datasets in S3. HTML templates are developed in `app/templates`, and the routing is defined in `app/routes/views.py`.

A **Search API** returns a list of datasets that match *keyword* or *filetype* parameters. The API is developed in `app/api_catalog/catalog.py`.

### OPeNDAP
>`/zarrdap/api/dap2`

DAP2-compliant **DAS**, **DDS**, and **DODS** URLs are available for OPeNDAP connectivity. Use the **DAP URL** with your favorite OPeNDAP client to get started. DAP2 calculations are performed in `app/api_dap2/backend.py` and routing in `app/api_dap2/dap2.py`.

### NetCDF Subsetting
>`/zarrdap/api/ncss`

Retrieve subsets of data files in NetCDF or CSV format . Subsetting calculations are performed in `app/api_dap2/backend.py` and routing in `app/api_dap2/ncss.py`.

### Intake
>`/zarrdap/api/intake`

An **Intake** catalog is automatically generated from the ZarrDAP catalog.

## Maintainers
Mark Capece
* mark@markcapece.net
* [@markccapece](https://github.com/markccapece)

## Contributing
All contributors are welcome. Just [open an issue](https://github.com/NCEI-NOAAGov/zarrdap/issues) or [submit a pull request](https://github.com/NCEI-NOAAGov/zarrdap/pulls)!

## License
Software code created by U.S. Government employees is not subject to copyright in the United States (17 U.S.C. §105).

The United States/Department of Commerce reserve all rights to seek and obtain copyright protection in countries other
than the United States for Software authored in its entirety by the Department of Commerce. To this end, the Department
of Commerce hereby grants to Recipient a royalty-free, nonexclusive license to use, copy, and create derivative works of
the Software outside of the United States.

