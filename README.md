# StlOpenDataEtl

_Author_: @nicseltzer, @jigglepuff, @mrpetrocket

_Status_: In-Development

## Description

This project aims to be the jumping off point for the OpenSTL Extract, Transform, and Load (ETL) pipeline.

## Developing

The below notes assume that you've forked the repo to your account and have pulled down a clone of that fork.

Note: This project has only been built on MacOS and Linux so far. Submit PRs with instructions on building / running in Windows (or whatever you use).

### Get Python3

If you don't have Python3 installed, you'll need to download it from the [Python website](https://www.python.org/downloads/) for your particular operating system.

### Setup your Virtual Environment (venv)

There are a lot of resources for doing this online, but I recommend the following (especically if you're using VSCode):

#### Virtual Environment

1. From your locally cloned repo, run `python3 -m venv .venv`. This will create a .venv directory which contains all of the pieces for your local Python project.
1. That's it. You've created a venv.

#### Activating Virtual Environment

1. Using the your favorite terminal or the terminal built-in to VSCode (which should pick up `.venv` automatically), run `source ./.venv/bin/activate`. This will do the magic of setting up your project in isolation from your global package manifest.
2. Next, we need to get the dependencies for the project. You can do this by running `./make.py`. If you need to add dependencies, you can add them with `pip` as you normally would. Just make sure to run `./package.py` before committing back to the repo.
3. If you are a Windows or RedHat user (any system without apt as package manager frontend), you might have to manually install `mdbtools`.
  - Visit [MDBTools official site](http://mdbtools.sourceforge.net/) for manual installation instructions.

#### Running the Application
If you're running the application locally on your PC, use the following instructions. This will create a local database.
1. Run `python3 ./app.py`.

#### Command Line Options
`--db dev` Use local database.
`--db prod` Use production database. If not specified, argument defaults to 'dev'.

`--local-sources` Use local files instead of downloading from the internet - this drastically reduces startup times while testing.

###### Examples:
1. Download from internet, commit to local database
```bash
python3 ./app.py
```
2. Use local sources, and commit to local database.
```bash
python3 ./app.py --local-sources file1.mdb file2.dbf
```
3. Download from internet, commit to production database.
```bash
python3 ./app.py --db prod
```
:warning: Example 3. will not work if you don't have the database admin credentials. For more details, [Go to Running with Production Database](#running-with-production-database).  

#### Running individual stages
To run individual stages (i.e. Fetcher only, Transformer only) without running the entire application, use the following commands:
1. Run `Fetcher` only:
Run with default `test_sources.yml`:
```bash
python3 tests/test_fetcher.py
```
To run with specific source YAML, run the following command replacing last argument with path to custom YAML:
```bash
python3 tests/test_fetcher.py ./data/sources/sources.yml
```

2. Run `Parser` only:
Use --local-sources to specify local files to parse:
```bash
python3 tests/test_parser.py --local-sources ./src/prcl.mdb ./src/par.dbf ./src/prcl_shape.zip
```

3. Run `Extractor` only:
```bash
python3 tests/test_extractor.py --local-sources ./src/prcl.mdb ./src/par.dbf ./src/prcl_shape.zip
```

4. Run `Transformer` only:
```bash
python3 tests/test_transformer.py --local-sources src/BldgCom.csv src/BldgRes.csv src/par.dbf.csv src/prcl.shp.csv src/Prcl.csv
```

5. Run `Loader` only:
```bash
python3 tests/test_loader.py --local-sources ./src/vacant_table.csv
```

#### Running unit tests
To run unit tests, run the following command from project root directory:
```bash
pytest
```

#### Deactivating Virtual Environment

1. Run `deactivate`.

#### Running with Production Database
If you're running the application to integrate with production database, use the following instructions. For development, you most likely will not need this.
1. Get database credentials from @jigglepuff
2. Run `python3 ./config.py`
   - Enter hostname: `dbopenstl.johnkramlich.com`
   - Enter database name: `openstl`
   - Enter username: (Ask project lead for credentials)
   - Enter password: (Ask project lead for credentials)
3. Run `python3 ./app.py --db prod`.



## Components

### Fetcher

_Author_: @nicseltzer

_Status_: Alpha

This script is run at a configurable interval and is responsible for fetching data from configured remote sources.

### Parser / Classifier

_Author_: @nicseltzer

_Status_: Alpha

This module is responsible for classifying fetched binary data. The application will hand this data off to the Extractor module.

### Extractor

_Author_: @jigglepuff

_Status_: In-Development

This module is responsible for taking data of a given format and extracting it to an agreed upon, unifrom format

### Transformer

_Author_: @mrpetrocket

_Status_: In-Development

This module will mold the data into a usable state.

### Loader

_Author_: @jigglepuff

_Status_: In-Development

This module is responsible for pushing the transformed data into a persistent datastore.

## Remote Data Sources

### Parcels (Shape, used for Key: HANDLE)

https://www.stlouis-mo.gov/data/upload/data-files/prcl_shape.zip

### Parcels (Information)

### Includes Tax Billing Information

https://www.stlouis-mo.gov/data/upload/data-files/prcl.zip
https://www.stlouis-mo.gov/data/upload/data-files/par.zip

### LRA Inventory

https://www.stlouis-mo.gov/data/upload/data-files/lra_public.zip

### Building Inspections

### Includes Condemnations and Vacant buildings

https://www.stlouis-mo.gov/data/upload/data-files/bldginsp.zip

### Building Permits

### Includes Occupancy and Demolition

https://www.stlouis-mo.gov/data/upload/data-files/prmbdo.zip

### Property Maintenance Billing (Using Forestry Property Maintenance)

https://www.stlouis-mo.gov/data/upload/data-files/forestry-maintenance-properties.csv
