# i24-write-to-mongo

This repository was used to transfer synthetic data in the form of CSV files into MongoDB for initial testing of post-process modules and database functionalities. 

## Overview

- **write_raw.py**: turns raw trajectory CSV files into Python dictionaries to insert into MongoDB by keeping a cache of ongoing car objects
- **write_gt.py**: turns sorted gound truth trajectory CSV files into Python dictionaries to insert into MongoDB
- **write_to_mongo_old**: old solution of writing CSV files into Python dictionaries by loading everything into memory and processing (space inefficient)