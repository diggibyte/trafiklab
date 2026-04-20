"""HTQ2 GTFS Realtime Pipeline for Skånetrafiken.

Processes GTFS Realtime data from Trafiklab.se into 22 analytical
Delta Lake tables conforming to the HTQ2 BI_EDA schema.

Modules:
    ingestion  - Fetches GTFS data from Trafiklab API to UC Volume
    processing - Transforms Bronze protobuf to Silver Delta tables
    quality    - Validates data quality across all 22 tables
    helpers    - Shared utilities (Spark helpers, file utils, logging)
"""

__version__ = "1.0.0"
__author__ = "DiggiByte Solutions"
