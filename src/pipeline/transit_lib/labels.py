"""Human-readable labels for GTFS enums used by BKK."""
from pyspark.sql import Column
from pyspark.sql import functions as F

# GTFS basic route types plus the extended codes BKK uses
# (11 = trolleybus, 109 = suburban railway / HÉV).
ROUTE_TYPE_LABELS = {
    0: "tram",
    1: "metro",
    2: "rail",
    3: "bus",
    4: "ferry",
    11: "trolleybus",
    109: "suburban_rail",
}

STOP_SCHEDULE_RELATIONSHIP = {0: "SCHEDULED", 1: "SKIPPED", 2: "NO_DATA", 3: "UNSCHEDULED"}
TRIP_SCHEDULE_RELATIONSHIP = {0: "SCHEDULED", 1: "ADDED", 2: "UNSCHEDULED", 3: "CANCELED", 5: "REPLACEMENT", 6: "DUPLICATED", 7: "DELETED"}
VEHICLE_STOP_STATUS = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}


def map_label(col: Column, mapping: dict, default: str = "other") -> Column:
    expr = None
    for code, label in mapping.items():
        expr = (F.when(col == code, label) if expr is None else expr.when(col == code, label))
    return expr.otherwise(default)


def route_type_label(col: Column) -> Column:
    return map_label(col.cast("int"), ROUTE_TYPE_LABELS)
