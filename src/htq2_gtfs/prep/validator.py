"""Inline fail-fast validation for the Prep layer.

Runs BEFORE staging tables are written. If any check fails,
the task exits with code 1 and Layer 3 never executes.

Checks:
  P1: parsed_journey row count > 0
  P2: parsed_call row count > 0
  P3: Schema matches JOURNEY_SCHEMA / CALL_SCHEMA
  P4: DVJId null rate < 10%
  P5: OperatingDate not null
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.processing.core import JOURNEY_SCHEMA, CALL_SCHEMA

logger = get_logger(__name__)


class PrepValidationError(Exception):
    """Raised when prep inline validation fails."""
    pass


def validate_prep_output(journey_df: DataFrame, call_df: DataFrame) -> None:
    """Fail-fast validation — prevents bad data from reaching Silver.

    Raises PrepValidationError if any check fails.
    """
    errors: list[str] = []

    # P1: Journey row count > 0
    j_count = journey_df.count()
    if j_count == 0:
        errors.append("P1: parsed_journey has 0 rows")

    # P2: Call row count > 0
    c_count = call_df.count()
    if c_count == 0:
        errors.append("P2: parsed_call has 0 rows")

    # P3: Schema matches expected columns
    expected_j = {f.name for f in JOURNEY_SCHEMA.fields}
    actual_j = set(journey_df.columns) - {"_run_id", "_parsed_timestamp"}
    if missing_j := expected_j - actual_j:
        errors.append(f"P3: parsed_journey missing columns: {missing_j}")

    expected_c = {f.name for f in CALL_SCHEMA.fields}
    actual_c = set(call_df.columns) - {"_run_id", "_parsed_timestamp"}
    if missing_c := expected_c - actual_c:
        errors.append(f"P3: parsed_call missing columns: {missing_c}")

    # P4: DVJId null rate < 10%
    if j_count > 0:
        j_null_count = journey_df.filter(F.col("DVJId").isNull()).count()
        j_null_rate = j_null_count / j_count
        if j_null_rate > 0.10:
            errors.append(
                f"P4: DVJId null rate {j_null_rate:.1%} exceeds 10% threshold "
                f"({j_null_count}/{j_count})"
            )
        else:
            logger.info(f"P4: DVJId null rate {j_null_rate:.1%} OK ({j_null_count}/{j_count})")

    # P5: OperatingDate not null
    if j_count > 0:
        op_null = journey_df.filter(F.col("OperatingDate").isNull()).count()
        if op_null > 0:
            errors.append(
                f"P5: OperatingDate contains {op_null} NULL values"
            )

    if errors:
        msg = (
            f"Prep validation FAILED ({len(errors)} checks):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
        logger.error(msg)
        raise PrepValidationError(msg)

    logger.info(
        f"Prep validation PASSED: {j_count:,} journeys, {c_count:,} calls"
    )
