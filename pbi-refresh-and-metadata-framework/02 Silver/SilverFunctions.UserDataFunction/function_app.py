import json
import logging
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import fabric.functions as fn

udf = fn.UserDataFunctions()

# Values treated as null after trimming (compared case-insensitively)
NULL_TOKENS = {"", "null", "none", "n/a", "na", "nan", "-", "--", "?"}

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

ISO_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d", "%Y%m%d",
)
DAY_FIRST_FORMATS = (
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
    "%d-%m-%Y %H:%M:%S", "%d-%m-%Y", "%d.%m.%Y", "%d %b %Y", "%d %B %Y",
)
MONTH_FIRST_FORMATS = (
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y", "%m-%d-%Y", "%b %d %Y", "%B %d %Y", "%b %d, %Y", "%B %d, %Y",
)

DATETIME_OUTPUTS = ("datetime", "midnight", "date")

TRUE_TOKENS = {"true", "t", "yes", "y", "1"}
FALSE_TOKENS = {"false", "f", "no", "n", "0"}


# ============================================================
# Shared helpers
# Plain Python, so the scalar functions and clean_table run the same logic.
# Parameter names are camelCase to match the user data function parameters.
# ============================================================

def _is_missing(value):
    """True for None, NaN, pd.NA and NaT. False for lists, dicts and other values."""
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _clean(value):
    """Trim, collapse whitespace, and map placeholder values to None."""
    if _is_missing(value):
        return None
    text = str(value).replace("\u200b", "")
    text = re.sub(r"\s+", " ", text).strip()
    return None if text.lower() in NULL_TOKENS else text


def _parse_datetime(cleaned, dayFirst):
    """Parse a cleaned string into a datetime, or return None."""
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        pass

    regional = DAY_FIRST_FORMATS if dayFirst else MONTH_FIRST_FORMATS
    for fmt in ISO_FORMATS + regional:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def _standardise_email(email):
    cleaned = _clean(email)
    if cleaned is None:
        return None

    cleaned = cleaned.lower().replace(" ", "")
    if cleaned.startswith("mailto:"):
        cleaned = cleaned[len("mailto:"):]

    if not EMAIL_PATTERN.match(cleaned):
        logging.warning("standardise_email: value rejected as invalid")
        return None
    return cleaned


def _standardise_datetime(value, sourceTimezone="", output="midnight", dayFirst=True):
    if output not in DATETIME_OUTPUTS:
        raise fn.UserThrownError(
            f"output must be one of {list(DATETIME_OUTPUTS)}", {"output": output}
        )

    source_tz = None
    if sourceTimezone:
        try:
            source_tz = ZoneInfo(sourceTimezone)
        except (ZoneInfoNotFoundError, ValueError, TypeError):
            raise fn.UserThrownError("Unknown timezone", {"sourceTimezone": sourceTimezone})

    cleaned = _clean(value)
    if cleaned is None:
        return None

    parsed = _parse_datetime(cleaned, dayFirst)
    if parsed is None:
        logging.warning("standardise_datetime: could not parse value")
        return None

    if source_tz is not None:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=source_tz)
        parsed = parsed.astimezone(timezone.utc)

    if output == "date":
        return parsed.date().isoformat()

    if output == "midnight":
        parsed = parsed.replace(hour=0, minute=0, second=0, microsecond=0)

    if source_tz is not None:
        return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
    return parsed.replace(microsecond=0).isoformat()


def _to_boolean(value):
    if isinstance(value, bool):
        return value

    cleaned = _clean(value)
    if cleaned is None:
        return None

    token = cleaned.lower()
    if token in TRUE_TOKENS:
        return True
    if token in FALSE_TOKENS:
        return False

    logging.warning("to_boolean: unrecognised flag value")
    return None


def _clean_numeric(value, decimalComma=False):
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return None if _is_missing(value) else float(value)

    cleaned = _clean(value)
    if cleaned is None:
        return None

    negative = cleaned.startswith("(") and cleaned.endswith(")")

    if decimalComma:
        cleaned = cleaned.replace(".", "").replace(",", ".")

    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    if negative:
        cleaned = "-" + cleaned.lstrip("-")

    try:
        return float(cleaned)
    except ValueError:
        logging.warning("clean_numeric: could not convert value")
        return None


CLEANING_RULES = {
    "clean_text": _clean,
    "standardise_email": _standardise_email,
    "clean_numeric": _clean_numeric,
    "to_boolean": _to_boolean,
    "standardise_datetime": _standardise_datetime,
}


# ============================================================
# Scalar functions (one value in, one value out)
# ============================================================

@udf.function()
def clean_text(value: str) -> str:
    """Trim, collapse internal whitespace (including non-breaking spaces),
    and return null for empty or placeholder values such as 'N/A' or 'NULL'."""
    return _clean(value)


@udf.function()
def standardise_email(email: str) -> str:
    """Lower-case and trim an email address, removing spaces and any 'mailto:' prefix.
    Returns null if it is not a valid shape."""
    return _standardise_email(email)


@udf.function()
def standardise_datetime(
    value: str,
    sourceTimezone: str = "",
    output: str = "midnight",
    dayFirst: bool = True,
) -> str:
    """Parse a date or datetime in a range of common formats, remove the time and
    return it in a standard ISO form. Returns null if the value cannot be parsed.

    sourceTimezone: an IANA name such as 'Europe/London' or 'America/Los_Angeles'.
        If set, the value is converted to UTC. Values without an offset are treated
        as local time in this zone; values that already carry an offset ('+02:00'
        or 'Z') are converted from that offset. Pass 'UTC' to convert offset values
        while treating values without an offset as UTC. If empty, no conversion
        happens and any offset is kept.
    output:
        'midnight' -> 2024-01-15T00:00:00 (the default): time removed but still a
                      datetime, useful when a datetime column has to join to a date table
        'date'     -> 2024-01-15
        'datetime' -> 2024-01-15T14:35:22, keeps the time
    dayFirst: whether 03/04/2024 is read as 3 April (True) or 4 March (False).

    The UTC conversion happens before the time is removed, so with 'midnight' or
    'date' the result is the UTC date, which can differ from the local date.
    """
    return _standardise_datetime(value, sourceTimezone, output, dayFirst)


@udf.function()
def to_boolean(value: str) -> bool:
    """Map common flag values (Y/N, Yes/No, True/False, 1/0, T/F) to a boolean.
    Returns null for anything unrecognised."""
    return _to_boolean(value)


@udf.function()
def clean_numeric(value: str, decimalComma: bool = False) -> float:
    """Convert a messy numeric string to a number. Removes currency symbols and codes,
    thousands separators and spaces, and treats accounting brackets as negative,
    so '($1,234.50)' becomes -1234.5. Set decimalComma for values like '1.234,50'.
    Returns null if no valid number remains."""
    return _clean_numeric(value, decimalComma)


# ============================================================
# Table functions (pandas DataFrame in, pandas DataFrame out)
# Require fabric-user-data-functions 1.0.0 or later (Library management).
# The combined request is limited to 4 MB, so these suit reference and
# dimension tables. Use Spark for large fact tables.
# ============================================================

@udf.function()
def json_to_table(jsonText: str, recordPath: str = "") -> pd.DataFrame:
    """Convert JSON text into a table.

    Accepts an array of objects, a single object, or an object that holds the array
    under recordPath. recordPath is dot-separated for nested keys, e.g. 'data.items'.
    Nested objects are flattened into columns such as 'address.city'.
    """
    try:
        payload = json.loads(jsonText)
    except (json.JSONDecodeError, TypeError) as e:
        raise fn.UserThrownError("Input is not valid JSON", {"error": str(e)})

    if recordPath:
        for key in recordPath.split("."):
            if not isinstance(payload, dict) or key not in payload:
                raise fn.UserThrownError("recordPath not found in JSON", {"recordPath": recordPath})
            payload = payload[key]

    if isinstance(payload, dict):
        payload = [payload]

    if not isinstance(payload, list) or not all(isinstance(r, dict) for r in payload):
        raise fn.UserThrownError(
            "JSON must be an object or an array of objects (set recordPath if the array is nested)",
            {"recordPath": recordPath},
        )

    return pd.json_normalize(payload)


@udf.function()
def clean_table(data: pd.DataFrame, columnRules: dict) -> pd.DataFrame:
    """Apply the cleaning rules to whole columns in one call.

    columnRules maps a column name to a scalar function name, or to an object with a
    'rule' key plus that function's options (the same parameter names):
        {
            "VendorName": "clean_text",
            "Email": "standardise_email",
            "BaseFare": "clean_numeric",
            "IsElectronic": "to_boolean",
            "PickupTime": {"rule": "standardise_datetime", "sourceTimezone": "America/New_York"},
            "Amount": {"rule": "clean_numeric", "decimalComma": true}
        }
    Rules: clean_text, standardise_email, clean_numeric, to_boolean, standardise_datetime.
    Columns not listed are returned unchanged.
    """
    result = data.copy()

    for column, spec in (columnRules or {}).items():
        if column not in result.columns:
            raise fn.UserThrownError(
                "Column not found", {"column": column, "available": list(result.columns)}
            )

        if isinstance(spec, str):
            rule, options = spec, {}
        elif isinstance(spec, dict):
            options = dict(spec)
            rule = options.pop("rule", None)
        else:
            raise fn.UserThrownError("Rule must be a name or an object", {"column": column})

        cleaner = CLEANING_RULES.get(rule)
        if cleaner is None:
            raise fn.UserThrownError(
                "Unknown rule", {"column": column, "rule": rule, "rules": list(CLEANING_RULES)}
            )

        try:
            result[column] = result[column].map(lambda v: cleaner(v, **options))
        except TypeError:
            raise fn.UserThrownError(
                "Invalid options for rule", {"column": column, "rule": rule, "options": options}
            )

    return result


@udf.function()
def dedupe_table(data: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where every column matches an earlier row, keeping the first."""
    return data.drop_duplicates().reset_index(drop=True)
