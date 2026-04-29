import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

_PARTICIPANT_ID_RE = re.compile(r'_([a-zA-Z0-9]{8})\.csv$')


def extract_participant_id(object_key: str):
    """
    Extract the 8-character hex participant ID from an S3 object key.
    Expects filenames ending in  _<8chars>.csv  e.g.
      250204_heartrate_Testing-1_cd037752.csv  →  'cd037752'
    Returns None when the pattern is not found.
    """
    match = _PARTICIPANT_ID_RE.search(object_key)
    return match.group(1) if match else None


def format_iso_timestamp(iso_date_string: str):
    """
    Convert an ISO 8601 date string to 'YYYY-MM-DD HH:MM' (seconds zeroed).
    Handles both UTC 'Z' suffix and +HH:MM offsets.
    Returns None when parsing fails so callers can detect bad input.
    """
    try:
        dt = datetime.fromisoformat(iso_date_string.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M')
    except Exception as e:
        logger.warning("Could not parse ISO date '%s': %s", iso_date_string, e)
        return None


def write_placeholder(s3_client, bucket: str, key: str) -> None:
    """
    Write a zero-byte CSV placeholder to S3.

    Used when a processing Lambda encounters an empty or unreadable source file.
    The placeholder counts as a processed file so batch checkers can still reach
    their expected count and EventBridge is not stalled indefinitely.
    Glue jobs and merge Lambdas skip zero-byte files when reading back.
    """
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=b"", ContentType='text/csv')
        logger.info("Wrote zero-byte placeholder: s3://%s/%s", bucket, key)
    except Exception as e:
        logger.warning("Could not write placeholder for s3://%s/%s: %s", bucket, key, e)