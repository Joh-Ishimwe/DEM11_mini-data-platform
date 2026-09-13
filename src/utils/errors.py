"""Exception hierarchy.

The whole point: separate failures that are worth retrying from failures that
are not. Retrying a 404 or a malformed file just wastes ten minutes before
failing anyway, and it hides the real cause.

    PipelineError
      ├── TransientError    -> retry with backoff  (timeout, 5xx, conn refused)
      └── PermanentError    -> fail fast           (404, bad schema, bad input)
"""


class PipelineError(Exception):
    """Base class. Catch this to catch anything the pipeline raises on purpose."""


class TransientError(PipelineError):
    """Something that might succeed if tried again shortly.

    Examples: connection refused, read timeout, HTTP 500/502/503, deadlock.
    """


class PermanentError(PipelineError):
    """Retrying will not help. Fail fast and loudly.

    Examples: HTTP 404, bucket does not exist, file is not valid CSV,
    required column missing, credentials rejected (401/403).
    """


class ValidationError(PermanentError):
    """Data did not meet a FATAL expectation. The run should stop."""


class RowRejected(PipelineError):
    """A single record failed a QUARANTINE check.

    This is deliberately NOT fatal. It is caught per-record, written to
    staging.rejected_rows, and the run continues. One bad record must never
    kill a pipeline run.
    """

    def __init__(self, reason: str, record: dict | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.record = record or {}
