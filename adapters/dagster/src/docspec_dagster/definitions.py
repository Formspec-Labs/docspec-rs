"""Native Dagster resource and op; Rust owns all retained lifecycle decisions."""

from dagster import Config, ConfigurableResource, Failure, OpExecutionContext, RetryRequested, job, op
from pydantic import Field

from .client import AdapterError, DocSpecClient, RemoteRefusal, ResponseError, UncertainOutcome


class DocSpecResource(ConfigurableResource):
    """Connect each Dagster worker to the existing loopback Rust host."""

    base_url: str = "http://127.0.0.1:8080"
    timeout_seconds: float = Field(default=60.0, gt=0, allow_inf_nan=False)
    max_response_bytes: int = Field(default=1 << 20, ge=1, le=32 << 20)
    uncertain_retries: int = Field(default=2, ge=0, le=10)
    retry_delay_seconds: float = Field(default=1.0, ge=0, allow_inf_nan=False)

    def submit(self, route: str, request_json: str) -> dict:
        return DocSpecClient(self.base_url, self.timeout_seconds, self.max_response_bytes).submit(route, request_json)


class OperationConfig(Config):
    """Exact native request JSON, held unchanged by Dagster across step retries."""

    route: str = "/run"
    request_json: str


@op
def docspec_operation(context: OpExecutionContext, config: OperationConfig, docspec: DocSpecResource) -> dict:
    """Submit one native operation and emit only bounded identity metadata."""
    try:
        result = docspec.submit(config.route, config.request_json)
    except RemoteRefusal as error:
        raise Failure(
            description=str(error),
            metadata={
                "code": error.code,
                "http_status": error.status,
                "message": error.message[:2048],
                "message_truncated": len(error.message) > 2048,
            },
            allow_retries=False,
        ) from None
    except UncertainOutcome:
        if context.retry_number < docspec.uncertain_retries:
            context.log.warning("DocSpec outcome is unknown; retrying the unchanged native request.")
            raise RetryRequested(max_retries=docspec.uncertain_retries, seconds_to_wait=docspec.retry_delay_seconds) from None
        raise Failure(
            description="DocSpec outcome remains unknown. Retain and resubmit the same native request to reconcile it.",
            metadata={"outcome": "unknown"},
            allow_retries=False,
        ) from None
    except ResponseError as error:
        raise Failure(
            description=f"{error}. Retain and resubmit the same native request after resolving the response problem.",
            metadata={"outcome": "unknown"},
            allow_retries=False,
        ) from None
    except AdapterError as error:
        raise Failure(description=str(error), allow_retries=False) from None
    context.add_output_metadata(result)
    return result


@job
def docspec_job():
    """Run one configured request using a caller-supplied docspec resource."""
    docspec_operation()
