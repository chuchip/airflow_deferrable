from typing import Any, Dict, Optional, Sequence

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

# Import the trigger (adjust path as necessary)
from http_polling_plugin.triggers.http_polling_trigger import HttpPollingTrigger

class HttpPollingDeferrableOperator(BaseOperator):
    """
    Defers to HttpPollingTrigger to poll an HTTP endpoint until a condition is met.

    :param http_conn_id: Airflow HTTP connection ID
    :param endpoint: The endpoint to poll (relative to the connection base URL)
    :param method: HTTP method (GET, POST, etc.)
    :param data: Data to send with the request (for POST/PUT)
    :param headers: Headers to send with the request
    :param response_field: The key in the JSON response to check (e.g., 'response')
    :param success_value: The value indicating successful completion (e.g., 'success')
    :param failure_values: A sequence of values indicating failure (e.g., ['failure', 'error'])
    :param poke_interval: Time in seconds to wait between polls when deferred
    :param http_check_retries: Number of retries for a failed HTTP call within the trigger
    :param retry_delay: Time in seconds to wait before retrying a failed HTTP call
    :param timeout: Overall time in seconds this operator should wait (optional)
    """

    # Define which fields are passed to the Trigger and templated by Airflow
    template_fields: Sequence[str] = (
        "endpoint",
        "data",
        "headers",
        "response_field",
        "success_value",
        "failure_values",
    )

    @apply_defaults
    def __init__(
        self,
        *, # Make arguments keyword-only
        http_conn_id: str,
        endpoint: str,
        response_field: str = "status",
        success_value: Any = "success",
        failure_values: Sequence[Any] = ("failure", "error"),
        poke_interval: float = 30.0,
        method: str = "GET",
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        http_check_retries: int = 3,
        retry_delay: float = 5.0,
        timeout: Optional[float] = None, # Overall operator timeout
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.response_field = response_field
        self.success_value = success_value
        self.failure_values = failure_values
        self.poke_interval = poke_interval
        self.http_check_retries = http_check_retries
        self.retry_delay = retry_delay
        self.timeout = timeout # Store timeout

    def execute(self, context: Dict[str, Any]) -> None:
        """Defers execution to the Trigger."""
        self.log.info(
            f"Deferring task {self.task_id}  to poll endpoint {self.endpoint} "
            f"every {self.poke_interval}s. Checking field '{self.response_field}' "
            f"for success ('{self.success_value}') or failure ({self.failure_values})."
        )
        self.defer(
            trigger=HttpPollingTrigger(
                http_conn_id=self.http_conn_id,
                endpoint=self.endpoint,
                method=self.method,
                data=self.data,
                headers=self.headers,
                response_field=self.response_field,
                success_value=self.success_value,
                failure_values=self.failure_values,
                poke_interval=self.poke_interval,
                http_check_retries=self.http_check_retries,
                retry_delay=self.retry_delay,
                timeout=self.timeout, # Pass timeout to trigger if needed
            ),
            method_name="execute_complete", # Method to call upon trigger firing
            timeout=self.execution_timeout, # Use operator's overall timeout if set
        )

    def execute_complete(self, context: Dict[str, Any], event: Optional[Dict[str, Any]] = None) -> None:
        """Callback for when the trigger fires."""
        self.log.info(f"Trigger event received for {self.task_id}: {event}")

        if event is None:
            # Should not happen with a properly implemented trigger but handle defensively
            raise AirflowException(f"Trigger for task {self.task_id} returned None event!")

        status = event.get("status")
        message = event.get("message", "No message received.")
        value = event.get("value") # Get the actual value found

        if status == "success":
            self.log.info(f"Task {self.task_id} completed successfully: {message}")
            # Optionally push the successful value to XCom
            if value is not None:
                 self.xcom_push(context=context, key="status_value", value=value)
            return # Task succeeds

        elif status == "failure":
             self.log.error(f"Task {self.task_id} failed: {message}")
             # Optionally push the failure value to XCom
             if value is not None:
                 self.xcom_push(context=context, key="status_value", value=value)
             raise AirflowException(f"Polling failed: {message}") # Task fails

        elif status == "error":
            self.log.error(f"Task {self.task_id} encountered an error: {message}")
            raise AirflowException(f"Trigger error: {message}") # Task fails

        else:
            # Should not happen
            raise AirflowException(f"Received unexpected status from trigger: {status}")