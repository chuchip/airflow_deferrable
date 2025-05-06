import asyncio
import aiohttp
import json
import ssl
from typing import Any, Dict, Tuple, Optional, Sequence, Union

from airflow.providers.http.hooks.http import HttpHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowException
from json import JSONDecodeError

class JsonException(Exception):
    pass

# Optional: Set up basic logging if running the trigger standalone for testing
# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger("HttpPollingTrigger")
# Use self.log within the class methods when running via Airflow

class HttpPollingTrigger(BaseTrigger):
    """
    Polls an HTTP endpoint asynchronously, checks a JSON response field ('response' by default),
    and handles retries on HTTP failures.

    Yields:
        TriggerEvent: Based on polling result ('success', 'failure', 'error').
    """

    def __init__(
        self,
        http_conn_id: str,
        endpoint: str,
        method: str = "GET",
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        response_field: str = "response", # Default field to check
        success_value: Any = "success",
        failure_values: Sequence[Any] = ("failure",), # Default failure value
        poke_interval: float = 5.0,
        http_check_retries: int = 3, # Number of *retries* after initial failure (total 4 attempts)
        retry_delay: float = 1.0, # Delay before retrying a FAILED http call
        # Note: Overall timeout is handled by the operator's deferral timeout
        **kwargs: Any,
    ):
        # Call super().__init__ FIRST to ensure 'log' is initialized.
        super().__init__(**kwargs)

        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers or {}
        self.response_field = response_field
        self.success_value = success_value
        # Use a set for efficient lookup of failure values
        self.failure_values = set(failure_values)
        self.poke_interval = poke_interval
        self.http_check_retries = http_check_retries
        self.retry_delay = retry_delay
        self.hook = HttpHook(method=self.method, http_conn_id=self.http_conn_id)


    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes Trigger arguments and classpath."""
        return (
            # Adjust the path if your plugin structure is different
            "http_polling_plugin.triggers.http_polling_trigger.HttpPollingTrigger",
            {
                "http_conn_id": self.http_conn_id,
                "endpoint": self.endpoint,
                "method": self.method,
                "data": self.data,
                "headers": self.headers,
                "response_field": self.response_field,
                "success_value": self.success_value,
                 # Serialize set back to list for JSON compatibility
                "failure_values": list(self.failure_values),
                "poke_interval": self.poke_interval,
                "http_check_retries": self.http_check_retries,
                "retry_delay": self.retry_delay,
            },
        )

    async def _make_http_call(self, session: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:
        total_attempts = 1 + self.http_check_retries
        url=self.endpoint
        
        self.log.info("- Headers: {self.headers}")
        self.log.info(f"- Body: {self.data}")
        data=self.data
        log_error=""
        try:
            if data is not None:
                data = json.load(data)
        except Exception as e:
            log_error= f"Failed to load JSON data {e}"
            self.log.warning("Failed to load JSON data: {e}")
        log_error+= f"Headers: {self.headers} (Type: {type(self.headers)}) - Body: {data} - URL: {url} Method: {self.method} "
        for attempt in range(total_attempts):
            self.log.info(f"Attempt {attempt + 1}/{total_attempts}: Calling {self.method} {url}")
            try:
                async with session.request(
                    self.method, url, json=data, headers=self.headers,ssl=False
                ) as response:
                    response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                    try:
                        result = await response.json()
                        self.log.debug(f"Response JSON received: {result}")
                        if result is None:
                         # _make_http_call failed after all its retries
                            raise JsonException(f'Result was None {log_error}')
                        if self.response_field not in result:                             
                             raise  JsonException(f"Field '{self.response_field}' not found in response {{result}}. {log_error}" )
                        return result # Success!
                    except (JSONDecodeError, JsonException,aiohttp.ContentTypeError) as json_err:
                         # Treat inability to parse JSON as a failure of this attempt
                         log_error+= f"Failed to decode JSON response: {response} (attempt {attempt + 1}): {json_err}"
                         self.log.warning(f"Failed to decode JSON response (attempt {attempt + 1}): {json_err}")                   

            except aiohttp.ClientResponseError as e:
                # Specific HTTP status errors
                log_error+= f"HTTP check failed (attempt {attempt + 1}): Status {e.status} - {e.message}"
                self.log.warning(f"HTTP check failed (attempt {attempt + 1}): Status {e.status} - {e.message}")
                # Don't retry client errors (4xx) by default unless configured otherwise
                if 400 <= e.status < 500:
                    self.log.error("Client error received. Failing check.")
                    return None # Indicate non-retriable failure for this check cycle
            except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionRefusedError) as e:
                 # Other connection/network errors - these are typically retriable
                 log_error+= f"HTTP check failed (attempt {attempt + 1}): {type(e).__name__} - {e}"
                 self.log.warning(f"HTTP check failed (attempt {attempt + 1}): {type(e).__name__} - {e}")
            except Exception as e:
                 # Catch unexpected errors during the request
                 log_error+= f"An unexpected error occurred during HTTP check (attempt {attempt + 1}): {e} "
                 self.log.exception(f"An unexpected error occurred during HTTP check (attempt {attempt + 1}): {e} ")
                 
            
            # If we are here, the attempt failed and we might retry
            if attempt < self.http_check_retries:
                self.log.info(f"Waiting {self.retry_delay}s before next HTTP check retry...")
                await asyncio.sleep(self.retry_delay)
            else:
                 # This was the last attempt
                 self.log.error("HTTP check failed after all retries.")
                 raise Exception(f"HTTP check failed after {total_attempts} attempts. {log_error}")  
                 

        # Should technically not be reached if logic is correct, but safety return
        raise Exception(f"Output not controllled {total_attempts} attempts. {log_error}")  


    async def run(self):
        self.log.info("Executing run... ")
        """Main polling loop run by the Triggerer."""
        try:
             # Create session within run using connection details from hook
            async with aiohttp.ClientSession() as session:
                while True:
                    json_response = await self._make_http_call(session)
                   
                    # Check the response field if the call was successful
                    try:
                        if json_response is None:
                            self.log.error("No response received. Exiting trigger")
                            yield TriggerEvent({"status": "error", "message": f"No response received. json_response: {json_response}"})
                        status_value = json_response.get(self.response_field)
                        self.log.info(f"Found status value: '{status_value}' in field '{self.response_field}'")

                        if status_value == self.success_value:
                            self.log.info("Success condition met.")
                            yield TriggerEvent({
                                "status": "success",
                                "message": f"Condition met: field '{self.response_field}' is '{status_value}'.",
                                "value": status_value # Include the value in the event
                            })
                            return # Exit trigger loop
                        elif status_value in self.failure_values:
                            self.log.warning(f"Failure condition met: field '{self.response_field}' is '{status_value}'.")
                            yield TriggerEvent({
                                "status": "failure",
                                "message": f"Failure condition met: field '{self.response_field}' is '{status_value}'.",
                                "value": status_value
                            })
                            return # Exit trigger loop
                        else:
                            # Condition not met, continue polling
                            self.log.info(f"Status '{status_value}' is not terminal. Waiting {self.poke_interval}s...")

                    except KeyError as e:
                         self.log.error(f"Response field error: {e}")
                         yield TriggerEvent({"status": "error", "message": str(e)})
                         return # Exit trigger loop on unexpected response structure
                    except Exception as e: # Catch other potential errors during check
                         self.log.exception(f"Error checking response status: {e}")
                         yield TriggerEvent({"status": "error", "message": f"Error checking response: {e}"})
                         return # Exit trigger loop

                    # Wait before the next poll if no terminal state was reached
                    await asyncio.sleep(self.poke_interval)

        except asyncio.CancelledError:
            # Trigger job was cancelled - log and exit gracefully
            self.log.info("Trigger was cancelled.")
            # Yield nothing, the deferring task instance is marked as failed upstream.
        except Exception as e:
            # Catch-all for unexpected errors in the main run loop
            self.log.exception("Unhandled exception in trigger run loop:")
            yield TriggerEvent({"status": "error", "message": f"Trigger failed: {str(e)}"})

