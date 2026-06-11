#!/usr/bin/env python3
# /// script
# dependencies = ["httpx", "rich", "typer", "python-dotenv", "jinja2", "typing-extensions", "asyncssh"]
# ///

import asyncio
import json
import math
import signal
import sys
import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import httpx
import typer
from dotenv import load_dotenv
from jinja2 import Template
from rich.console import Console
from rich.live import Live
from rich.pretty import Pretty
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text
from typing_extensions import Annotated

load_dotenv()

app = typer.Typer(
    help="zPod VCF Deployer - Unified deployment and VCF depot management tool",
    rich_markup_mode="rich",
    no_args_is_help=True,
)

console = Console()
global_debug = False
monitoring_active = False
# SDDC milestone span (seconds), set when initiate_sddc_deployment reaches a
# terminal state. Drives the "Deploying SDDC completed in" line — script
# wall-clock under-counts it on a resumed run.
final_deployment_seconds = None
# ISO timestamps spanning the full deployment, captured from the APIs so that
# the bottom "Total deployment time" is accurate even when the script is
# relaunched: zPodFactory's /zpods returns creation_date for the start, and
# the last VCF milestone's updateTimestamp marks the end.
zpod_creation_iso = None
sddc_end_iso = None


class _TimestampedLogFile:
    """File wrapper that prefixes every written line with a [timestamp].

    Rich's Console writes content in arbitrary chunks (sometimes a partial
    line, sometimes several lines at once), so writes are tracked on line
    boundaries and a timestamp is emitted at the start of each line. Any
    other file attribute (flush, isatty, encoding, ...) proxies to the
    underlying handle.
    """

    def __init__(self, handle):
        self._handle = handle
        self._at_line_start = True

    def write(self, text):
        if not text:
            return
        chunks = []
        for line in text.splitlines(keepends=True):
            if self._at_line_start:
                chunks.append(datetime.now().strftime("[%Y-%m-%d %H:%M:%S] "))
            chunks.append(line)
            self._at_line_start = line.endswith("\n")
        self._handle.write("".join(chunks))

    def __getattr__(self, name):
        return getattr(self._handle, name)


class DebugConsole:
    """Routes verbose debug output to the screen, a log file, or both.

    ``--debug`` enables the screen target; ``--debug-log`` enables a
    timestamped log file. Debug call sites use ``debug_console.print(...)``
    so the main ``console`` (normal output) stays clean when only
    ``--debug-log`` is active.
    """

    def __init__(self):
        self._screen = None
        self._file_console = None
        self._file_handle = None
        self.logfile_path = None

    def enable_screen(self, screen_console):
        self._screen = screen_console

    def enable_logfile(self, path):
        """Open a log file and attach a (color-free) Console writing to it."""
        self._file_handle = open(path, "w", encoding="utf-8")
        self.logfile_path = path
        self._file_console = Console(
            file=_TimestampedLogFile(self._file_handle),
            width=160,
            force_terminal=False,
            no_color=True,
            highlight=False,
            soft_wrap=False,
        )

    def print(self, *args, **kwargs):
        if self._screen is not None:
            self._screen.print(*args, **kwargs)
        if self._file_console is not None:
            self._file_console.print(*args, **kwargs)

    def close(self):
        if self._file_handle is not None:
            try:
                self._file_handle.flush()
                self._file_handle.close()
            except Exception:
                pass
            self._file_handle = None


debug_console = DebugConsole()


def version_callback(value: bool):
    """
    Display version information and exit.

    Args:
        value (bool): Whether to show version (triggered by --version flag)

    Raises:
        typer.Exit: Always exits after displaying version
    """
    if value:
        console.print("zPod VCF Deployer version 1.0.0")
        raise typer.Exit()


@app.command(
    help="""
zPod VCF Deployer - Unified deployment and VCF depot management tool

[bold yellow]Examples:[/bold yellow]

[bold cyan]Online Mode:[/bold cyan]
[blue]uv run zpod-vcf-deployer.py \\
  --vcf-json-template config/v902_std_3hosts.json \\
  --zpod-name my-zpod \\
  --zpodfactory-profile vcf \\
  --zpodfactory-endpoint my-endpoint \\
  --zpodfactory-access-token your-token \\
  --zpodfactory-base-url http://zpodfactory.example.com:8000 \\
  --depot-mode online \\
  --online-depot-download-token your-download-token[/blue]

[bold cyan]Offline Mode:[/bold cyan]
[blue]uv run zpod-vcf-deployer.py \\
  --vcf-json-template config/v902_std_3hosts.json \\
  --zpod-name my-zpod \\
  --zpodfactory-profile vcf \\
  --zpodfactory-endpoint my-endpoint \\
  --zpodfactory-access-token your-token \\
  --zpodfactory-base-url http://zpodfactory.example.com:8000 \\
  --depot-mode offline \\
  --offline-depot-hostname depot.example.com \\
  --offline-depot-username depot-user \\
  --offline-depot-password depot-password[/blue]
"""
)
def main(
    vcf_json_template: Annotated[
        typer.FileText,
        typer.Option(
            "--vcf-json-template",
            "-j",
            help="VCF JSON template file",
            envvar="VCF_JSON_TEMPLATE",
        ),
    ],
    zpod_name: Annotated[
        str,
        typer.Option("--zpod-name", "-n", help="zPod name"),
    ],
    zpodfactory_profile: Annotated[
        str,
        typer.Option(
            "--zpodfactory-profile",
            "-p",
            help="zPodFactory profile name",
            envvar="ZPODFACTORY_DEFAULT_PROFILE",
        ),
    ],
    zpodfactory_endpoint: Annotated[
        str,
        typer.Option(
            "--zpodfactory-endpoint",
            "-e",
            help="zPodFactory endpoint name",
            envvar="ZPODFACTORY_DEFAULT_ENDPOINT",
        ),
    ],
    zpodfactory_access_token: Annotated[
        str,
        typer.Option(
            "--zpodfactory-access-token",
            "-a",
            help="zPodFactory access token",
            envvar="ZPODFACTORY_ACCESS_TOKEN",
        ),
    ],
    zpodfactory_base_url: Annotated[
        str,
        typer.Option(
            "--zpodfactory-base-url",
            "-u",
            help="zPodFactory base URL",
            envvar="ZPODFACTORY_BASE_URL",
        ),
    ],
    depot_mode: Annotated[
        str,
        typer.Option(
            "--depot-mode",
            help="Depot mode (online or offline)",
            show_choices=True,
            envvar="VCF_DEPOT_MODE",
        ),
    ] = "offline",
    # Online depot options
    online_depot_download_token: Annotated[
        str,
        typer.Option(
            "--online-depot-download-token",
            help="Online depot download token",
            envvar="VCF_ONLINE_DEPOT_DOWNLOAD_TOKEN",
        ),
    ] = None,
    # Offline depot options
    offline_depot_hostname: Annotated[
        str,
        typer.Option(
            "--offline-depot-hostname",
            help="Offline depot hostname",
            envvar="VCF_OFFLINE_DEPOT_HOSTNAME",
        ),
    ] = None,
    offline_depot_username: Annotated[
        str,
        typer.Option(
            "--offline-depot-username",
            help="Offline depot username",
            envvar="VCF_OFFLINE_DEPOT_USERNAME",
        ),
    ] = None,
    offline_depot_password: Annotated[
        str,
        typer.Option(
            "--offline-depot-password",
            help="Offline depot password",
            envvar="VCF_OFFLINE_DEPOT_PASSWORD",
        ),
    ] = None,
    offline_depot_port: Annotated[
        int,
        typer.Option(
            "--offline-depot-port",
            help="Offline depot port",
            envvar="VCF_OFFLINE_DEPOT_PORT",
        ),
    ] = 443,
    verify_only: bool = typer.Option(
        False,
        "--verify-only",
        help="Run the SDDC validation and stop before deployment. Re-run "
        "without this flag to deploy the last validated SDDC spec.",
    ),
    debug: bool = typer.Option(
        False,
        "--debug",
        "-d",
        help="Enable debug output on screen (API headers, payloads, responses)",
        is_eager=True,
    ),
    debug_log: bool = typer.Option(
        False,
        "--debug-log",
        help="Write debug output to a timestamped log file in the current "
        "directory (keeps the screen output clean)",
        is_eager=True,
    ),
    version: bool = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show the version and exit.",
    ),
):
    """zPod VCF Deployer - Deploy zPod and configure VCF with depot management"""
    global global_debug
    # global_debug gates every debug block; it is True when debug output is
    # wanted on the screen (--debug) and/or in a log file (--debug-log).
    global_debug = debug or debug_log

    if debug:
        debug_console.enable_screen(console)
        console.print("[bold yellow]Debug mode enabled[/bold yellow]")

    if debug_log:
        logfile = (
            Path.cwd()
            / f"zpod-vcf-deployer-{zpod_name}-{time.strftime('%Y%m%d-%H%M%S')}.log"
        )
        debug_console.enable_logfile(logfile)
        debug_console.print(
            f"zPod VCF Deployer debug log — zPod '{zpod_name}' — "
            f"started {time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        console.print(
            f"[bold yellow]Debug logging enabled → {logfile}[/bold yellow]"
        )

    # Validate depot mode and required parameters
    if depot_mode.lower() not in ("online", "offline"):
        console.print(
            "[bold red]❌ --depot-mode must be either 'online' or 'offline'[/bold red]"
        )
        raise typer.Exit(code=1)

    if depot_mode.lower() == "online":
        if not online_depot_download_token:
            console.print(
                "[bold red]❌ --online-depot-download-token is required for "
                "online mode[/bold red]"
            )
            raise typer.Exit(code=1)
    else:  # offline mode
        if (
            not offline_depot_hostname
            or not offline_depot_username
            or not offline_depot_password
        ):
            console.print(
                "[bold red]❌ --offline-depot-hostname, --offline-depot-username, "
                "and --offline-depot-password are required for offline "
                "mode[/bold red]"
            )
            raise typer.Exit(code=1)

    # Run the deployment with overall timing
    start_time = time.perf_counter()
    console.print("[bold cyan]Starting zPod VCF deployment...[/bold cyan]")

    try:
        asyncio.run(
            _deploy_entry(
                vcf_json_template=vcf_json_template,
                zpod_name=zpod_name,
                zpodfactory_profile=zpodfactory_profile,
                zpodfactory_endpoint=zpodfactory_endpoint,
                zpodfactory_access_token=zpodfactory_access_token,
                zpodfactory_base_url=zpodfactory_base_url,
                depot_mode=depot_mode,
                online_depot_download_token=online_depot_download_token,
                offline_depot_hostname=offline_depot_hostname,
                offline_depot_username=offline_depot_username,
                offline_depot_password=offline_depot_password,
                offline_depot_port=offline_depot_port,
                verify_only=verify_only,
            )
        )
    finally:
        end_time = time.perf_counter()
        # Prefer the true end-to-end span: zPod creation_date → last VCF
        # milestone updateTimestamp. That covers zPod creation + depot
        # config + validation + deployment, and is accurate even on a
        # relaunched/resumed run. Fall back to the SDDC milestone span if
        # the zPod creation date wasn't captured, then to script wall-clock.
        total_seconds = None
        if zpod_creation_iso and sddc_end_iso:
            total_seconds = elapsed_seconds(zpod_creation_iso, sddc_end_iso)
        if total_seconds is None:
            total_seconds = (
                final_deployment_seconds
                if final_deployment_seconds is not None
                else end_time - start_time
            )
        time_str = format_time(total_seconds)
        console.print(
            f"[bold green]✅ Total deployment time: {time_str}[/bold green]"
        )
        if debug_log:
            debug_console.print(
                f"Total deployment time: {time_str} — "
                f"finished {time.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            console.print(
                f"[dim]Debug log written to {debug_console.logfile_path}[/dim]"
            )
            debug_console.close()


def format_time(total_time):
    hours = int(total_time // 3600)
    minutes = int((total_time % 3600) // 60)
    seconds = int(total_time % 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def timeit(func):
    """
    Decorator to measure and log function execution time.
    Handles both sync and async functions.

    Args:
        func: The function to be timed

    Returns:
        function: Wrapped function with timing functionality
    """

    # Get a human-readable name for the function
    func_names = {
        "deploy_zpod": "Creating zPod",
        "configure_vcf_depot_and_bundles": "Configuring VCF depot and downloading bundles",
        "initiate_sddc_validations": "Validating SDDC Spec",
        "initiate_sddc_deployment": "Deploying SDDC",
    }

    step_name = func_names.get(func.__name__, func.__name__)

    @wraps(func)
    async def async_timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time

        # For the SDDC deployment step, prefer the real milestone-derived
        # deployment time over the script wall-clock — the latter under-counts
        # when the script is relaunched to monitor an already-running deploy.
        if (
            func.__name__ == "initiate_sddc_deployment"
            and final_deployment_seconds is not None
        ):
            total_time = final_deployment_seconds

        # Format time to be more human-friendly
        time_str = format_time(total_time)

        # Always show timing for major steps
        console.print(
            f"[bold green]✓ {step_name} completed in {time_str}[/bold green]"
        )

        # Additional debug info if debug mode is enabled
        if global_debug:
            debug_console.print(f"[dim]Function {func.__name__} took {time_str}[/dim]\n")

        return result

    return async_timeit_wrapper


def format_size(size_bytes: int) -> str:
    """
    Convert bytes to human readable format.

    Args:
        size_bytes (int): Size in bytes

    Returns:
        str: Human readable size string (e.g., "1.5 GB")
    """
    if size_bytes == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]

    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_names[i]}"


def normalize_vcf_url(url: str) -> str:
    """
    Normalize VCF installer URL to ensure it has HTTPS protocol.

    Args:
        url (str): URL to normalize

    Returns:
        str: Normalized URL with HTTPS protocol

    Raises:
        ValueError: If URL uses HTTP protocol (not allowed for security)
    """
    if not url:
        return url

    url = url.rstrip("/")

    if url.startswith("http://"):
        raise ValueError(
            "Only HTTPS URLs are allowed for security reasons. "
            f"Please use 'https://{url[7:]}' instead of '{url}'"
        )

    if url.startswith("https://"):
        return url

    return f"https://{url}"


def setup_signal_handlers():
    """
    Setup signal handlers for graceful shutdown.

    Registers SIGINT handler to gracefully handle Ctrl+C interruptions
    during long-running operations like download monitoring.
    """

    def signal_handler(signum, frame):
        global monitoring_active
        if monitoring_active:
            console.print(
                "\n\n[bold yellow]⚠️ Interrupted by user (Ctrl+C)[/bold yellow]"
            )
            console.print("[cyan]🔄 Stopping download monitoring...[/cyan]")
            sys.exit(0)
        else:
            console.print(
                "\n\n[bold yellow]⚠️ Interrupted by user (Ctrl+C)[/bold yellow]"
            )
            console.print("[cyan]🔄 Cleaning up and shutting down gracefully...[/cyan]")
            sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)


def zpod_component_status_color(status):
    """
    Get color-coded string for zPod component status.

    Args:
        status (str): Component status (e.g., "ACTIVE", "BUILDING")

    Returns:
        str: Rich-formatted colored status string
    """
    colors = {"ACTIVE": "green", "BUILDING": "yellow"}
    color = colors.get(status, "white")
    return f"[{color}]{status}[/{color}]"


def gather_zpod_info(zpod):
    """
    Format zPod information for display.

    Args:
        zpod (dict): zPod data dictionary

    Returns:
        str: Formatted string with zPod status and components
    """
    # Sort components by IP address numerically for correct alphanumeric order
    sorted_components = sorted(
        zpod["components"],
        key=lambda x: tuple(int(part) for part in x["ip"].split(".")),
    )

    out = [
        f"Status: {zpod['status']}",
        "Components:",
        "\n".join(
            [
                f"  {zpod_component_status_color(x['status'])}: {x['fqdn']} ({x['ip']})"
                for x in sorted_components
            ]
        )
        or "  N/a",
    ]
    return "\n".join(out)


def summarize_response(result: Any) -> str:
    """Build a compact one-line summary of an API response for the debug log.

    Used in place of the full JSON dump for high-frequency polling calls so
    the debug log stays small. Surfaces the status fields and progress counts
    that actually matter when scanning a poll timeline.
    """
    if not isinstance(result, dict):
        return f"{type(result).__name__} payload"

    parts = [
        f"{key}={result[key]}"
        for key in ("status", "executionStatus", "resultStatus", "syncStatus")
        if key in result
    ]

    milestones = result.get("milestones")
    if isinstance(milestones, list) and milestones:
        done = sum(
            1 for m in milestones if m.get("status") == "COMPLETED_WITH_SUCCESS"
        )
        parts.append(f"milestones={done}/{len(milestones)}")

    subtasks = result.get("sddcSubTasks")
    if isinstance(subtasks, list) and subtasks:
        done = sum(
            1
            for s in subtasks
            if "COMPLETED_WITH_SUCCESS" in str(s.get("status", ""))
        )
        parts.append(f"subtasks={done}/{len(subtasks)}")

    return ", ".join(parts) if parts else "(no status fields)"


class VCFClient:
    """
    VCF API Client leveraging the VMware Cloud Foundation API.

    Provides async methods for authentication, API calls with automatic token refresh,
    and comprehensive error handling with retry logic. Supports depot configurations,
    bundle downloads, and SDDC operations.
    """

    def __init__(self, base_url: str, username: str, password: str):
        """
        Initialize VCF API client.

        Args:
            base_url (str): Base URL of the VCF API (e.g., https://vcfinstaller.domain.com)
            username (str): Username for authentication (typically admin@local)
            password (str): Password for authentication
        """
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.token = None
        # A generous read timeout: calls like POST /v1/sddcs take a while for
        # the installer to acknowledge. The httpx default (5s) is far too low
        # and caused premature ReadTimeouts.
        self.client = httpx.AsyncClient(
            verify=False,  # Skip SSL verification
            timeout=httpx.Timeout(180.0, connect=30.0),
        )

    async def _handle_connection_error(
        self,
        error: Exception,
        attempt: int,
        max_retries: int,
        retry_delay: int,
    ) -> bool:
        """Handle connection errors with retry logic"""
        if attempt < max_retries - 1:
            error_msg = f"{type(error).__name__}"
            # Transient — retries automatically; keep it off the normal console
            # (it only alarms the user) and log it for troubleshooting.
            debug_console.print(
                f"[yellow]⚠️ Service error, "
                f"Retrying in {retry_delay} seconds... "
                f"(Attempt {attempt + 1:2d}/{max_retries}) - {error_msg}[/yellow]"
            )
            await asyncio.sleep(retry_delay)
            return True
        else:
            console.print(
                f"[bold red]❌ Failed after {max_retries} attempts. "
                "Please check if the service is running.[/bold red]"
            )
            return False

    async def _make_request(
        self,
        method: str,
        url: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 10,
        retry_delay: int = 10,
        handle_401: bool = False,
        idempotent: bool = True,
    ) -> httpx.Response:
        """Make HTTP request with retry logic and error handling.

        ``idempotent`` must be False for requests that create a resource
        (e.g. POST /v1/sddcs). A timeout/connection error on such a request
        may mean the server already processed it, so retrying would create a
        duplicate — those requests are attempted exactly once.
        """
        if not idempotent:
            max_retries = 1
        for attempt in range(max_retries):
            try:
                response = await self.client.request(
                    method, url, json=data, headers=headers
                )

                if global_debug:
                    debug_console.print(
                        f"[dim]HTTP Status Code: {response.status_code}[/dim]"
                    )

                response.raise_for_status()
                return response

            except httpx.HTTPStatusError as e:
                # Handle 401 Unauthorized - refresh token and retry
                if (
                    handle_401
                    and e.response.status_code == 401
                    and attempt < max_retries - 1
                ):
                    if global_debug:
                        debug_console.print()
                        debug_console.print(
                            f"[yellow]⚠️ Authentication token expired (HTTP 401).[/yellow]"
                        )
                        debug_console.print(
                            f"[yellow]Refreshing token and retrying... (Attempt {attempt + 1:2d}/{max_retries})[/yellow]"
                        )
                    try:
                        await self.refresh_token()
                        # Update headers with new token for next attempt
                        if headers:
                            headers["Authorization"] = f"Bearer {self.token}"
                        if global_debug:
                            debug_console.print(
                                "[bold green]✓ Authentication successful[/bold green]"
                            )
                        continue
                    except Exception as refresh_error:
                        console.print(
                            f"[red]❌ Failed to refresh token: {refresh_error}[/red]"
                        )
                        raise Exception(f"Token refresh failed: {refresh_error}")

                # Handle other retryable errors
                elif (
                    e.response.status_code in [502, 503, 504]
                    and attempt < max_retries - 1
                ):
                    error_type = f"HTTP {e.response.status_code}"
                    debug_console.print(
                        f"[yellow]⚠️ Service error, "
                        f"Retrying in {retry_delay} seconds... "
                        f"(Attempt {attempt + 1:2d}/{max_retries}) - {error_type}[/yellow]"
                    )
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    # For non-retryable errors, build error message
                    error_msg = f"HTTP {e.response.status_code} error"
                    try:
                        error_body = e.response.json()
                        error_msg += f": {json.dumps(error_body, indent=2)}"
                    except Exception:
                        error_msg += f": {e.response.text}"

                    if global_debug:
                        debug_console.print(
                            f"[red]Error response ({e.response.status_code}): {error_msg}[/red]"
                        )
                    raise Exception(error_msg)

            except (
                httpx.ConnectError,
                httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.WriteTimeout,
            ) as e:
                if global_debug:
                    debug_console.print(
                        f"[red]Connection error: {type(e).__name__}: {str(e)}[/red]"
                    )
                if await self._handle_connection_error(
                    e, attempt, max_retries, retry_delay
                ):
                    continue
                else:
                    raise Exception(f"Connection error: {type(e).__name__}: {str(e)}")

            except Exception as e:
                if global_debug:
                    debug_console.print(
                        f"[red]Unexpected error: {type(e).__name__}: {str(e)}[/red]"
                    )
                if await self._handle_connection_error(
                    e, attempt, max_retries, retry_delay
                ):
                    continue
                else:
                    raise

    async def wait_until_ready(
        self, max_wait: int = 600, poll_interval: int = 15
    ) -> None:
        """Poll the VCF Installer API until it answers.

        The appliance returns ConnectError / HTTP 502-504 while it is still
        booting. Without this gate each subsequent step absorbs that boot
        window with its own per-call retry counter, which reads like a stuck
        or looping retry. This is a single, clearly-labelled readiness phase so
        those early retries are understood as "waiting for the installer", not
        repeated failures of the same API call.

        Treats connection errors and 502/503/504 as "still booting"; any other
        HTTP response (even 401/404/405) means the API server is answering.
        Returns once ready, or after ``max_wait`` seconds (then lets the normal
        per-call retry logic take over).
        """
        probe = f"{self.base_url}/v1/system/settings/depot/depot-sync-info"
        base_msg = (
            "[bold cyan]⏳ Waiting for VCF Installer API to come online "
            f"({self.base_url})…[/bold cyan]"
        )
        start = time.monotonic()
        # A single in-place spinner instead of one printed line per poll, so the
        # boot wait is one tidy status rather than a wall of "still booting" text.
        with console.status(base_msg, spinner="dots"):
            while True:
                try:
                    resp = await self.client.get(probe)
                    if resp.status_code in (502, 503, 504):
                        reason = f"HTTP {resp.status_code}"
                    else:
                        elapsed = int(time.monotonic() - start)
                        console.print(
                            "[bold green]✓ VCF Installer API is online "
                            f"(after {elapsed}s)[/bold green]"
                        )
                        return
                except (
                    httpx.ConnectError,
                    httpx.ConnectTimeout,
                    httpx.ReadTimeout,
                    httpx.WriteTimeout,
                    httpx.RemoteProtocolError,
                ) as e:
                    reason = type(e).__name__
                except Exception as e:
                    reason = type(e).__name__

                elapsed = int(time.monotonic() - start)
                if elapsed >= max_wait:
                    console.print(
                        f"[yellow]⚠️ VCF Installer still not responding after "
                        f"{elapsed}s ({reason}); proceeding anyway.[/yellow]"
                    )
                    return
                await asyncio.sleep(poll_interval)

    async def get_token(self) -> str:
        """Get access token from VCF API with retry logic"""
        url = f"{self.base_url}/v1/tokens"
        data = {"username": self.username, "password": self.password}

        if global_debug:
            debug_console.print(f"[dim]Making POST request to: {url}[/dim]")
            debug_console.print(f"[dim]Request data: {json.dumps(data, indent=2)}[/dim]")

        response = await self._make_request("POST", url, data=data, retry_delay=30)

        token_data = response.json()
        self.token = token_data.get("accessToken")

        if global_debug:
            debug_console.print(
                f"[dim]Token Response: {json.dumps(token_data, indent=2)}[/dim]"
            )
            debug_console.print("[bold green]✓ Authentication successful[/bold green]")

        return self.token

    async def refresh_token(self) -> str:
        """Refresh the access token"""
        if global_debug:
            debug_console.print("[dim]🔄 Refreshing authentication token...[/dim]")

        # Clear the current token to force a new authentication
        self.token = None
        return await self.get_token()

    async def api_call(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        idempotent: bool = True,
        log_response: bool = True,
    ) -> Dict[str, Any]:
        """Make API call with authentication and automatic token refresh.

        Pass ``idempotent=False`` for resource-creating POSTs so a timeout is
        not retried into a duplicate resource.

        Pass ``log_response=False`` for high-frequency polling calls (the 5s
        deployment/validation/depot status polls). Their full JSON bodies are
        huge and near-identical across thousands of polls — dumping every one
        is what bloated debug logs to hundreds of MB. A one-line summary is
        logged instead; callers dump the full body once on a terminal status.
        """
        if not self.token:
            await self.get_token()

        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        if global_debug:
            debug_console.print(f"\n[dim]Making {method} request to: {url}[/dim]")
            if data:
                debug_console.print(f"[dim]Request data: {json.dumps(data, indent=2)}[/dim]")

        response = await self._make_request(
            method, url, data=data, headers=headers, handle_401=True,
            idempotent=idempotent,
        )

        # Handle HTTP 204 (No Content) responses
        if response.status_code == 204:
            if global_debug:
                debug_console.print(
                    f"[dim]Response ({response.status_code}): "
                    "No Content (empty response body)[/dim]"
                )
            return {"status": "success", "message": "No content returned"}

        result = response.json()
        if global_debug:
            if log_response:
                debug_console.print(
                    f"[dim]Response ({response.status_code}): "
                    f"{json.dumps(result, indent=2)}[/dim]"
                )
            else:
                # High-frequency poll: log a compact one-liner instead of the
                # full body. Callers dump the full JSON once on terminal state.
                debug_console.print(
                    f"[dim]Response ({response.status_code}) "
                    f"{method} {endpoint}: {summarize_response(result)}[/dim]"
                )
        return result

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


def phase_banner(step: int, title: str) -> None:
    """Labelled phase banner with a blank line before and after, to separate
    automation phases."""
    console.print(f"\n[bold cyan]━━ [{step}/7] {title} ━━[/bold cyan]\n")


async def _deploy_entry(
    vcf_json_template: str,
    zpod_name: str,
    zpodfactory_profile: str,
    zpodfactory_endpoint: str,
    zpodfactory_access_token: str,
    zpodfactory_base_url: str,
    depot_mode: str,
    online_depot_download_token: str = None,
    offline_depot_hostname: str = None,
    offline_depot_username: str = None,
    offline_depot_password: str = None,
    offline_depot_port: int = 443,
    verify_only: bool = False,
):
    """
    Main deployment entry point orchestrating the entire VCF deployment process.

    Args:
        vcf_json_template (str): VCF JSON template file content
        zpod_name (str): Name of the zPod to deploy
        zpodfactory_profile (str): zPodFactory profile name
        zpodfactory_endpoint (str): zPodFactory endpoint name
        zpodfactory_access_token (str): zPodFactory access token
        zpodfactory_base_url (str): zPodFactory base URL
        depot_mode (str): Depot mode ('online' or 'offline')
        online_depot_download_token (str, optional): Online depot download token
        offline_depot_hostname (str, optional): Offline depot hostname
        offline_depot_username (str, optional): Offline depot username
        offline_depot_password (str, optional): Offline depot password
        offline_depot_port (int, optional): Offline depot port (default: 443)
        verify_only (bool, optional): Stop after SDDC validation, before
            deployment (default: False)

    The release version and SKU are read from the template's mandatory
    top-level "version" and "workflowType" keys (no CLI flags).

    Raises:
        typer.Exit: On deployment failures
    """
    setup_signal_handlers()

    # Parse VCF JSON template
    vcf_template_data = json.loads(vcf_json_template.read())

    # The template's top-level "version" key is the single source of truth for
    # the release version that drives the depot/release-component API calls.
    vcf_version = str(vcf_template_data.get("version", "")).strip()
    if not vcf_version:
        console.print(
            "[bold red]❌ Template is missing a top-level \"version\" key, which "
            "is required to drive the depot/bundle API calls.[/bold red]"
        )
        raise typer.Exit(code=1)

    # The SKU (VCF or VVF) likewise comes from the template's "workflowType"
    # key; it selects the release line in the depot/release-component API.
    vcf_sku = str(vcf_template_data.get("workflowType", "")).strip().upper()
    if vcf_sku not in ("VCF", "VVF"):
        console.print(
            f"[bold red]❌ Template \"workflowType\" must be 'VCF' or 'VVF' "
            f"(got {vcf_sku or 'empty'}).[/bold red]"
        )
        raise typer.Exit(code=1)

    console.print(
        f"[dim]Using VCF version {vcf_version} ({vcf_sku}) from template[/dim]"
    )

    # VCF 9.1+ replaced the online depot with a new activation system, so the
    # online depot mode no longer works there. Fail fast (before provisioning)
    # and point the user at the offline depot.
    if depot_mode.lower() == "online" and is_vcf91(vcf_template_data):
        console.print(
            f"[bold red]❌ Online depot mode is not supported for VCF "
            f"{vcf_version}: 9.1 and later replaced it with a new activation "
            "system. Use the offline depot instead (--depot-mode offline with "
            "--offline-depot-hostname / --offline-depot-username / "
            "--offline-depot-password).[/bold red]"
        )
        raise typer.Exit(code=1)

    # Create zPod client
    zpod_client = httpx.Client(
        base_url=zpodfactory_base_url,
        headers={"access_token": zpodfactory_access_token},
        timeout=httpx.Timeout(30.0, connect=60.0),
    )

    try:
        phase_banner(1, "zPod Deployment")
        zpod = await deploy_zpod(
            zpod_client=zpod_client,
            zpod_name=zpod_name,
            profile=zpodfactory_profile,
            endpoint_name=zpodfactory_endpoint,
        )

        # Fetch zPodFactory host IP from settings API
        zpodfactory_ip = fetch_zpodfactory_host_ip(zpod_client)

        phase_banner(2, "Preparing VCF Spec")
        vcf_json = build_vcf_template(
            zpod, json.dumps(vcf_template_data), zpodfactory_ip
        )

        # Write VCF template
        output_file = (
            Path("/tmp") / f"{zpod_name}-{time.strftime('%Y%m%d-%H%M%S')}.json"
        )
        write_vcf_template(vcf_json, output_file)

        # Create VCF API client for SDDC operations. Built early so we can
        # detect whether a prior run already started validation/deployment.
        vcf_client = VCFClient(
            normalize_vcf_url(f"vcfinstaller.{zpod['domain']}"),
            "admin@local",
            zpod["password"],
        )

        # Wait for the freshly-provisioned VCF Installer appliance to finish
        # booting before any API calls. Without this, the first few steps each
        # ride out the boot window with their own retry counters, which looks
        # like the same call looping. This is one clear readiness phase.
        await vcf_client.wait_until_ready()

        phase_banner(3, "Preparing ESXi hosts")

        # Install the nested vSAN ESA mock-HW VIB on the ESXi hosts (VCF 9.1
        # enables vSAN ESA, which needs this on nested hardware). No-op for 9.0.
        #
        # This step needs SSH on the ESXi hosts, but the VCF Installer disables
        # ESXi SSH once a validation/deployment POST has been issued. On a retry
        # of a deployment that already reached that point, the VIB is already
        # installed — skip it rather than fail on a closed SSH port.
        if is_vcf91(vcf_json):
            if await sddc_operation_started(vcf_client):
                console.print(
                    "[cyan]ℹ️ Existing validation/deployment detected — "
                    "skipping vSAN ESA mock-HW VIB install (ESXi SSH is "
                    "disabled once VCF deployment has started).[/cyan]"
                )
            else:
                await install_vsan_esa_mock_vib(vcf_json, zpod)
        else:
            console.print(
                "[green]✓ No ESXi host preparation required (VCF 9.0.x)[/green]"
            )

        phase_banner(4, "Configuring DNS records")
        configure_dns(zpod_client, zpod_name, vcf_json)

        phase_banner(5, "Downloading VCF bundles")
        await configure_vcf_depot_and_bundles(
            zpod=zpod,
            depot_mode=depot_mode,
            online_depot_download_token=online_depot_download_token,
            offline_depot_hostname=offline_depot_hostname,
            offline_depot_username=offline_depot_username,
            offline_depot_password=offline_depot_password,
            offline_depot_port=offline_depot_port,
            vcf_sku=vcf_sku,
            vcf_version=vcf_version,
            vcf_json=vcf_json,
        )

        # Check for existing validation first
        debug_console.print("[cyan]🔍 Checking for existing validation...[/cyan]")
        latest_validation = await get_latest_validation(vcf_client)
        if latest_validation:
            console.print(
                f"[cyan]✓ Found existing validation "
                f"({latest_validation.get('executionStatus', 'UNKNOWN')})[/cyan]"
            )
            debug_console.print(
                f"[dim]Validation ID: {latest_validation['id']}[/dim]"
            )

        if not latest_validation:
            # No existing validation - launch a fresh validation + deployment.
            await _run_sddc_operations(
                vcf_client,
                vcf_json,
                "Starting fresh validation and deployment",
                verify_only=verify_only,
            )
            return

        # Check for existing SDDC deployment
        debug_console.print(
            "[cyan]🔍 Checking for existing SDDC deployment...[/cyan]"
        )
        latest_sddc = await get_latest_sddc(vcf_client)
        if latest_sddc:
            console.print(
                f"[cyan]✓ Found existing SDDC deployment "
                f"({latest_sddc.get('status', 'UNKNOWN')})[/cyan]"
            )
            debug_console.print(
                f"[dim]SDDC deployment ID: {latest_sddc['id']}[/dim]"
            )

        if not latest_sddc:
            # No existing SDDC deployment - monitor validation and then deploy
            await initiate_sddc_validations(
                client=vcf_client,
                vcf_json=vcf_json,
                validation_id=latest_validation["id"],
            )
            if verify_only:
                _print_verify_only_stop()
                return
            # After validation completes, proceed to deployment
            await initiate_sddc_deployment(
                client=vcf_client,
                vcf_json=vcf_json,
            )
            return

        # Found existing SDDC deployment - check status and handle accordingly
        detected_sddc_id = latest_sddc["id"]

        if verify_only:
            console.print(
                f"[yellow]⚠️ --verify-only: an SDDC deployment already exists "
                f"({detected_sddc_id}); validation is already complete. Re-run "
                "without --verify-only to monitor/resume deployment.[/yellow]"
            )
            return

        # Get the current deployment status to determine if we need to resume
        try:
            current_sddc = await vcf_client.api_call(
                "GET", f"/v1/sddcs/{detected_sddc_id}"
            )
            current_status = current_sddc.get("status", "UNKNOWN")

            if current_status == "COMPLETED_WITH_SUCCESS":
                console.print(
                    "[bold green]✅ Found existing deployment that completed successfully![/bold green]"
                )
                console.print("[cyan]Displaying final deployment status...[/cyan]")

            elif current_status == "IN_PROGRESS":
                console.print(
                    "[cyan]🔄 Found existing deployment in progress. Continuing monitoring...[/cyan]"
                )
            else:
                console.print("[cyan]🔄 Resuming existing deployment...[/cyan]")
                try:
                    await vcf_client.api_call("PATCH", f"/v1/sddcs/{detected_sddc_id}")
                    console.print(
                        "[bold green]✓ Deployment resume request sent successfully[/bold green]"
                    )
                except Exception as e:
                    console.print(f"[yellow]⚠️ Resume request failed: {e}[/yellow]")
                    console.print("[cyan]Continuing with monitoring...[/cyan]")
        except Exception as e:
            console.print(f"[yellow]⚠️ Error checking deployment status: {e}[/yellow]")
            console.print("[cyan]Continuing with monitoring...[/cyan]")

        # Skip validation and go straight to monitoring existing deployment
        await initiate_sddc_deployment(
            client=vcf_client,
            vcf_json=vcf_json,
            sddc_id=detected_sddc_id,
        )

        console.print("[bold green]✅ Deployment completed successfully![/bold green]")

    finally:
        zpod_client.close()


async def sddc_operation_started(client: VCFClient) -> bool:
    """
    Check whether a validation or SDDC deployment has already been started.

    Used to decide whether SSH-dependent pre-deployment steps (such as the
    vSAN ESA mock-HW VIB install) can still run: the VCF Installer disables
    ESXi SSH once a validation/deployment POST has been issued, so once either
    exists those steps must be skipped on a retry.

    Args:
        client: VCF client instance

    Returns:
        bool: True if a validation or SDDC deployment already exists
    """
    for path in ("/v1/sddcs/validations/latest", "/v1/sddcs/latest"):
        try:
            result = await client.api_call("GET", path)
            if result and "id" in result:
                return True
        except Exception as e:
            if global_debug:
                debug_console.print(
                    f"[yellow]⚠️ Error checking {path}: {e}[/yellow]"
                )
    return False


async def _get_latest(client: VCFClient, path: str, label: str) -> Optional[dict]:
    """GET a '…/latest' SDDC resource, returning it only if it carries an id.

    Returns None when nothing exists yet or the call fails — these endpoints
    legitimately error before the first validation/deployment is created.
    Callers use truthiness for "does it exist?" and ``["id"]`` for the id, so a
    single fetch serves both (no second round-trip).
    """
    try:
        result = await client.api_call("GET", path)
        return result if result and "id" in result else None
    except Exception as e:
        if global_debug:
            debug_console.print(
                f"[yellow]⚠️ Error fetching latest {label}: {e}[/yellow]"
            )
        return None


async def get_latest_validation(client: VCFClient) -> Optional[dict]:
    """Latest SDDC validation object, or None if none yet / on error."""
    return await _get_latest(client, "/v1/sddcs/validations/latest", "validation")


async def get_latest_sddc(client: VCFClient) -> Optional[dict]:
    """Latest SDDC deployment object, or None if none yet / on error."""
    return await _get_latest(client, "/v1/sddcs/latest", "SDDC deployment")


def _print_verify_only_stop():
    """Report that we stopped after validation due to --verify-only."""
    console.print(
        "\n[bold green]✅ --verify-only: SDDC validation complete, stopping "
        "before deployment.[/bold green]"
    )
    console.print(
        "[cyan]Re-run without --verify-only to deploy the validated SDDC "
        "spec.[/cyan]"
    )


async def _run_sddc_operations(
    client: VCFClient, vcf_json: dict, message: str, verify_only: bool = False
):
    """
    Helper function to run SDDC validation and deployment operations.

    Args:
        client: VCF client instance
        vcf_json: VCF template JSON
        message: Message to display before starting operations
        verify_only: When True, stop after validation without deploying
    """
    debug_console.print(
        f"[cyan]{message}. Proceeding with validation and new deployment...[/cyan]"
    )

    # Initiate SDDC operations
    await initiate_sddc_validations(
        client=client,
        vcf_json=vcf_json,
    )
    if verify_only:
        _print_verify_only_stop()
        return
    await initiate_sddc_deployment(
        client=client,
        vcf_json=vcf_json,
    )


@timeit
async def deploy_zpod(
    zpod_client: httpx.Client,
    zpod_name: str,
    profile: str,
    endpoint_name: str,
):
    """Deploy zPod"""
    global zpod_creation_iso
    console.print(f"[bold cyan]🚀 Deploying zPod: {zpod_name}[/bold cyan]")

    zpod = zpod_client.get(f"/zpods/name={zpod_name}").json()
    if zpod.get("status") == "ACTIVE":
        console.print("[bold green]✓ zPod is already active[/bold green]")
        console.print(gather_zpod_info(zpod))
        zpod_creation_iso = zpod.get("creation_date")
        return zpod

    endpoint = zpod_client.get(f"/endpoints/name={endpoint_name}").json()

    if not endpoint:
        console.print(f"[bold red]❌ Endpoint not found: {endpoint_name}[/bold red]")
        raise typer.Exit(code=1)

    if global_debug:
        debug_console.print(f"[dim]Creating zPod with endpoint: {endpoint}[/dim]")

    zpod = zpod_client.post(
        "/zpods",
        json={
            "name": zpod_name,
            "endpoint_id": endpoint["id"],
            "profile": profile,
        },
    ).json()

    zpod_id = zpod["id"]

    with Live(auto_refresh=False) as live:
        while True:
            zpod = zpod_client.get(f"/zpods/{zpod_id}").json()

            live.update(gather_zpod_info(zpod), refresh=True)
            status = zpod["status"]
            if status not in ("BUILDING", "PENDING"):
                break
            await asyncio.sleep(10)

    if status == "ACTIVE":
        # Success + timing is reported by the @timeit wrapper as
        # "✅ Creating zPod completed in …", so no separate success line here.
        zpod_creation_iso = zpod.get("creation_date")
        return zpod
    elif status == "DEPLOY_FAILED":
        console.print("[bold red]❌ zPod Deployment Failed[/bold red]")
        if global_debug:
            debug_console.print(Pretty(zpod))
        raise typer.Exit(code=1)

    console.print(f"[bold red]❌ Unexpected status: {status}[/bold red]")
    raise typer.Exit(code=1)


def fetch_zpodfactory_host_ip(zpod_client: httpx.Client) -> str:
    """Fetch the zPodFactory host IP from the settings API.

    Args:
        zpod_client: httpx.Client configured for zPodFactory API

    Returns:
        str: The zPodFactory host IP address
    """
    debug_console.print(
        "[cyan]🔍 Fetching zPodFactory host IP from settings...[/cyan]"
    )
    response = zpod_client.get("/settings/name=zpodfactory_host")
    response.raise_for_status()
    data = response.json()
    ip = data.get("value", "")
    if not ip:
        console.print(
            "[bold red]❌ zpodfactory_host setting returned empty value[/bold red]"
        )
        raise typer.Exit(code=1)
    debug_console.print(f"[green]✓ zPodFactory host IP: {ip}[/green]")
    return ip


# ---------------------------------------------------------------------------
# VCF version-family support (9.0.x and 9.1.x)
#
# Both mappings below are single, version-agnostic tables. They are driven by
# the rendered template: a key only takes effect when it is actually present
# in the template JSON, so VCF 9.0-only and 9.1-only entries coexist safely in
# one dict. is_vcf91() remains for the few behaviors that genuinely differ
# (ESXi vSAN ESA VIB install, depot-skip warning verbosity).
# ---------------------------------------------------------------------------

# hostname (short name) -> last octet of the zPod /26 subnet.
# Layout note: .50-.60 is a DHCP range; the VCF 9.1 vspClusterSpec.ipv4Pool
# occupies .36-.49 (a pool, not a host -> no DNS record; >=12 IPs required
# by VCF Management Services).
# 9.0 and 9.1 share octets. The cloud proxy was renamed between versions
# (9.0: "vcfopscollector", 9.1: "cloudproxy") — both names map to the same
# octet since they are the same role and a template only references one.
# The fleet manager ("fleetmgr") and NSX manager node ("nsx21") each use a
# single name in every template.
HOSTNAME_IP_MAPPING = {
    # core infrastructure (low octets)
    "vcfops": "3",
    "cloudproxy": "4",
    "vcfopscollector": "4",  # 9.0 name for the cloud proxy — same role/IP
    "sddcmgr": "5",
    "fleetmgr": "6",
    "vcflicense": "8",
    "identitybroker": "9",
    # shared across 9.0 and 9.1
    "vcsa": "10",
    "nsx": "20",
    "nsx21": "21",
    # VCF 9.1 VCF Services Platform (VSP) FQDNs
    "vcfservicesruntime": "30",
    "instancecomponents": "31",
    # VCF 9.0-only component
    "vcfa": "30",
}

# VCF JSON spec key -> depot/release component name(s). A spec key only
# contributes its components when that key is present in the rendered template.
# This single table serves both versions: 9.1 split VCF Operations and its
# Cloud Proxy across vcfOperationsSpec / vcfOperationsCollectorSpec, but since
# both spec keys are present in 9.0 and 9.1 templates alike, the resulting
# component set ({VROPS, VCF_OPS_CLOUD_PROXY}) is identical either way.
# NOTE: the 9.1 component name strings are preliminary and must be reconciled
# against a live release-components API response before a real run.
SPEC_TO_COMPONENTS = {
    "vcenterSpec": ["VCENTER"],
    "sddcManagerSpec": ["SDDC_MANAGER"],
    "nsxtSpec": ["NSX_T_MANAGER"],
    "vcfOperationsSpec": ["VROPS"],
    "vcfOperationsLogsSpec": ["VRLI"],  # FIXME: verify component string
    "vcfOperationsFleetManagementSpec": ["VRSLCM"],
    "vcfOperationsCollectorSpec": ["VCF_OPS_CLOUD_PROXY"],
    "vspClusterSpec": ["VSP"],
    "vcfAutomationSpec": ["VRA", "VCF_SERVICE_VCD_MIGRATION_BACKEND"],
    "licenseServerSpec": ["VCF_LICENSE_SERVER"],
    "vidbSpec": ["VIDB"],
    "saltSpec": ["VCF_SALT"],
    "saltRaasSpec": ["VCF_SALT_RAAS"],
    "telemetryAcceptorSpec": ["TELEMETRY_ACCEPTOR"],
    "fleetLcmSpec": ["VCF_FLEET_LCM"],
    "sddcLcmSpec": ["VCF_SDDC_LCM"],
    "fleetDepotSpec": ["DEPOT_SERVICE"],
}


def is_vcf91(vcf_json: dict) -> bool:
    """Return True for VCF 9.1.x (and later), derived from the rendered
    template's top-level ``version`` field. Defaults to False (9.0 behavior)
    when the version cannot be parsed."""
    parts = str(vcf_json.get("version", "")).split(".")
    try:
        return (int(parts[0]), int(parts[1])) >= (9, 1)
    except (IndexError, ValueError):
        return False


def build_vcf_template(zpod, tmpl, zpodfactory_ip: str):
    """Build VCF template with zpod variables"""
    debug_console.print("[bold cyan]📋 Building VCF template...[/bold cyan]")

    # Validate zpodfactory_ip is provided
    if not zpodfactory_ip:
        console.print(
            "[bold red]❌ zPodFactory host IP is required but not provided[/bold red]"
        )
        raise typer.Exit(code=1)

    # Extract zpod subnet more robustly
    zpod_subnet = None
    if zpod.get("networks") and len(zpod["networks"]) > 0:
        cidr = zpod["networks"][0].get("cidr", "")
        if cidr:
            if "/" in cidr:
                ip_part = cidr.split("/")[0]
            else:
                ip_part = cidr
            parts = ip_part.split(".")
            if len(parts) >= 3:
                zpod_subnet = f"{parts[0]}.{parts[1]}.{parts[2]}"

    if not zpod_subnet:
        console.print(
            f"[bold red]❌ Could not extract zpod_subnet from zpod networks: {zpod.get('networks', [])}[/bold red]"
        )
        raise typer.Exit(code=1)

    # Prepare template variables
    template_vars = {
        "zpod_domain": zpod.get("domain", ""),
        "zpod_name": zpod.get("name", ""),
        "zpod_password": zpod.get("password", ""),
        "zpod_subnet": zpod_subnet,
        "zpodfactory_ip": zpodfactory_ip,
    }

    if global_debug:
        debug_console.print(f"[dim]Template variables:[/dim]")
        debug_console.print(f"[dim]  zpod_domain: {template_vars['zpod_domain']}[/dim]")
        debug_console.print(f"[dim]  zpod_name: {template_vars['zpod_name']}[/dim]")
        debug_console.print(f"[dim]  zpod_subnet: {template_vars['zpod_subnet']}[/dim]")
        debug_console.print(f"[dim]  zpodfactory_ip: {template_vars['zpodfactory_ip']}[/dim]")
        debug_console.print(
            f"[dim]  zpod_password: {'*' * len(template_vars['zpod_password']) if template_vars['zpod_password'] else 'None'}[/dim]"
        )

    t = Template(tmpl)
    vcf_json = json.loads(t.render(**template_vars))

    # Verify that template variables were replaced
    vcf_str = json.dumps(vcf_json)
    if "{{zpod_subnet}}" in vcf_str:
        console.print(
            "[yellow]⚠️ {{zpod_subnet}} template variable was not replaced![/yellow]"
        )
    if "{{zpod_name}}" in vcf_str:
        console.print(
            "[yellow]⚠️ {{zpod_name}} template variable was not replaced![/yellow]"
        )
    if "{{zpod_domain}}" in vcf_str:
        console.print(
            "[yellow]⚠️ {{zpod_domain}} template variable was not replaced![/yellow]"
        )
    if "{{zpodfactory_ip}}" in vcf_str:
        console.print(
            "[yellow]⚠️ {{zpodfactory_ip}} template variable was not replaced![/yellow]"
        )

    return vcf_json


def write_vcf_template(vcf_json, filename):
    """Write VCF template to file"""
    console.print("[bold cyan]📋 VCF template ready[/bold cyan]")
    debug_console.print(f"[dim]Wrote VCF template to: {filename}[/dim]")
    with open(filename, "w") as f:
        json.dump(vcf_json, f, indent=2)


# nested vSAN ESA mock-HW VIB (https://github.com/lamw/nested-vsan-esa-mock-hw-vib).
# VCF 9.1 turns on vSAN ESA; nested ESXi lacks certified hardware, so this VIB
# fakes the capability flags. Pre-built artifact from the repo's "1.0" release.
VSAN_ESA_MOCK_VIB_URL = (
    "https://github.com/lamw/nested-vsan-esa-mock-hw-vib/"
    "releases/download/1.0/nested-vsan-esa-mock-hw.vib"
)
VSAN_ESA_MOCK_VIB_NAME = "nested-vsan-esa-mock-hw"


async def _install_vib_on_host(hostname: str, username: str, password: str) -> str:
    """Install the nested vSAN ESA mock-HW VIB on a single ESXi host over SSH.

    Returns 'installed' or 'already-installed'. Raises RuntimeError on failure.
    """
    import asyncssh  # lazy import: only the VCF 9.1 ESA path needs it

    try:
        async with asyncssh.connect(
            hostname,
            username=username,
            password=password,
            known_hosts=None,  # nested ESXi host keys are ephemeral
            login_timeout=30,
        ) as conn:
            # Idempotency: skip hosts that already have the VIB installed
            check = await conn.run(
                f"esxcli software vib list | grep -i {VSAN_ESA_MOCK_VIB_NAME}",
                check=False,
                timeout=60,
            )
            if check.exit_status == 0:
                return "already-installed"

            # httpClient ruleset is disabled by default and blocks the
            # outbound fetch of the VIB URL — open it before installing.
            commands = [
                "esxcli network firewall ruleset set -e true -r httpClient",
                "esxcli software acceptance set --level CommunitySupported",
                f"esxcli software vib install -v {VSAN_ESA_MOCK_VIB_URL} "
                f"--no-sig-check",
                "/etc/init.d/vsanmgmtd restart",
            ]
            for cmd in commands:
                if global_debug:
                    debug_console.print(f"[dim]{hostname}: {cmd}[/dim]")
                result = await conn.run(cmd, check=False, timeout=180)
                if global_debug and result.stdout:
                    debug_console.print(
                        f"[dim]{hostname} stdout: {result.stdout.strip()}[/dim]"
                    )
                if result.exit_status != 0:
                    raise RuntimeError(
                        f"command failed (exit {result.exit_status}): {cmd}\n"
                        f"{(result.stderr or '').strip()}"
                    )
            return "installed"
    except asyncssh.Error as e:
        raise RuntimeError(f"SSH error on {hostname}: {e}") from e
    except OSError as e:
        raise RuntimeError(f"connection to {hostname} failed: {e}") from e


async def install_vsan_esa_mock_vib(vcf_json: dict, zpod: dict):
    """Install the nested vSAN ESA mock-HW VIB on every ESXi host when the
    template enables vSAN ESA. No-op when esaConfig is disabled."""
    esa_enabled = (
        vcf_json.get("datastoreSpec", {})
        .get("vsanSpec", {})
        .get("esaConfig", {})
        .get("enabled", False)
    )
    if not esa_enabled:
        console.print(
            "[dim]vSAN ESA not enabled in template — "
            "skipping mock-HW VIB install[/dim]"
        )
        return

    hostnames = [
        h["hostname"]
        for h in vcf_json.get("hostSpecs", [])
        if h.get("hostname")
    ]
    if not hostnames:
        console.print("[yellow]⚠️ No ESXi hosts found in template[/yellow]")
        return

    console.print(
        f"[bold cyan]🔧 Installing nested vSAN ESA mock-HW VIB on "
        f"{len(hostnames)} ESXi host(s)...[/bold cyan]"
    )

    password = zpod.get("password", "")
    results = await asyncio.gather(
        *(_install_vib_on_host(h, "root", password) for h in hostnames),
        return_exceptions=True,
    )

    failures = []
    for hostname, result in zip(hostnames, results):
        if isinstance(result, Exception):
            console.print(f"  [red]✗[/red] {hostname}: {result}")
            failures.append(hostname)
        elif result == "already-installed":
            console.print(
                f"  [green]✓[/green] {hostname}: VIB already installed"
            )
        else:
            console.print(f"  [green]✓[/green] {hostname}: VIB installed")

    if failures:
        console.print(
            f"[bold red]❌ vSAN ESA mock-HW VIB install failed on: "
            f"{', '.join(failures)}[/bold red]"
        )
        raise typer.Exit(code=1)


def configure_dns(
    zpod_client: httpx.Client,
    zpod_name: str,
    vcf_json: dict,
):
    """Configure DNS records for the zPod"""
    console.print("[bold cyan]🌐 Configuring DNS records...[/bold cyan]")

    def find_hostnames_in_json(obj, hostnames=None):
        """Recursively find all hostnames in JSON structure that contain the domain"""
        if hostnames is None:
            hostnames = []

        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in [
                    "hostname",
                    "vcenterHostname",
                    "vipFqdn",
                    "platformFqdn",
                    "instanceFqdn",
                    "fleetFqdn",
                ] and isinstance(value, str):
                    if "." in value and not value.startswith("{{"):
                        hostnames.append(value)
                else:
                    find_hostnames_in_json(value, hostnames)
        elif isinstance(obj, list):
            for item in obj:
                find_hostnames_in_json(item, hostnames)

        return hostnames

    def get_ip_for_hostname(hostname, zpod_subnet):
        """Get IP address for hostname using correspondence table"""
        hostname_part = hostname.split(".")[0]

        if hostname_part in HOSTNAME_IP_MAPPING:
            ip_suffix = HOSTNAME_IP_MAPPING[hostname_part]
            ip_address = f"{zpod_subnet}.{ip_suffix}"
            return ip_address, hostname_part

        # ESXi hosts and the VCF Installer intentionally get no DNS record here;
        # any other unmapped hostname is likely a template/mapping mismatch.
        if global_debug and not (
            hostname_part.startswith("esxi") or hostname_part == "vcfinstaller"
        ):
            debug_console.print(
                f"[yellow]⚠️ No IP mapping for hostname '{hostname_part}' "
                f"({hostname}) — skipping DNS record[/yellow]"
            )
        return None, hostname

    # Get the zpod subnet from the VCF JSON
    zpod_subnet = None
    for network_spec in vcf_json.get("networkSpecs", []):
        if network_spec.get("networkType") == "MANAGEMENT":
            subnet = network_spec.get("subnet", "")
            if subnet:
                zpod_subnet = (
                    subnet.split(".")[0]
                    + "."
                    + subnet.split(".")[1]
                    + "."
                    + subnet.split(".")[2]
                )
                break

    if not zpod_subnet:
        console.print("[yellow]⚠️ Could not find zpod subnet in VCF JSON[/yellow]")
        return

    # Find all hostnames in the JSON structure
    hostnames = find_hostnames_in_json(vcf_json)

    # Get IP addresses for all hostnames using correspondence table
    resolved_records = []
    for hostname in hostnames:
        ip_address, resolved_hostname = get_ip_for_hostname(hostname, zpod_subnet)
        if ip_address:
            resolved_records.append((ip_address, resolved_hostname))

    if not resolved_records:
        console.print("[yellow]No hostnames found to configure.[/yellow]")
        return

    # Create DNS records with per-record spinner
    for ip_address, hostname in resolved_records:
        with console.status(
            f"  Adding DNS record for [cyan]{hostname}[/cyan] with ip [cyan]{ip_address}[/cyan]..."
        ):
            result = _ensure_dns_record(zpod_client, zpod_name, ip_address, hostname)

        if result == "exists":
            console.print(
                f"  [green]✓[/green] DNS record for [cyan]{hostname}[/cyan] ({ip_address}) already exists"
            )
        elif result == "created":
            console.print(
                f"  [green]✓[/green] DNS record for [cyan]{hostname}[/cyan] ({ip_address}) created"
            )
        else:
            console.print(
                f"  [red]✗[/red] DNS record for [cyan]{hostname}[/cyan] ({ip_address}) failed"
            )


def _ensure_dns_record(zpod_client, zpod_name, ip, hostname):
    """Create a DNS record if it doesn't already exist.

    Returns:
        str: 'exists', 'created', or 'failed'
    """
    response = zpod_client.get(f"/zpods/name={zpod_name}/dns/{ip}/{hostname}")

    if response.status_code == 200:
        return "exists"

    if response.status_code == 404:
        dns_data = {"ip": ip, "hostname": hostname}
        if global_debug:
            debug_console.print(f"  [dim]DNS data: {dns_data}[/dim]")

        post_response = zpod_client.post(
            f"/zpods/name={zpod_name}/dns",
            json=dns_data,
        )
        if post_response.status_code in [200, 201]:
            return "created"

        if global_debug:
            debug_console.print(f"  [dim]Status: {post_response.status_code}[/dim]")
            try:
                debug_console.print(f"  [dim]Error: {post_response.json()}[/dim]")
            except json.JSONDecodeError:
                debug_console.print(f"  [dim]Error: {post_response.text}[/dim]")
        return "failed"

    if global_debug:
        debug_console.print(f"  [dim]Unexpected status: {response.status_code}[/dim]")
        try:
            debug_console.print(f"  [dim]Error: {response.json()}[/dim]")
        except json.JSONDecodeError:
            debug_console.print(f"  [dim]Error: {response.text}[/dim]")
    return "failed"


@timeit
async def configure_vcf_depot_and_bundles(
    zpod: dict,
    depot_mode: str,
    online_depot_download_token: str = None,
    offline_depot_hostname: str = None,
    offline_depot_username: str = None,
    offline_depot_password: str = None,
    offline_depot_port: int = 443,
    vcf_sku: str = "VCF",
    vcf_version: str = "9.0.0.0",
    vcf_json: dict = None,
):
    """
    Configure VCF depot settings and download required bundles.

    Args:
        zpod (dict): zPod information including domain and password
        depot_mode (str): Depot mode ('online' or 'offline')
        online_depot_download_token (str, optional): Online depot download token
        offline_depot_hostname (str, optional): Offline depot hostname
        offline_depot_username (str, optional): Offline depot username
        offline_depot_password (str, optional): Offline depot password
        offline_depot_port (int, optional): Offline depot port (default: 443)
        vcf_sku (str, optional): VCF SKU ('VCF' or 'VVF', default: 'VCF')
        vcf_version (str, optional): VCF version (default: '9.0.0.0')
        vcf_json (dict, optional): Rendered VCF config template to determine required components

    Raises:
        Exception: On depot configuration or bundle download failures
    """
    console.print(
        "[bold cyan]📦 Configuring VCF depot and downloading bundles...[/bold cyan]"
    )

    # Create VCF API client
    vcf_url = normalize_vcf_url(f"vcfinstaller.{zpod['domain']}")
    client = VCFClient(vcf_url, "admin@local", zpod["password"])

    try:
        # Get authentication token
        await client.get_token()

        # Configure depot based on mode
        if depot_mode.lower() == "online":
            await configure_online_depot(client, online_depot_download_token)
        else:
            await configure_offline_depot(
                client,
                offline_depot_hostname,
                offline_depot_username,
                offline_depot_password,
                offline_depot_port,
            )

        # Handle depot sync
        await handle_depot_sync(client)

        # Handle bundle operations
        await handle_bundle_operations(client, vcf_sku, vcf_version, vcf_json)

    finally:
        await client.close()


async def configure_online_depot(client: VCFClient, download_token: str):
    """
    Configure depot settings for online mode.

    Args:
        client (VCFClient): VCF API client for API calls
        download_token (str): VMware download token for online depot access

    Raises:
        Exception: On depot configuration failures
    """
    depot_data = {"vmwareAccount": {"downloadToken": download_token}}

    debug_console.print(
        "[dim]Configuring online depot — download token: "
        f"{download_token[:20]}...{download_token[-4:] if len(download_token) > 24 else ''}[/dim]"
    )

    try:
        await client.api_call("PUT", "/v1/system/settings/depot", depot_data)
        console.print("[bold green]✓ Online depot configured[/bold green]")
    except Exception as e:
        console.print(
            f"[bold red]❌ Failed to configure depot settings: {e}[/bold red]"
        )
        if global_debug:
            debug_console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
            debug_console.print(
                f"[red]Request data: {json.dumps(depot_data, indent=2)}[/red]"
            )
        raise


async def configure_offline_depot(
    client: VCFClient, hostname: str, username: str, password: str, port: int
):
    """
    Configure depot settings for offline mode.

    Args:
        client (VCFInstallerClient): VCF installer client for API calls
        hostname (str): Offline depot hostname
        username (str): Offline depot username
        password (str): Offline depot password
        port (int): Offline depot port

    Raises:
        Exception: On depot configuration failures
    """
    depot_data = {
        "offlineAccount": {
            "username": username,
            "password": password,
            "port": port,
        },
        "depotConfiguration": {
            "isOfflineDepot": True,
            "hostname": hostname,
            "port": port,
        },
    }

    # Full settings (password obfuscated) go to the debug log only.
    obfuscated_password = "*" * len(password) if password else ""
    debug_console.print(
        "[dim]Configuring offline depot:\n"
        f"  Hostname: {hostname}\n"
        f"  Username: {username}\n"
        f"  Password: {obfuscated_password}\n"
        f"  Port: {port}[/dim]"
    )

    try:
        await client.api_call("PUT", "/v1/system/settings/depot", depot_data)
        console.print(
            f"[bold green]✓ Offline depot configured[/bold green] "
            f"[cyan]({hostname})[/cyan]"
        )
    except Exception as e:
        console.print(
            f"[bold red]❌ Failed to configure depot settings: {e}[/bold red]"
        )
        if global_debug:
            debug_console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
            debug_console.print(
                f"[red]Request data: {json.dumps(depot_data, indent=2)}[/red]"
            )
        raise


async def handle_depot_sync(client: VCFClient):
    """Handle depot sync operations"""
    debug_console.print("[bold cyan]🔄 Checking depot sync status...[/bold cyan]")

    try:
        sync_info = await client.api_call(
            "GET", "/v1/system/settings/depot/depot-sync-info"
        )
        sync_status = sync_info.get("syncStatus", "UNKNOWN")
        debug_console.print(f"[dim]Depot sync status: {sync_status}[/dim]")

        if sync_status == "SYNCED":
            console.print("[bold green]✓ Depot already in sync[/bold green]")
        elif sync_status == "UNSYNCED":
            await trigger_depot_sync(client)
            await wait_for_depot_sync(client)
        else:
            # Unexpected/intermediate state — monitor it to completion.
            await wait_for_depot_sync(client)

    except Exception as e:
        console.print(f"[bold red]❌ Failed to check depot sync info: {e}[/bold red]")
        if global_debug:
            debug_console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
        raise


async def trigger_depot_sync(client: VCFClient):
    """Trigger depot sync"""
    try:
        await client.api_call("PATCH", "/v1/system/settings/depot/depot-sync-info")
        debug_console.print("[dim]✓ Depot sync triggered[/dim]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Could not trigger depot sync: {e}[/yellow]")
        if global_debug:
            debug_console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
        console.print("[cyan]Continuing to monitor sync status...[/cyan]")


async def wait_for_depot_sync(client: VCFClient):
    """Wait for the depot to sync, shown as a single in-place spinner.

    Per-poll status lines go to the debug log; the screen shows one spinner that
    resolves to a single success/failure line.
    """
    failed = False
    with console.status("[bold cyan]⏳ Syncing depot…[/bold cyan]", spinner="dots"):
        while True:
            try:
                sync_info = await client.api_call(
                    "GET",
                    "/v1/system/settings/depot/depot-sync-info",
                    log_response=False,  # polled every 10s — see api_call()
                )
                sync_status = sync_info.get("syncStatus", "UNKNOWN")
                debug_console.print(f"[dim]Depot sync status: {sync_status}[/dim]")

                if sync_status == "SYNCED":
                    break
                if sync_status == "SYNC_FAILED":
                    failed = True
                    if global_debug:
                        debug_console.print(
                            f"[red]Sync info: {json.dumps(sync_info, indent=2)}[/red]"
                        )
                    break
            except Exception as e:
                debug_console.print(
                    f"[red]Error checking depot sync status: "
                    f"{type(e).__name__}: {e}[/red]"
                )

            await asyncio.sleep(10)

    if failed:
        console.print("[bold red]❌ Depot sync failed![/bold red]")
        console.print(
            "[yellow]Please check your depot configuration and try again.[/yellow]"
        )
        return
    console.print("[bold green]✓ Depot synced[/bold green]")


def get_required_components(vcf_json: dict) -> set:
    """Determine which VCF components are required based on the config template.

    Components are considered required if their corresponding spec key is
    present in the VCF JSON config template. This is the single source of
    truth for the mapping between template specs and depot bundle components.

    Args:
        vcf_json: Rendered VCF config template dict

    Returns:
        Set of required component names matching the VCF release API
    """
    # SPEC_TO_COMPONENTS maps VCF JSON spec keys to API component names
    # (shared 9.0/9.1 table); a spec key can map to multiple components.
    required = set()
    for spec_key, component_names in SPEC_TO_COMPONENTS.items():
        if spec_key in vcf_json:
            required.update(component_names)

    return required


def get_component_version_overrides(vcf_json: dict) -> Dict[str, str]:
    """Map component name -> pinned version string from spec ``version`` fields.

    Per William Lam's VCF 9.1 quick tip, the VCF Installer API defaults to the
    component versions you have downloaded (i.e. the latest), but a deployment
    spec section may carry an explicit ``version`` property to pin a specific
    component version, e.g.::

        "vcfOperationsCollectorSpec": { ..., "version": "9.1.0" }

    When present, that pin must drive which bundle gets downloaded instead of
    the latest. We reuse SPEC_TO_COMPONENTS to translate spec keys to the
    release-API component names.
    """
    overrides = {}
    for spec_key, component_names in SPEC_TO_COMPONENTS.items():
        spec = vcf_json.get(spec_key)
        if isinstance(spec, dict):
            pinned = spec.get("version")
            if pinned:
                for name in component_names:
                    overrides[name] = str(pinned)

    return overrides


async def handle_bundle_operations(
    client: VCFClient, vcf_sku: str, vcf_version: str, vcf_json: dict = None
):
    """Handle bundle download operations"""
    console.print(
        f"\n[bold cyan]📦 Fetching {vcf_sku} {vcf_version} components...[/bold cyan]"
    )

    try:
        # Get release components
        release_components_response = await client.api_call(
            "GET",
            f"/v1/releases/{vcf_sku}/release-components?"
            f"releaseVersion={vcf_version}&imageType=INSTALL",
        )

        # Parse components, honoring any per-component version pins from the spec
        version_overrides = (
            get_component_version_overrides(vcf_json) if vcf_json else {}
        )
        vcf_components = parse_vcf_components(
            release_components_response, version_overrides
        )

        if not vcf_components:
            console.print("[bold red]❌ No VCF components found. Exiting.[/bold red]")
            return

        # Filter to only required components based on config template
        if vcf_json:
            required_components = get_required_components(vcf_json)
            all_component_names = set(vcf_components.keys())
            vcf_components = {
                name: data
                for name, data in vcf_components.items()
                if name in required_components
            }
            missing = required_components - all_component_names
            if missing and is_vcf91(vcf_json):
                console.print(
                    f"[bold yellow]⚠️ Required components not found in the "
                    f"release components list: {', '.join(sorted(missing))} — "
                    f"verify the spec-to-component mapping[/bold yellow]"
                )

        # Get current download status
        bundle_status_map = await get_bundle_download_status(client, vcf_version)

        # Check if all bundles are already downloaded
        all_success, bundles_to_download = analyze_bundle_status(
            vcf_components, bundle_status_map
        )

        if all_success:
            console.print(
                "\n[bold green]✅ All bundles are already downloaded successfully![/bold green]"
            )
            console.print("[dim]No downloads needed.[/dim]")
        else:
            # Initiate downloads for bundles with PENDING status
            await initiate_bundle_downloads(
                client, bundles_to_download, bundle_status_map
            )

            # Show live monitoring if any bundles are not in SUCCESS status
            if not all_success:
                console.print(
                    "[yellow]Press Ctrl+C to interrupt monitoring at any time.[/yellow]"
                )
                await monitor_download_progress(
                    client, vcf_components, vcf_version
                )
            else:
                console.print(
                    "\n[yellow]⚠️ No downloads were initiated successfully.[/yellow]"
                )

    except Exception as e:
        console.print(
            f"[bold red]❌ Failed to handle bundle operations: {e}[/bold red]"
        )
        if global_debug:
            debug_console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
        raise


def _version_sort_key(product_version: str) -> Tuple[int, ...]:
    """Build a numeric sort key from a VCF productVersion string.

    VCF 9.0+ uses the format X.Y.Z.AABC.<build> (see Broadcom KB 410435), where
    higher numbers in earlier positions take precedence — e.g. the express-patch
    release 9.1.0.0100.25426672 supersedes the GA 9.1.0.0.25318520. Splitting on
    "." and comparing the segments as integers implements that ordering directly.
    Non-numeric or missing segments fall back to -1 so they sort lowest.
    """
    parts = []
    for segment in str(product_version).split("."):
        parts.append(int(segment) if segment.isdigit() else -1)
    return tuple(parts)


def _latest_version(versions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Return the newest version entry from a component's versions list."""
    return max(
        versions, key=lambda v: _version_sort_key(v.get("productVersion", ""))
    )


def _version_matches(product_version: str, pinned: str) -> bool:
    """Return True if ``product_version`` matches the ``pinned`` spec version.

    Matching is segment-wise (split on ".") so a pin is treated as a prefix on
    dot boundaries: "9.1.0.0" matches the GA "9.1.0.0.25318520" but NOT the
    express patch "9.1.0.0100.25426672", while "9.1.0.0100" matches only the
    patch line and a coarse "9.1.0" matches both (the caller then keeps the
    newest match). This avoids the false positives a raw string prefix would
    cause (e.g. "9.1.0.0" string-prefixing "9.1.0.0100").
    """
    pv = str(product_version).split(".")
    pin = str(pinned).split(".")
    return pv[: len(pin)] == pin


def parse_vcf_components(
    release_components_response: Dict[str, Any],
    version_overrides: Dict[str, str] = None,
) -> Dict[str, Any]:
    """Parse VCF components from API response.

    ``version_overrides`` maps a component name to a pinned version string (from
    a deployment spec's ``version`` field). For pinned components the matching
    version's bundles are selected instead of the latest.
    """
    vcf_components = {}
    version_overrides = version_overrides or {}

    if (
        isinstance(release_components_response, dict)
        and "elements" in release_components_response
    ):
        elements = release_components_response["elements"]

        if elements and len(elements) > 0:
            target_release = elements[0]
            if "components" in target_release:
                components = target_release["components"]
                debug_console.print(
                    f"[bold green]✓ Found {len(components)} components[/bold green]"
                )

                for component in components:
                    component_name = component.get("name", "Unknown")
                    component_public_name = component.get("publicName", "Unknown")

                    vcf_components[component_name] = {
                        "publicName": component_public_name,
                        "sku": component.get("sku", "Unknown"),
                        "automatedInstall": component.get("automatedInstall", False),
                        "bundles": [],
                    }

                    if (
                        "versions" in component
                        and isinstance(component["versions"], list)
                        and component["versions"]
                    ):
                        versions = component["versions"]
                        pinned = version_overrides.get(component_name)
                        version = None

                        # A spec may pin a specific component version; honor it
                        # over the latest (William Lam VCF 9.1 quick tip).
                        if pinned:
                            matches = [
                                v
                                for v in versions
                                if _version_matches(
                                    v.get("productVersion", ""), pinned
                                )
                            ]
                            if matches:
                                version = _latest_version(matches)
                                debug_console.print(
                                    f"[dim]  {component_name}: using pinned version "
                                    f"{version.get('productVersion', 'Unknown')} "
                                    f"(spec version='{pinned}')[/dim]"
                                )
                            else:
                                available = ", ".join(
                                    v.get("productVersion", "?") for v in versions
                                )
                                console.print(
                                    f"[bold yellow]⚠️ {component_name}: pinned version "
                                    f"'{pinned}' not found among available versions "
                                    f"({available}); falling back to latest[/bold yellow]"
                                )

                        # Otherwise (or on a failed pin) only the newest version
                        # of each component must be downloaded — older releases
                        # (e.g. GA vs. express patch) are superseded. See
                        # _version_sort_key / Broadcom KB 410435.
                        if version is None:
                            version = _latest_version(versions)
                            if not pinned and len(versions) > 1:
                                debug_console.print(
                                    f"[dim]  {component_name}: selected latest version "
                                    f"{version.get('productVersion', 'Unknown')} of "
                                    f"{len(versions)} available[/dim]"
                                )

                        product_version = version.get("productVersion", "Unknown")

                        if (
                            "artifacts" in version
                            and "bundles" in version["artifacts"]
                        ):
                            bundles = version["artifacts"]["bundles"]
                            for bundle in bundles:
                                bundle_id = bundle.get("id", "Unknown")
                                bundle_name = bundle.get("name", "Unknown")
                                bundle_size = bundle.get("size", 0)

                                vcf_components[component_name]["bundles"].append(
                                    {
                                        "id": bundle_id,
                                        "name": bundle_name,
                                        "size": bundle_size,
                                        "type": bundle.get("type", "Unknown"),
                                        "productVersion": product_version,
                                    }
                                )

                # Print summary
                total_bundles = sum(
                    len(comp["bundles"]) for comp in vcf_components.values()
                )
                total_size = sum(
                    sum(bundle["size"] for bundle in comp["bundles"])
                    for comp in vcf_components.values()
                )

                console.print(
                    f"\n[bold cyan]📊 Summary:[/bold cyan] {len(vcf_components)} components, "
                    f"{total_bundles} bundles, {format_size(total_size)} total"
                )
            else:
                console.print(
                    "[bold red]❌ No components found in the release[/bold red]"
                )
        else:
            console.print("[bold red]❌ No release elements found[/bold red]")
    else:
        console.print(
            "[bold red]❌ Unexpected release components response structure[/bold red]"
        )

    return vcf_components


async def get_bundle_download_status(client: VCFClient, version: str) -> Dict[str, Any]:
    """Get current download status for all bundles"""
    status_response = await client.api_call(
        "GET",
        f"/v1/bundles/download-status?imageType=INSTALL&releaseVersion={version}",
    )

    bundle_status_map = {}
    if isinstance(status_response, dict) and "elements" in status_response:
        for element in status_response["elements"]:
            bundle_id = element.get("bundleId")
            bundle_status_map[bundle_id] = element

    return bundle_status_map


def analyze_bundle_status(
    vcf_components: Dict[str, Any], bundle_status_map: Dict[str, Any]
) -> Tuple[bool, List[Tuple[str, Dict[str, Any]]]]:
    """Analyze bundle download status and identify bundles to download"""
    # Sort by component name so the download-initiation order (and its printed
    # lines) is stable across runs, matching the status table.
    all_bundles = []
    for comp_name, comp_data in sorted(vcf_components.items()):
        for bundle in comp_data["bundles"]:
            all_bundles.append((comp_name, bundle))

    all_success = True
    bundles_to_download = []

    for comp_name, bundle in all_bundles:
        bundle_id = bundle["id"]
        bundle_status = bundle_status_map.get(bundle_id, {})
        download_status = bundle_status.get("downloadStatus", "UNKNOWN")

        if download_status != "SUCCESS":
            all_success = False
            if download_status not in ("DOWNLOADING",):
                bundles_to_download.append((comp_name, bundle))

    return all_success, bundles_to_download


async def initiate_bundle_downloads(
    client: VCFClient,
    bundles_to_download: List[Tuple[str, Dict[str, Any]]],
    bundle_status_map: Dict[str, Any],
) -> Dict[str, Any]:
    """Initiate downloads for bundles with PENDING status.

    Per-bundle progress is shown by the live "Bundle Depot Download Status"
    table, so this stays silent on success and only reports a failure to
    initiate a download.
    """
    download_results = {}

    for comp_name, bundle in bundles_to_download:
        bundle_id = bundle["id"]
        bundle_status = bundle_status_map.get(bundle_id, {})
        download_status = bundle_status.get("downloadStatus", "UNKNOWN")

        if download_status != "PENDING":
            continue

        try:
            download_payload = {"bundleDownloadSpec": {"downloadNow": True}}
            result = await client.api_call(
                "PATCH",
                f"/v1/bundles/{bundle_id}",
                download_payload,
            )
            download_results.setdefault(comp_name, []).append(
                {"bundle_id": bundle_id, "status": "initiated", "result": result}
            )
        except Exception as e:
            download_results.setdefault(comp_name, []).append(
                {"bundle_id": bundle_id, "status": "error", "error": str(e)}
            )
            console.print(
                f"[bold red]✗ Failed to initiate download for {comp_name} "
                f"({bundle_id[:12]}…): {str(e)[:100]}[/bold red]"
            )

    return download_results


def generate_status_table(
    vcf_components: Dict[str, Any], status_data: Dict[str, Any]
) -> Table:
    """Generate status table with current download information"""
    table = Table(title="VCF Installer - Bundle Depot Download Status")
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Product Name", style="green")
    table.add_column("Version", style="green", no_wrap=True)
    table.add_column("Bundle ID", style="magenta", no_wrap=True)
    table.add_column("Size", style="blue", justify="right")
    table.add_column("Status", style="yellow", width=14)

    # Sort by component name so row order is stable across runs (the API /
    # download-completion order varies, which makes troubleshooting confusing).
    for comp_name, comp_data in sorted(vcf_components.items()):
        public_name = comp_data.get("publicName", "")
        for bundle in comp_data["bundles"]:
            bundle_id = bundle["id"]
            bundle_version = bundle["productVersion"]
            total_size = bundle["size"]
            bundle_status_info = status_data.get(bundle_id, {})
            download_status = bundle_status_info.get("downloadStatus", "UNKNOWN")
            downloaded_size = bundle_status_info.get("downloadedSize", 0)

            # Static glyph colour shared with the other displays, but NO spinner
            # here: a progress bar already conveys "in progress" for downloads.
            color = status_indicator(download_status)[1]
            in_progress = download_status in (
                "INPROGRESS",
                "DOWNLOADING",
                "VALIDATING",
            )
            if (
                download_status in ("INPROGRESS", "DOWNLOADING")
                and downloaded_size > 0
            ):
                percentage = (
                    min(100, int((downloaded_size / total_size) * 100))
                    if total_size > 0
                    else 0
                )
                filled_length = int((percentage / 100) * 10)
                progress_bar = "█" * filled_length + "░" * (10 - filled_length)
                status_display = f"[{color}]{progress_bar} {percentage}%[/{color}]"
            else:
                label = {
                    "SUCCESS": "SUCCESS",
                    "FAILED": "FAILED",
                    "PENDING": "NOT DOWNLOADED",
                    "SCHEDULED": "SCHEDULED",
                    "VALIDATING": "VALIDATING…",
                }.get(download_status, download_status)
                if in_progress:
                    status_display = f"[{color}]{_spinner_frame()} {label}[/{color}]"
                else:
                    icon = status_indicator(download_status)[0]
                    status_display = f"[{color}]{icon} {label}[/{color}]"

            table.add_row(
                comp_name,
                public_name,
                bundle_version,
                bundle_id,
                format_size(total_size),
                status_display,
            )

    return table


class LiveDownloadDisplay:
    """Dynamic renderable for the bundle download table so the in-progress
    spinner (e.g. VALIDATING…) animates on every Live refresh instead of
    freezing between the 5s-spaced polls. Progress bars only change on poll,
    which is fine — they reflect real downloaded bytes."""

    def __init__(self, vcf_components: Dict[str, Any]):
        self.vcf_components = vcf_components
        self.status_data: Dict[str, Any] = {}

    def __rich_console__(self, console, options):
        yield generate_status_table(self.vcf_components, self.status_data)


async def monitor_download_progress(
    client: VCFClient,
    vcf_components: Dict[str, Any],
    version: str,
) -> None:
    """Monitor download progress for all bundles using live updating table"""
    global monitoring_active
    monitoring_active = True
    live = None
    try:
        download_display = LiveDownloadDisplay(vcf_components)
        # 12.5 fps so the in-progress spinner animates smoothly between the
        # 5s-spaced polls (the renderable is dynamic, see the class).
        live = Live(download_display, refresh_per_second=12.5, transient=False)
        live.start()

        while True:
            try:
                status_response = await client.api_call(
                    "GET",
                    f"/v1/bundles/download-status?imageType=INSTALL&releaseVersion={version}",
                    log_response=False,  # polled every 5s — see api_call()
                )

                if isinstance(status_response, dict) and "elements" in status_response:
                    elements = status_response["elements"]
                    status_data = {}
                    for element in elements:
                        bundle_id = element.get("bundleId")
                        if bundle_id:
                            status_data[bundle_id] = element

                    # Hand fresh status to the dynamic renderable; Live
                    # re-renders it (advancing the spinner) each refresh.
                    download_display.status_data = status_data

                    # Check if all downloads are complete
                    all_complete = True
                    has_downloads = False

                    for comp_name, comp_data in vcf_components.items():
                        for bundle in comp_data["bundles"]:
                            bundle_id = bundle["id"]
                            bundle_status_info = status_data.get(bundle_id, {})
                            download_status = bundle_status_info.get(
                                "downloadStatus", "UNKNOWN"
                            )

                            # Reinitiate download if status is FAILED
                            if download_status == "FAILED":
                                download_payload = {
                                    "bundleDownloadSpec": {"downloadNow": True}
                                }
                                await client.api_call(
                                    "PATCH",
                                    f"/v1/bundles/{bundle_id}",
                                    download_payload,
                                )

                            if download_status in [
                                "DOWNLOADING",
                                "PENDING",
                                "SCHEDULED",
                                "VALIDATING",
                            ]:
                                has_downloads = True
                                all_complete = False
                                break
                        if not all_complete:
                            break

                    # Check if all downloads are successful
                    if not has_downloads:
                        all_success = True
                        for comp_name, comp_data in vcf_components.items():
                            for bundle in comp_data["bundles"]:
                                bundle_id = bundle["id"]
                                bundle_status_info = status_data.get(bundle_id, {})
                                download_status = bundle_status_info.get(
                                    "downloadStatus", "UNKNOWN"
                                )

                                if download_status != "SUCCESS":
                                    all_success = False
                                    break
                            if not all_success:
                                break

                        if all_success:
                            if live:
                                live.stop()
                            console.print(
                                "\n🎉 [bold green]All downloads completed successfully![/bold green]"
                            )
                            break

                else:
                    console.print(
                        "[red]Unexpected download status response structure[/red]"
                    )

            except Exception as e:
                console.print(f"[red]Error checking download status: {e}[/red]")

            await asyncio.sleep(5)

    except KeyboardInterrupt:
        if live:
            live.stop()
        console.print("\n\n⚠️ [yellow]Monitoring interrupted by user (Ctrl+C)[/yellow]")
        console.print("🔄 [blue]Stopping download monitoring...[/blue]")
    except Exception as e:
        if live:
            live.stop()
        console.print(f"\n❌ [red]Error in download monitoring: {e}[/red]")
    finally:
        # Guarantee cleanup: stop() is a no-op if the Live was never started or
        # is already stopped, so this covers every exit path. Then clear the flag.
        if live is not None:
            live.stop()
        monitoring_active = False


@timeit
async def initiate_sddc_validations(
    client: VCFClient,
    vcf_json: dict,
    validation_id: str = None,
):
    """
    Initiate and monitor SDDC validations with live visual display.

    Args:
        client (VCFClient): VCF API client for API calls
        vcf_json (dict): VCF configuration JSON
        validation_id (str, optional): Existing validation ID to monitor

    Raises:
        typer.Exit: On validation failures or errors
    """
    global monitoring_active

    phase_banner(6, "Validating SDDC Spec")

    if validation_id:
        console.print(
            f"[bold cyan]📋 Monitoring existing SDDC validation "
            f"(Validation ID: {validation_id})[/bold cyan]"
        )
        sddc_val_id = validation_id
    else:
        # Create new validation
        if global_debug:
            debug_console.print(f"[dim]Sending validation request with JSON payload:[/dim]")
            debug_console.print("-" * 80)
            debug_console.print(json.dumps(vcf_json, indent=2))
            debug_console.print("-" * 80)

        try:
            response = await client.api_call(
                "POST", "/v1/sddcs/validations", vcf_json, idempotent=False
            )
            sddc_val_id = response["id"]
            console.print(
                f"[bold cyan]📋 Initiating SDDC validations... "
                f"(Validation ID: {sddc_val_id})[/bold cyan]"
            )
        except Exception as e:
            if "403" in str(e):
                console.print("[red]Validation in progress, try again later.[/red]")
                raise typer.Exit(code=1)
            raise

    # Monitor validation progress with live display
    monitoring_active = True
    live = None

    try:
        validation_display = LiveValidationDisplay()
        with Live(
            validation_display,
            # 12.5 fps so the in-progress spinner animates smoothly between the
            # 5s-spaced API polls (the renderable is dynamic, see the class).
            refresh_per_second=12.5,
            console=console,
        ) as live:
            while True:
                try:
                    # log_response=False: polled every 5s — see api_call().
                    sddc_val = await client.api_call(
                        "GET", f"/v1/sddcs/validations/{sddc_val_id}",
                        log_response=False,
                    )

                    # Hand the fresh payload to the dynamic renderable; Live
                    # re-renders it (advancing the spinner) at refresh_per_second.
                    validation_display.validation = sddc_val

                    executionStatus = sddc_val["executionStatus"]

                    if executionStatus != "IN_PROGRESS":
                        # Terminal state — dump the full payload once.
                        if global_debug:
                            debug_console.print(
                                f"[dim]Final SDDC validation response "
                                f"({executionStatus}):\n"
                                f"{json.dumps(sddc_val, indent=2)}[/dim]"
                            )
                        break

                    await asyncio.sleep(5)

                except Exception as e:
                    console.print(f"[red]Error checking validation status: {e}[/red]")
                    await asyncio.sleep(10)
                    continue

    except KeyboardInterrupt:
        if live:
            live.stop()
        console.print(
            "\n\n⚠️ [yellow]Validation monitoring interrupted by user (Ctrl+C)[/yellow]"
        )
        console.print("🔄 [blue]Stopping validation monitoring...[/blue]")
    except Exception as e:
        if live:
            live.stop()
        console.print(f"\n❌ [red]Error in validation monitoring: {e}[/red]")
    finally:
        # Guarantee cleanup: stop() is a no-op if the Live was never started or
        # is already stopped, so this covers every exit path. Then clear the flag.
        if live is not None:
            live.stop()
        monitoring_active = False

    # Check final status
    if executionStatus == "COMPLETED":
        console.print(
            "\n[bold green]✅ SDDC validation completed successfully![/bold green]"
        )
        return
    elif executionStatus == "FAILED":
        console.print("\n[bold red]❌ SDDC validation failed[/bold red]")
        # Full payload already dumped to the debug log on the
        # terminal-state break above.
        raise typer.Exit(code=1)

    console.print(
        f"\n[bold red]❌ Unexpected executionStatus: {executionStatus}[/bold red]"
    )
    raise typer.Exit(code=1)


@timeit
async def initiate_sddc_deployment(
    client: VCFClient,
    vcf_json: dict,
    sddc_id: str = None,
):
    """
    Initiate and monitor SDDC deployment with live visual display.

    Args:
        client (VCFClient): VCF API client for API calls
        vcf_json (dict): VCF configuration JSON
        sddc_id (str, optional): Existing SDDC ID to monitor

    Raises:
        typer.Exit: On deployment failures or errors
    """
    global monitoring_active, final_deployment_seconds, sddc_end_iso

    phase_banner(7, "Deploying SDDC")

    # Create new deployment if no sddc_id provided
    if not sddc_id:
        # Guard against creating a duplicate: if a deployment already exists
        # (e.g. a previous run created one), attach to it instead of POSTing.
        existing = await get_latest_sddc(client)
        existing_id = existing["id"] if existing else None
        if existing_id:
            console.print(
                f"[yellow]⚠️ An SDDC deployment already exists ({existing_id}) "
                f"— attaching to it instead of creating a new one.[/yellow]"
            )
            sddc_id = existing_id
        else:
            debug_console.print("[cyan]Creating new SDDC deployment...[/cyan]")
            try:
                # POST /v1/sddcs is NOT idempotent — attempt it exactly once.
                sddc = await client.api_call(
                    "POST", "/v1/sddcs", vcf_json, idempotent=False
                )
                sddc_id = sddc["id"]
            except Exception as e:
                # A timeout/error here may still mean the deployment was
                # created server-side. Recover by querying the latest
                # deployment rather than retrying the POST (which would
                # create a duplicate).
                console.print(
                    f"[yellow]⚠️ SDDC create request did not return cleanly "
                    f"({type(e).__name__}). Checking whether it was created "
                    f"anyway...[/yellow]"
                )
                recovered = await get_latest_sddc(client)
                sddc_id = recovered["id"] if recovered else None
                if not sddc_id:
                    console.print(
                        "[bold red]❌ SDDC deployment was not created.[/bold red]"
                    )
                    raise typer.Exit(code=1)

        console.print(
            f"[bold cyan]🚀 Initiating SDDC deployment... "
            f"(SDDC ID: {sddc_id})[/bold cyan]"
        )
    else:
        console.print(
            f"[cyan]Monitoring existing SDDC deployment with ID: {sddc_id}[/cyan]"
        )

    # Monitor deployment progress with live display.
    #
    # A VCF deployment can end in COMPLETED_WITH_FAILURE on a transient or
    # otherwise recoverable error; the installer supports resuming it via
    # PATCH /v1/sddcs/{id}. Wrap the live monitor in a resume loop so a failed
    # deployment is retried automatically in-flight — not only when the script
    # is relaunched.
    max_resume_attempts = 3
    resume_attempt = 0

    while True:
        monitoring_active = True
        live = None

        try:
            deployment_display = LiveDeploymentDisplay()
            with Live(
                deployment_display,
                # 12.5 fps so the in-progress milestone spinner animates
                # smoothly between the 5s-spaced API polls.
                refresh_per_second=12.5,
                console=console,
            ) as live:
                while True:
                    try:
                        # log_response=False: this GET is polled every 5s for
                        # the whole (multi-hour) deployment — dumping its full
                        # JSON each time is what bloated debug logs to 100s of
                        # MB. The full body is dumped once on terminal status.
                        sddc = await client.api_call(
                            "GET", f"/v1/sddcs/{sddc_id}", log_response=False
                        )

                        # Hand the fresh payload to the dynamic renderable;
                        # Live re-renders it (and advances the spinner) at
                        # refresh_per_second.
                        deployment_display.sddc = sddc

                        # Check if sddcSubTasks exists
                        if "sddcSubTasks" not in sddc:
                            await asyncio.sleep(5)
                            continue

                        sddc_status = sddc["status"]
                        if sddc_status != "IN_PROGRESS":
                            # Terminal state — record the real (milestone-
                            # derived) deployment time for the final summary,
                            # the absolute SDDC end timestamp (latest
                            # milestone updateTimestamp) so main() can pair
                            # it with the zPod creation_date for the true
                            # total, and dump the full payload once for
                            # troubleshooting (per-poll dumps are suppressed).
                            final_deployment_seconds = (
                                sddc_milestones_elapsed_seconds(sddc)
                            )
                            sddc_end_iso = max(
                                (
                                    m["updateTimestamp"]
                                    for m in (sddc.get("milestones") or [])
                                    if m.get("updateTimestamp")
                                ),
                                default=None,
                            )
                            if global_debug:
                                debug_console.print(
                                    f"[dim]Final SDDC deployment response "
                                    f"({sddc_status}):\n"
                                    f"{json.dumps(sddc, indent=2)}[/dim]"
                                )
                            break

                        await asyncio.sleep(5)

                    except Exception as e:
                        console.print(
                            f"[red]Error checking deployment status: {e}[/red]"
                        )
                        await asyncio.sleep(5)
                        continue

        except KeyboardInterrupt:
            if live:
                live.stop()
            console.print(
                "\n\n⚠️ [yellow]Deployment monitoring interrupted by user (Ctrl+C)[/yellow]"
            )
            console.print("🔄 [blue]Stopping deployment monitoring...[/blue]")
            return
        except Exception as e:
            if live:
                live.stop()
            console.print(f"\n❌ [red]Error in deployment monitoring: {e}[/red]")
            raise typer.Exit(code=1)
        finally:
            # Guarantee cleanup: stop() is a no-op if not started / already
            # stopped, covering every exit path. Then clear the flag.
            if live is not None:
                live.stop()
            monitoring_active = False

        # Deployment reached a terminal state — decide what to do next.
        if sddc_status == "COMPLETED_WITH_SUCCESS":
            console.print(
                "\n[bold green]✅ SDDC deployment completed successfully![/bold green]"
            )
            return

        if sddc_status == "COMPLETED_WITH_FAILURE":
            if resume_attempt >= max_resume_attempts:
                console.print(
                    f"\n[bold red]❌ SDDC deployment still failing after "
                    f"{max_resume_attempts} resume attempt(s)[/bold red]"
                )
                # Full payload already dumped to the debug log on the
                # terminal-state break above.
                raise typer.Exit(code=1)

            resume_attempt += 1
            console.print(
                f"\n[yellow]⚠️ SDDC deployment ended with COMPLETED_WITH_FAILURE "
                f"— resuming (attempt {resume_attempt}/{max_resume_attempts})..."
                f"[/yellow]"
            )
            try:
                await client.api_call("PATCH", f"/v1/sddcs/{sddc_id}")
                console.print(
                    "[bold green]✓ Deployment resume request sent successfully"
                    "[/bold green]"
                )
            except Exception as e:
                console.print(f"[bold red]❌ Resume request failed: {e}[/bold red]")
                raise typer.Exit(code=1)

            # The installer needs a moment to act on the PATCH and flip the
            # deployment back to IN_PROGRESS. Wait for that before re-entering
            # the live monitor, otherwise the first GET would still read
            # COMPLETED_WITH_FAILURE and burn another resume attempt.
            console.print("[cyan]⏳ Waiting for the installer to resume...[/cyan]")
            for _ in range(24):  # up to ~2 minutes
                await asyncio.sleep(5)
                try:
                    refreshed = await client.api_call(
                        "GET", f"/v1/sddcs/{sddc_id}", log_response=False
                    )
                    if refreshed.get("status") == "IN_PROGRESS":
                        break
                except Exception:
                    continue
            continue

        if sddc_status == "FAILED":
            console.print("\n[bold red]❌ SDDC deployment failed[/bold red]")
            # Full payload already dumped to the debug log on the
            # terminal-state break above.
            raise typer.Exit(code=1)

        console.print(f"\n[bold red]❌ Unexpected status: {sddc_status}[/bold red]")
        raise typer.Exit(code=1)


def generate_validation_status_display(validation_data: dict = None) -> Text:
    """
    Generate a formatted display for validation status with color-coded indicators.

    Args:
        validation_data (dict, optional): Validation response data from the API

    Returns:
        Text: Rich text containing the formatted validation status
    """
    # Handle empty or None data gracefully
    if not validation_data:
        return Text("Waiting for validation data…", style="dim italic")

    validation_checks = validation_data.get("validationChecks", [])

    # Header label only (the per-check lines below carry the leading glyphs;
    # the one-time "Initiating SDDC validations…" line carries the ID).
    out = Text()
    out.append("VCF Spec Validation Status:", style="bold white")

    for check in validation_checks:
        check_description = check.get("description", "Unknown Check")
        check_status = check.get("resultStatus", "UNKNOWN")

        # Leading status glyph, consistent across displays.
        cs = (check_status or "").upper()
        if cs == "SUCCEEDED":
            icon, color = "✓", "green"
        elif "IN_PROGRESS" in cs:
            icon, color = _spinner_frame(), "blue"
        elif "FAIL" in cs or "ERROR" in cs:
            icon, color = "✗", "red"
        elif cs == "SKIPPED":
            icon, color = "⊝", "bright_black"
        else:  # UNKNOWN / not yet evaluated -> pending
            icon, color = "◌", "bright_black"

        out.append("\n  ")
        out.append(icon, style=f"bold {color}")
        out.append(" ")
        out.append(check_description, style="white")

        # Error details for non-success checks: indented severity/code/message.
        if check_status != "SUCCEEDED" and check.get("errorResponse"):
            nested_errors = check["errorResponse"].get("nestedErrors", [])
            if nested_errors:
                out.append(f" ({len(nested_errors)} error(s))", style="dim")
                for err in nested_errors:
                    code = err.get("errorCode", "UNKNOWN")
                    if "." in code:
                        base_code, _, suffix = code.rpartition(".")
                        severity = suffix.upper() if suffix else check_status
                    else:
                        base_code, severity = code, check_status
                    sev_color = (
                        "red" if severity in ("ERROR", "FAILED") else "yellow"
                    )
                    message = err.get("message", "")
                    out.append("\n      - ", style="dim")
                    out.append(severity, style=f"bold {sev_color}")
                    out.append(" - ", style="dim")
                    out.append(base_code, style=sev_color)
                    out.append(f": '{message}'", style="white")

    return out


def format_timestamp(timestamp_str: str) -> str:
    """
    Format ISO timestamp to human-readable time.

    Args:
        timestamp_str (str): ISO timestamp string

    Returns:
        str: Formatted time string (HH:MM:SS)
    """
    try:
        # Parse ISO timestamp and format as HH:MM:SS
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return dt.strftime("%H:%M:%S")
    except (ValueError, AttributeError):
        return "??:??:??"


def elapsed_seconds(start_iso: str, end_iso: str = None) -> Optional[int]:
    """
    Compute the number of seconds between two ISO timestamps.

    Args:
        start_iso (str): ISO start timestamp
        end_iso (str, optional): ISO end timestamp. Defaults to now (UTC)
            when omitted — i.e. the milestone is still in progress.

    Returns:
        Optional[int]: Elapsed whole seconds, or None if unparseable.
    """
    try:
        start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        if start.tzinfo is None:
            # zPodFactory stores creation_date as a naive datetime built from
            # datetime.now(UTC); attach UTC so it can be subtracted from the
            # tz-aware VCF milestone timestamps.
            start = start.replace(tzinfo=timezone.utc)
        if end_iso:
            end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
            if end.tzinfo is None:
                end = end.replace(tzinfo=timezone.utc)
        else:
            end = datetime.now(timezone.utc)
        return max(0, int((end - start).total_seconds()))
    except (ValueError, AttributeError, TypeError):
        return None


def format_seconds(total_seconds: int) -> str:
    """Render a whole-second count as a human-readable duration.

    Returns a string like "1 hour 55 minutes" / "23 minutes" / "8 seconds".
    """

    def _plural(value: int, unit: str) -> str:
        return f"{value} {unit}" + ("" if value == 1 else "s")

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours:
        return f"{_plural(hours, 'hour')} {_plural(minutes, 'minute')}"
    if minutes:
        return _plural(minutes, "minute")
    return _plural(seconds, "second")


def sddc_milestones_elapsed_seconds(sddc: dict) -> Optional[int]:
    """Wall-clock deployment time from an SDDC object.

    Spans from the earliest milestone ``creationTimestamp`` (falling back to the
    SDDC's own ``creationTimestamp``) to:

    * **now**, while the deployment is still in progress — the running milestone
      has no ``updateTimestamp`` yet, so a ``max(updateTimestamp)`` span would
      read ~0; we want a live counter from the start instead; or
    * the latest milestone ``updateTimestamp`` once terminal — the true total.
      NOT the sum of per-milestone durations: milestones have gaps and some
      9.0.x ones report ``creation == update``, which summing undercounts. The
      span matches the subtask span and is independent of when monitoring began.

    Returns None when no usable start timestamp is present.
    """
    # Collect every creation/update timestamp in the object — top-level,
    # milestones AND subtasks. On 9.1 a milestone's creationTimestamp is set
    # when the milestone is *reached*, which can be LATER than its own
    # subtasks' timestamps; anchoring to the earliest timestamp anywhere avoids
    # the resulting undercount (the "30 seconds" at task 13/14).
    starts, ends = [], []
    if sddc.get("creationTimestamp"):
        starts.append(sddc["creationTimestamp"])
    for group in ("milestones", "sddcSubTasks"):
        for item in sddc.get(group) or []:
            if item.get("creationTimestamp"):
                starts.append(item["creationTimestamp"])
            if item.get("updateTimestamp"):
                ends.append(item["updateTimestamp"])
    if not starts:
        return None
    start = min(starts)  # ISO-8601 UTC strings sort chronologically

    status = (sddc.get("status") or "").upper()
    if status in ("COMPLETED_WITH_SUCCESS", "FAILED", "COMPLETED_WITH_FAILURE"):
        # Terminal: span to the latest recorded update (milestone or subtask).
        return elapsed_seconds(start, max(ends) if ends else None)
    # Still in progress: live elapsed from the earliest start (end -> now).
    return elapsed_seconds(start)


# Rich's built-in "dots" spinner drives the in-progress milestone indicator.
# rich.spinner.Spinner is a block renderable, so it can't be embedded inline
# in a Text — instead we let it own the frame set and timing and just pull
# the current glyph for the milestone line.
_MILESTONE_SPINNER = Spinner("dots")


def _spinner_frame() -> str:
    """Return the current 'dots' spinner glyph for the wall-clock time."""
    return _MILESTONE_SPINNER.render(time.time()).plain


# One visual language for item status across every live display (bundle table,
# validation checks, deployment milestones). Maps the various API status
# vocabularies onto a single (glyph, rich-style) pair so success / failure /
# in-progress / scheduled look identical everywhere. In-progress always uses the
# animated "dots" spinner — never a static emoji.
def status_indicator(status: str) -> Tuple[str, str]:
    """Return ``(glyph, style)`` for an item status string.

    Matches on substrings so prefixed API statuses (e.g.
    ``POSTVALIDATION_COMPLETED_WITH_SUCCESS``) map correctly. Failure is checked
    before success so ``COMPLETED_WITH_FAILURE`` resolves to a failure.
    """
    s = (status or "").upper()
    if "FAIL" in s or "ERROR" in s:
        return "✗", "red"
    if "SUCCESS" in s or "SUCCEEDED" in s or "COMPLETED" in s:
        return "✓", "green"
    if (
        "IN_PROGRESS" in s
        or "INPROGRESS" in s
        or "DOWNLOADING" in s
        or "VALIDATING" in s
    ):
        return _spinner_frame(), "blue"
    if (
        "SCHEDULED" in s
        or "PENDING" in s
        or "QUEUED" in s
        or "NOT_STARTED" in s
        or "INITIALIZED" in s
    ):
        return "◌", "cyan"
    if "SKIPPED" in s:
        return "⊝", "dim"
    if "WARNING" in s:
        return "⚠", "yellow"
    return "?", "yellow"


class LiveDeploymentDisplay:
    """Dynamic renderable wrapper for the deployment status display.

    Holding the SDDC payload behind a renderable — rather than handing Live a
    static Text — lets ``generate_deployment_status_display`` be re-invoked on
    every Live refresh. That is what animates the in-progress milestone
    spinner between the (5s-spaced) API polls.
    """

    def __init__(self, sddc: dict = None):
        self.sddc = sddc

    def __rich_console__(self, console, options):
        yield generate_deployment_status_display(self.sddc)


class LiveValidationDisplay:
    """Dynamic renderable for the validation status display.

    Same idea as LiveDeploymentDisplay: re-invoke the generator on every Live
    refresh so the in-progress spinner animates fluidly, instead of freezing on
    a static Text between the (5s-spaced) API polls.
    """

    def __init__(self, validation: dict = None):
        self.validation = validation

    def __rich_console__(self, console, options):
        yield generate_validation_status_display(self.validation)


def generate_deployment_status_display(deployment_data: dict = None) -> Text:
    """
    Generate a formatted display for deployment status with milestones and subtasks.

    Args:
        deployment_data (dict, optional): Deployment response data from the API

    Returns:
        Text: Rich text containing the formatted deployment status
    """
    # Handle empty or None data gracefully
    if not deployment_data:
        return Text("🚀 Waiting for deployment data...", style="dim italic")

    # Extract main status information
    name = deployment_data.get("name", "Unknown")
    status = deployment_data.get("status", "UNKNOWN")
    milestones = deployment_data.get("milestones", [])
    sddc_subtasks = deployment_data.get("sddcSubTasks", [])

    # Per-milestone elapsed time. milestone_labels[i] is the parenthesised
    # individual time, e.g. "(23 minutes)", or None when unparseable.
    milestone_labels = []
    for milestone in milestones:
        creation = milestone.get("creationTimestamp")
        update = milestone.get("updateTimestamp")
        secs = elapsed_seconds(creation, update)
        # Some 9.0.x milestones (vSphere cluster, NSX) report creation==update,
        # i.e. they were never individually timed (work shows in their
        # subtasks). Don't render a misleading "(0 seconds)" for those.
        if secs is None or (creation and update and creation == update):
            milestone_labels.append(None)
            continue
        label = format_seconds(secs)
        if milestone.get("status") == "IN_PROGRESS":
            label = f"{label} so far"
        milestone_labels.append(f"({label})")

    # Total deployment time so far (sum across milestones) — same source the
    # final summary uses, so the header and summary always agree.
    total_deployment_seconds = sddc_milestones_elapsed_seconds(deployment_data) or 0

    # Build the main status line. While running, show a blue spinner +
    # IN_PROGRESS and the live total deployment time; terminal states show
    # the plain coloured status word.
    main_status = Text()
    main_status.append(f"{name}: ", style="bold white")
    if status == "IN_PROGRESS":
        main_status.append(f"{_spinner_frame()} {status}", style="bold blue")
        main_status.append(
            f" (Current deployment time: "
            f"{format_seconds(total_deployment_seconds)})",
            style="yellow",
        )
    else:
        status_color = {
            "COMPLETED_WITH_SUCCESS": "green",
            "FAILED": "red",
        }.get(status, "yellow")
        main_status.append(status, style=f"bold {status_color}")

    # If no milestones, just return the main status
    if not milestones:
        return main_status

    # Build the milestones and subtasks display
    milestones_text = Text()

    # First pass: collect all task names to find the longest for alignment
    all_task_names = []
    for milestone in milestones:
        milestone_name = milestone.get("name", "Unknown Milestone")
        all_task_names.append(f"  MILESTONE: {milestone_name}")

        # If milestone is in progress, collect subtask names
        if milestone.get("status") == "IN_PROGRESS":
            # Get all subtasks (including INITIALIZED)
            all_subtasks = list(sddc_subtasks)
            milestone_subtasks = get_sorted_milestone_subtasks(
                all_subtasks, milestone=milestone, n=10
            )

            for subtask in milestone_subtasks:
                subtask_name = subtask.get("name", "Unknown Subtask")
                all_task_names.append(f"    TASK: {subtask_name}")

    # Find the longest task name for alignment
    max_task_name_length = (
        max(len(name) for name in all_task_names) if all_task_names else 0
    )

    # Width of the widest task-count badge, so the elapsed-time column lines
    # up across milestones regardless of badge size (e.g. "(6/19)" vs
    # "(133/133)").
    max_badge_length = 0
    for milestone in milestones:
        total_tasks = milestone.get("totalTasks")
        if total_tasks is not None:
            completed_tasks = milestone.get("completedTasks", 0)
            max_badge_length = max(
                max_badge_length, len(f"({completed_tasks}/{total_tasks})")
            )

    # Second pass: build the actual display with alignment
    for i, milestone in enumerate(milestones):
        milestone_name = milestone.get("name", "Unknown Milestone")
        milestone_status = milestone.get("status", "UNKNOWN")

        # Determine color and icon for milestone status
        milestone_icon, milestone_color = status_indicator(milestone_status)

        # Add milestone line with aligned icon
        milestones_text.append("  MILESTONE: ", style="bold cyan")
        milestones_text.append(f"{milestone_name}", style="bold white")

        # Pad with spaces to align the status icon
        task_name = f"  MILESTONE: {milestone_name}"
        padding_length = max_task_name_length - len(task_name) + 2
        milestones_text.append(" " * padding_length, style="white")
        milestones_text.append(milestone_icon, style=f"bold {milestone_color}")

        # Add creation timestamp if available
        creation_timestamp = milestone.get("creationTimestamp")
        if creation_timestamp:
            formatted_time = format_timestamp(creation_timestamp)
            milestones_text.append(" ", style="white")
            milestones_text.append(f"[{formatted_time}]", style="dim")

        # Add a task-count badge when available (VCF 9.1 milestones carry
        # completedTasks/totalTasks; absent on 9.0 so the badge is omitted)
        total_tasks = milestone.get("totalTasks")
        if total_tasks is not None:
            completed_tasks = milestone.get("completedTasks", 0)
            badge = f"({completed_tasks}/{total_tasks})"
            milestones_text.append(" ", style="white")
            milestones_text.append(badge, style="dim cyan")
        else:
            badge = ""
            if max_badge_length:
                milestones_text.append(" ", style="white")

        # Add the milestone's elapsed time: total wall-clock for a finished
        # milestone (updateTimestamp marks completion), or "so far" while it
        # is still running. The badge above is right-padded to a fixed width
        # so this elapsed-time column lines up across all milestones.
        duration_label = milestone_labels[i]
        if duration_label:
            milestones_text.append(
                " " * (max_badge_length - len(badge) + 1), style="white"
            )
            milestones_text.append(duration_label, style="dim")

        # If milestone is in progress, show its subtasks
        if milestone_status == "IN_PROGRESS":
            # Get all subtasks (including INITIALIZED)
            all_subtasks = list(sddc_subtasks)
            milestone_subtasks = get_sorted_milestone_subtasks(
                all_subtasks, milestone=milestone, n=10
            )

            for subtask in milestone_subtasks:
                subtask_name = subtask.get("name", "Unknown Subtask")
                subtask_status = subtask.get("status", "UNKNOWN")

                # Determine color and icon for subtask status (shared language)
                subtask_icon, subtask_color = status_indicator(subtask_status)

                milestones_text.append("\n")
                # Add subtask with aligned icon
                milestones_text.append("    TASK: ", style="dim cyan")
                milestones_text.append(f"{subtask_name}", style="white")

                # Pad with spaces to align the status icon
                subtask_name_display = f"    TASK: {subtask_name}"
                padding_length = max_task_name_length - len(subtask_name_display) + 2
                milestones_text.append(" " * padding_length, style="white")
                milestones_text.append(subtask_icon, style=f"bold {subtask_color}")

                # Add creation timestamp if available
                subtask_creation_timestamp = subtask.get("creationTimestamp")
                if subtask_creation_timestamp:
                    formatted_time = format_timestamp(subtask_creation_timestamp)
                    milestones_text.append(" ", style="white")
                    milestones_text.append(f"[{formatted_time}]", style="dim")

        if i < len(milestones) - 1:
            milestones_text.append("\n")

    # Combine main status and milestones
    full_text = Text()
    full_text.append(main_status)
    full_text.append("\n")
    full_text.append(milestones_text)

    return full_text


def _window_subtasks(tasks, n=10):
    # Show a window of n subtasks around the current activity point.
    # The subtask list is sequential — tasks progress from INITIALIZED →
    # IN_PROGRESS → completed. We find the activity point and show a
    # window centered around it to stay within the current milestone scope.

    if not tasks:
        return []

    # Find the IN_PROGRESS task index as our anchor point
    anchor_idx = None
    for i, s in enumerate(tasks):
        if s.get("status", "") == "IN_PROGRESS":
            anchor_idx = i
            break

    # If no IN_PROGRESS task, find the first INITIALIZED task (next up)
    if anchor_idx is None:
        for i, s in enumerate(tasks):
            if s.get("status", "") == "INITIALIZED":
                anchor_idx = i
                break

    # If still no anchor (all completed), show the last n
    if anchor_idx is None:
        return tasks[-n:]

    # Show a window: a few completed tasks before the anchor, then forward
    context_before = 3
    start = max(0, anchor_idx - context_before)
    end = min(len(tasks), start + n)
    # Adjust start if we're near the end of the list
    if end - start < n:
        start = max(0, end - n)

    return tasks[start:end]


def get_sorted_milestone_subtasks(all_subtasks, milestone=None, n=10):
    # VCF 9.1 tags each subtask with a "milestoneTask" field equal to its
    # parent milestone's name — when present, scope the window to just that
    # milestone's subtasks. VCF 9.0 payloads have no such field, so fall back
    # to a flat window over the whole subtask list (unchanged 9.0 behavior).
    if milestone is not None:
        milestone_name = milestone.get("name")
        grouped = [
            s for s in all_subtasks if s.get("milestoneTask") == milestone_name
        ]
        if grouped:
            return _window_subtasks(grouped, n)

    return _window_subtasks(all_subtasks, n)


if __name__ == "__main__":
    try:
        app()
    except typer.BadParameter:
        typer.echo(app.get_help())
        sys.exit(0)
    except typer.Exit as e:
        sys.exit(e.exit_code)
    except KeyboardInterrupt:
        console.print(
            "\n\n[bold yellow]⚠️ Script interrupted by user (Ctrl+C)[/bold yellow]"
        )
        console.print("[cyan]🔄 Exiting gracefully...[/cyan]")
        sys.exit(0)
    except Exception as e:
        typer.echo(app.get_help())
        console.print(f"\n[bold red]❌ Error: {e}[/bold red]")
        sys.exit(1)
