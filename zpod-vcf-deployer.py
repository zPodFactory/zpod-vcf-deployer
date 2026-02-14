#!/usr/bin/env python3
# /// script
# dependencies = ["httpx", "rich", "typer", "python-dotenv", "jinja2", "typing-extensions"]
# ///

import asyncio
import json
import math
import signal
import sys
import time
from datetime import datetime
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
    vcf_sku: Annotated[
        str,
        typer.Option(
            "--vcf-sku",
            help="VCF SKU (VCF or VVF)",
            envvar="VCF_SKU",
        ),
    ] = "VCF",
    vcf_version: Annotated[
        str,
        typer.Option(
            "--vcf-version",
            help="VCF version",
            envvar="VCF_VERSION",
        ),
    ] = "9.0.0.0",
    offline_depot_port: Annotated[
        int,
        typer.Option(
            "--offline-depot-port",
            help="Offline depot port",
            envvar="VCF_OFFLINE_DEPOT_PORT",
        ),
    ] = 443,
    debug: bool = typer.Option(
        False,
        "--debug",
        "-d",
        help="Enable debug output (API headers, payloads, responses)",
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
    global_debug = debug
    if debug:
        console.print("[bold yellow]Debug mode enabled[/bold yellow]")

    # Validate VCF SKU
    if vcf_sku.upper() not in ("VCF", "VVF"):
        typer.secho(
            "[red]Error: --vcf-sku must be either 'VCF' or 'VVF'[/red]", err=True
        )
        raise typer.Exit(code=1)

    # Validate depot mode and required parameters
    if depot_mode.lower() not in ("online", "offline"):
        typer.secho(
            "[red]Error: --depot-mode must be either 'online' or 'offline'[/red]",
            err=True,
        )
        raise typer.Exit(code=1)

    if depot_mode.lower() == "online":
        if not online_depot_download_token:
            typer.secho(
                "[red]Error: --online-depot-download-token is required for online mode[/red]",
                err=True,
            )
            raise typer.Exit(code=1)
    else:  # offline mode
        if (
            not offline_depot_hostname
            or not offline_depot_username
            or not offline_depot_password
        ):
            typer.secho(
                "[red]Error: --offline-depot-hostname, --offline-depot-username, and --offline-depot-password are required for offline mode[/red]",
                err=True,
            )
            raise typer.Exit(code=1)

    # Run the deployment with overall timing
    start_time = time.perf_counter()
    console.print("[bold cyan]Starting zPod VCF deployment...[/bold cyan]")

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
            vcf_sku=vcf_sku,
            vcf_version=vcf_version,
        )
    )

    end_time = time.perf_counter()
    total_time = end_time - start_time

    time_str = format_time(total_time)
    console.print(f"[bold green]✅ Total deployment time: {time_str}[/bold green]")


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

        # Format time to be more human-friendly
        time_str = format_time(total_time)

        # Always show timing for major steps
        console.print(
            f"[bold green]✅ {step_name} completed in {time_str}[/bold green]"
        )

        # Additional debug info if debug mode is enabled
        if global_debug:
            console.print(f"[dim]Function {func.__name__} took {time_str}[/dim]\n")

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
        self.client = httpx.AsyncClient(verify=False)  # Skip SSL verification

    async def _handle_connection_error(
        self,
        error: Exception,
        attempt: int,
        max_retries: int,
        retry_delay: int,
        operation_name: str = "operation",
    ) -> bool:
        """Handle connection errors with retry logic"""
        if attempt < max_retries - 1:
            error_msg = f"{type(error).__name__}"
            console.print(
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
    ) -> httpx.Response:
        """Make HTTP request with retry logic and error handling"""
        for attempt in range(max_retries):
            try:
                response = await self.client.request(
                    method, url, json=data, headers=headers
                )

                if global_debug:
                    console.print(
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
                        console.print()
                        console.print(
                            f"[yellow]⚠️ Authentication token expired (HTTP 401).[/yellow]"
                        )
                        console.print(
                            f"[yellow]Refreshing token and retrying... (Attempt {attempt + 1:2d}/{max_retries})[/yellow]"
                        )
                    try:
                        await self.refresh_token()
                        # Update headers with new token for next attempt
                        if headers:
                            headers["Authorization"] = f"Bearer {self.token}"
                        if global_debug:
                            console.print(
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
                    console.print(
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
                        console.print(
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
                    console.print(
                        f"[red]Connection error: {type(e).__name__}: {str(e)}[/red]"
                    )
                if await self._handle_connection_error(
                    e, attempt, max_retries, retry_delay, "Connection"
                ):
                    continue
                else:
                    raise Exception(f"Connection error: {type(e).__name__}: {str(e)}")

            except Exception as e:
                if global_debug:
                    console.print(
                        f"[red]Unexpected error: {type(e).__name__}: {str(e)}[/red]"
                    )
                if await self._handle_connection_error(
                    e, attempt, max_retries, retry_delay, "Unexpected"
                ):
                    continue
                else:
                    raise

    async def get_token(self) -> str:
        """Get access token from VCF API with retry logic"""
        url = f"{self.base_url}/v1/tokens"
        data = {"username": self.username, "password": self.password}

        if global_debug:
            console.print(f"[dim]Making POST request to: {url}[/dim]")
            console.print(f"[dim]Request data: {json.dumps(data, indent=2)}[/dim]")

        response = await self._make_request("POST", url, data=data, retry_delay=30)

        token_data = response.json()
        self.token = token_data.get("accessToken")

        if global_debug:
            console.print(
                f"[dim]Token Response: {json.dumps(token_data, indent=2)}[/dim]"
            )
            console.print("[bold green]✓ Authentication successful[/bold green]")

        return self.token

    async def refresh_token(self) -> str:
        """Refresh the access token"""
        if global_debug:
            console.print("[dim]🔄 Refreshing authentication token...[/dim]")

        # Clear the current token to force a new authentication
        self.token = None
        return await self.get_token()

    async def api_call(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make API call with authentication and automatic token refresh"""
        if not self.token:
            await self.get_token()

        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        if global_debug:
            console.print(f"\n[dim]Making {method} request to: {url}[/dim]")
            if data:
                console.print(f"[dim]Request data: {json.dumps(data, indent=2)}[/dim]")

        response = await self._make_request(
            method, url, data=data, headers=headers, handle_401=True
        )

        # Handle HTTP 204 (No Content) responses
        if response.status_code == 204:
            if global_debug:
                console.print(
                    f"[dim]Response ({response.status_code}): "
                    "No Content (empty response body)[/dim]"
                )
            return {"status": "success", "message": "No content returned"}

        result = response.json()
        if global_debug:
            console.print(
                f"[dim]Response ({response.status_code}): "
                f"{json.dumps(result, indent=2)}[/dim]"
            )
        return result

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


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
    vcf_sku: str = "VCF",
    vcf_version: str = "9.0.0.0",
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
        vcf_sku (str, optional): VCF SKU ('VCF' or 'VVF', default: 'VCF')
        vcf_version (str, optional): VCF version (default: '9.0.0.0')
    Raises:
        typer.Exit: On deployment failures
    """
    setup_signal_handlers()

    # Parse VCF JSON template
    vcf_template_data = json.loads(vcf_json_template.read())

    # Create zPod client
    zpod_client = httpx.Client(
        base_url=zpodfactory_base_url,
        headers={"access_token": zpodfactory_access_token},
        timeout=httpx.Timeout(30.0, connect=60.0),
    )

    try:
        # Deploy zPod
        zpod = await deploy_zpod(
            zpod_client=zpod_client,
            zpod_name=zpod_name,
            profile=zpodfactory_profile,
            endpoint_name=zpodfactory_endpoint,
        )

        # Fetch zPodFactory host IP from settings API
        zpodfactory_ip = fetch_zpodfactory_host_ip(zpod_client)

        # Build VCF template
        vcf_json = build_vcf_template(
            zpod, json.dumps(vcf_template_data), zpodfactory_ip
        )

        # Write VCF template
        output_file = (
            Path("/tmp") / f"{zpod_name}-{time.strftime('%Y%m%d-%H%M%S')}.json"
        )
        write_vcf_template(vcf_json, output_file)

        # Configure DNS
        configure_dns(zpod_client, zpod_name, vcf_json)

        # Create VCF API client for SDDC operations
        vcf_client = VCFClient(
            normalize_vcf_url(f"vcfinstaller.{zpod['domain']}"),
            "admin@local",
            zpod["password"],
        )

        # Configure VCF depot and download bundles
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
        latest_validation = await check_latest_validation(vcf_client)

        if not latest_validation:
            # No existing validation - launch validation
            console.print(
                "[cyan]No existing validation found. Starting validation...[/cyan]"
            )
            await _run_sddc_operations(
                vcf_client, vcf_json, "Starting fresh validation and deployment"
            )
            return

        # Check for existing SDDC deployment
        latest_sddc = await check_latest_sddc(vcf_client)

        if not latest_sddc:
            # No existing SDDC deployment - monitor validation and then deploy
            console.print(
                "[cyan]Found existing validation. Monitoring validation...[/cyan]"
            )
            validation_id = await get_latest_validation_id(vcf_client)
            await initiate_sddc_validations(
                client=vcf_client,
                vcf_json=vcf_json,
                validation_id=validation_id,
            )
            # After validation completes, proceed to deployment
            await initiate_sddc_deployment(
                client=vcf_client,
                vcf_json=vcf_json,
            )
            return

        # Found existing SDDC deployment - check status and handle accordingly
        detected_sddc_id = await get_latest_sddc_id(vcf_client)

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


async def check_latest_validation(client: VCFClient) -> bool:
    """
    Check for the latest validation task.

    Args:
        client: VCF client instance

    Returns:
        bool: True if validation exists with valid ID, False otherwise
    """
    try:
        console.print("[cyan]🔍 Checking for existing validation...[/cyan]")
        latest_validation = await client.api_call("GET", "/v1/sddcs/validations/latest")

        if latest_validation and "id" in latest_validation:
            console.print(
                f"[cyan]✓ Found existing validation with ID: {latest_validation['id']}[/cyan]"
            )
            console.print(
                f"[cyan]Validation status: {latest_validation.get('executionStatus', 'UNKNOWN')}[/cyan]"
            )
            return True
        else:
            return False
    except Exception as e:
        if global_debug:
            console.print(
                f"[yellow]⚠️ Error checking for existing validation: {e}[/yellow]"
            )
        return False


async def get_latest_validation_id(client: VCFClient) -> Optional[str]:
    """
    Get the ID of the latest validation task.

    Args:
        client: VCF client instance

    Returns:
        Optional[str]: Validation ID if found, None otherwise
    """
    try:
        latest_validation = await client.api_call("GET", "/v1/sddcs/validations/latest")
        return latest_validation.get("id") if latest_validation else None
    except Exception as e:
        if global_debug:
            console.print(f"[yellow]⚠️ Error getting validation ID: {e}[/yellow]")
        return None


async def check_latest_sddc(client: VCFClient) -> bool:
    """
    Check for the latest SDDC deployment task.

    Args:
        client: VCF client instance

    Returns:
        bool: True if SDDC deployment exists with valid ID, False otherwise
    """
    try:
        console.print("[cyan]🔍 Checking for existing SDDC deployment...[/cyan]")
        latest_sddc = await client.api_call("GET", "/v1/sddcs/latest")

        if latest_sddc and "id" in latest_sddc:
            console.print(
                f"[cyan]✓ Found existing SDDC deployment with ID: {latest_sddc['id']}[/cyan]"
            )
            console.print(
                f"[cyan]Latest deployment status: {latest_sddc.get('status', 'UNKNOWN')}[/cyan]"
            )
            return True
        else:
            return False
    except Exception as e:
        if global_debug:
            console.print(
                f"[yellow]⚠️ Error checking for existing SDDC deployment: {e}[/yellow]"
            )
        return False


async def get_latest_sddc_id(client: VCFClient) -> Optional[str]:
    """
    Get the ID of the latest SDDC deployment task.

    Args:
        client: VCF client instance

    Returns:
        Optional[str]: SDDC ID if found, None otherwise
    """
    try:
        latest_sddc = await client.api_call("GET", "/v1/sddcs/latest")
        return latest_sddc.get("id") if latest_sddc else None
    except Exception as e:
        if global_debug:
            console.print(f"[yellow]⚠️ Error getting SDDC ID: {e}[/yellow]")
        return None


async def _run_sddc_operations(client: VCFClient, vcf_json: dict, message: str):
    """
    Helper function to run SDDC validation and deployment operations.

    Args:
        client: VCF client instance
        vcf_json: VCF template JSON
        message: Message to display before starting operations
    """
    console.print(
        f"[cyan]{message}. Proceeding with validation and new deployment...[/cyan]"
    )

    # Initiate SDDC operations
    await initiate_sddc_validations(
        client=client,
        vcf_json=vcf_json,
    )
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
    console.print(f"[bold cyan]🚀 Deploying zPod: {zpod_name}[/bold cyan]")

    zpod = zpod_client.get(f"/zpods/name={zpod_name}").json()
    if zpod.get("status") == "ACTIVE":
        console.print("[bold green]✓ zPod is already active[/bold green]")
        console.print(gather_zpod_info(zpod))
        return zpod

    endpoint = zpod_client.get(f"/endpoints/name={endpoint_name}").json()

    if not endpoint:
        console.print(f"[bold red]❌ Endpoint not found: {endpoint_name}[/bold red]")
        raise typer.Exit(code=1)

    if global_debug:
        console.print(f"[dim]Creating zPod with endpoint: {endpoint}[/dim]")

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
        console.print("[bold green]✅ zPod deployment successful![/bold green]")
        return zpod
    elif status == "DEPLOY_FAILED":
        console.print("[bold red]❌ zPod Deployment Failed[/bold red]")
        if global_debug:
            console.print(Pretty(zpod))
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
    console.print("[cyan]🔍 Fetching zPodFactory host IP from settings...[/cyan]")
    response = zpod_client.get("/settings/name=zpodfactory_host")
    response.raise_for_status()
    data = response.json()
    ip = data.get("value", "")
    if not ip:
        console.print(
            "[bold red]❌ zpodfactory_host setting returned empty value[/bold red]"
        )
        raise typer.Exit(code=1)
    console.print(f"[green]✓ zPodFactory host IP: {ip}[/green]")
    return ip


def build_vcf_template(zpod, tmpl, zpodfactory_ip: str):
    """Build VCF template with zpod variables"""
    console.print("[bold cyan]📋 Building VCF template...[/bold cyan]")

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
        console.print(f"[dim]Template variables:[/dim]")
        console.print(f"[dim]  zpod_domain: {template_vars['zpod_domain']}[/dim]")
        console.print(f"[dim]  zpod_name: {template_vars['zpod_name']}[/dim]")
        console.print(f"[dim]  zpod_subnet: {template_vars['zpod_subnet']}[/dim]")
        console.print(f"[dim]  zpodfactory_ip: {template_vars['zpodfactory_ip']}[/dim]")
        console.print(
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
    console.print(f"[cyan]📄 Writing VCF template to: {filename}[/cyan]")
    with open(filename, "w") as f:
        json.dump(vcf_json, f, indent=2)



def configure_dns(
    zpod_client: httpx.Client,
    zpod_name: str,
    vcf_json: dict,
):
    """Configure DNS records for the zPod"""
    console.print("[bold cyan]🌐 Configuring DNS records...[/bold cyan]")

    # Correspondence table for hostname to IP mapping
    hostname_ip_mapping = {
        "vcsa": "10",
        "nsx": "20",
        "nsx21": "21",
        "sddcmgr": "26",
        "vcfopsfleetmgr": "27",
        "vcfops": "28",
        "vcfopscollector": "29",
        "vcfa": "30",
    }

    def find_hostnames_in_json(obj, hostnames=None):
        """Recursively find all hostnames in JSON structure that contain the domain"""
        if hostnames is None:
            hostnames = []

        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in ["hostname", "vcenterHostname", "vipFqdn"] and isinstance(
                    value, str
                ):
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

        if hostname_part in hostname_ip_mapping:
            if hostname_part == "vcfinstaller" or hostname_part.startswith("esxi"):
                return None, hostname

            ip_suffix = hostname_ip_mapping[hostname_part]
            ip_address = f"{zpod_subnet}.{ip_suffix}"
            return ip_address, hostname_part

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
            console.print(f"  [dim]DNS data: {dns_data}[/dim]")

        post_response = zpod_client.post(
            f"/zpods/name={zpod_name}/dns",
            json=dns_data,
        )
        if post_response.status_code in [200, 201]:
            return "created"

        if global_debug:
            console.print(f"  [dim]Status: {post_response.status_code}[/dim]")
            try:
                console.print(f"  [dim]Error: {post_response.json()}[/dim]")
            except json.JSONDecodeError:
                console.print(f"  [dim]Error: {post_response.text}[/dim]")
        return "failed"

    if global_debug:
        console.print(f"  [dim]Unexpected status: {response.status_code}[/dim]")
        try:
            console.print(f"  [dim]Error: {response.json()}[/dim]")
        except json.JSONDecodeError:
            console.print(f"  [dim]Error: {response.text}[/dim]")
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
    console.print(
        "[bold cyan]⚙️ Configuring depot settings for online mode...[/bold cyan]"
    )

    depot_data = {"vmwareAccount": {"downloadToken": download_token}}

    console.print(
        f"[bold green]✓ Configuring depot for online mode:[/bold green]\n"
        f"  [cyan]Download Token:[/cyan] {download_token[:20]}...{download_token[-4:] if len(download_token) > 24 else ''}"
    )

    try:
        await client.api_call("PUT", "/v1/system/settings/depot", depot_data)
        console.print(
            "[bold green]✓ Depot settings updated for online mode[/bold green]"
        )
    except Exception as e:
        console.print(
            f"[bold red]❌ Failed to configure depot settings: {e}[/bold red]"
        )
        if global_debug:
            console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
            console.print(
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
    console.print(
        "[bold cyan]⚙️ Configuring depot settings for offline mode...[/bold cyan]"
    )

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

    # Obfuscate password for display
    obfuscated_password = "*" * len(password) if password else ""
    console.print(
        f"[bold green]✓ Configuring depot for offline mode:[/bold green]\n"
        f"  [cyan]Hostname:[/cyan] {hostname}\n"
        f"  [cyan]Username:[/cyan] {username}\n"
        f"  [cyan]Password:[/cyan] {obfuscated_password}\n"
        f"  [cyan]Port:[/cyan] {port}"
    )

    try:
        await client.api_call("PUT", "/v1/system/settings/depot", depot_data)
        console.print(
            "[bold green]✓ Depot settings updated for offline mode[/bold green]"
        )
    except Exception as e:
        console.print(
            f"[bold red]❌ Failed to configure depot settings: {e}[/bold red]"
        )
        if global_debug:
            console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
            console.print(
                f"[red]Request data: {json.dumps(depot_data, indent=2)}[/red]"
            )
        raise


async def handle_depot_sync(client: VCFClient):
    """Handle depot sync operations"""
    console.print("[bold cyan]🔄 Checking depot sync status...[/bold cyan]")

    try:
        sync_info = await client.api_call(
            "GET", "/v1/system/settings/depot/depot-sync-info"
        )
        sync_status = sync_info.get("syncStatus", "UNKNOWN")
        console.print(f"[bold green]✓ Depot sync status: {sync_status}[/bold green]")

        if sync_status == "UNSYNCED":
            console.print(
                "[bold yellow]⚠️ Depot is not in sync. Triggering depot sync...[/bold yellow]"
            )
            await trigger_depot_sync(client)
            await wait_for_depot_sync(client)
        elif sync_status == "SYNCED":
            console.print("[bold green]✅ Depot is already in sync![/bold green]")
        else:
            console.print(f"[yellow]⚠️ Depot sync status: {sync_status}[/yellow]")

    except Exception as e:
        console.print(f"[bold red]❌ Failed to check depot sync info: {e}[/bold red]")
        if global_debug:
            console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
        raise


async def trigger_depot_sync(client: VCFClient):
    """Trigger depot sync"""
    try:
        await client.api_call("PATCH", "/v1/system/settings/depot/depot-sync-info")
        console.print("[bold green]✅ Depot sync triggered successfully![/bold green]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Could not trigger depot sync: {e}[/yellow]")
        if global_debug:
            console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
        console.print("[cyan]Continuing to monitor sync status...[/cyan]")


async def wait_for_depot_sync(client: VCFClient):
    """Wait for depot to sync with status monitoring"""
    console.print("\n[bold cyan]⏳ Waiting for depot to sync...[/bold cyan]")
    console.print("[dim]This may take several seconds. Please wait...[/dim]")
    console.print("[yellow]Press Ctrl+C to interrupt at any time.[/yellow]")

    while True:
        try:
            sync_info = await client.api_call(
                "GET",
                "/v1/system/settings/depot/depot-sync-info",
            )
            sync_status = sync_info.get("syncStatus", "UNKNOWN")

            if sync_status == "SYNCED":
                console.print("[bold green]✅ Depot is now in sync![/bold green]")
                break
            elif sync_status == "SYNCING":
                console.print("[cyan]🔄 Depot is syncing...[/cyan]")
            elif sync_status == "SYNC_FAILED":
                console.print("[bold red]❌ Depot sync failed![/bold red]")
                console.print(
                    "[yellow]Please check your depot configuration and try again.[/yellow]"
                )
                if global_debug:
                    console.print(
                        f"[red]Sync info: {json.dumps(sync_info, indent=2)}[/red]"
                    )
                return
            else:
                console.print(f"[yellow]⏳ Depot status: {sync_status}[/yellow]")

            await asyncio.sleep(10)

        except Exception as e:
            console.print(f"[red]Error checking depot sync status: {e}[/red]")
            if global_debug:
                console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
            await asyncio.sleep(10)


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
    # Single mapping from VCF JSON spec keys to API component names.
    # A spec key can map to multiple components.
    spec_to_components = {
        "hostSpecs": ["ESXI"],
        "vcenterSpec": ["VCENTER"],
        "sddcManagerSpec": ["SDDC_MANAGER"],
        "nsxtSpec": ["NSX_T_MANAGER"],
        "vcfOperationsSpec": ["VROPS", "VCF_OPS_CLOUD_PROXY"],
        "vcfOperationsFleetManagementSpec": ["VRSLCM"],
        "vcfOperationsCollectorSpec": ["VROPS"],
        # FIXME: VRA and VRLI spec key names are unknown, update once confirmed
        "UNKNOWN_VRA_SPEC": ["VRA"],
        "UNKNOWN_VRLI_SPEC": ["VRLI"],
    }

    required = set()
    for spec_key, component_names in spec_to_components.items():
        if spec_key in vcf_json:
            required.update(component_names)

    return required


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

        # Parse components
        vcf_components = parse_vcf_components(release_components_response)

        if not vcf_components:
            console.print("[bold red]❌ No VCF components found. Exiting.[/bold red]")
            return

        # Filter to only required components based on config template
        if vcf_json:
            required_components = get_required_components(vcf_json)
            all_component_names = set(vcf_components.keys())
            skipped = all_component_names - required_components
            vcf_components = {
                name: data
                for name, data in vcf_components.items()
                if name in required_components
            }
            if skipped:
                console.print(
                    f"[dim]Skipping bundles not needed by config template: "
                    f"{', '.join(sorted(skipped))}[/dim]"
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
            download_results = await initiate_bundle_downloads(
                client, bundles_to_download, bundle_status_map
            )

            # Show live monitoring if any bundles are not in SUCCESS status
            if not all_success:
                console.print(
                    "[yellow]Press Ctrl+C to interrupt monitoring at any time.[/yellow]"
                )
                await monitor_download_progress(
                    client, vcf_components, download_results, vcf_version
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
            console.print(f"[red]Error details: {type(e).__name__}: {str(e)}[/red]")
        raise


def parse_vcf_components(release_components_response: Dict[str, Any]) -> Dict[str, Any]:
    """Parse VCF components from API response"""
    vcf_components = {}

    if (
        isinstance(release_components_response, dict)
        and "elements" in release_components_response
    ):
        elements = release_components_response["elements"]

        if elements and len(elements) > 0:
            target_release = elements[0]
            if "components" in target_release:
                components = target_release["components"]
                console.print(
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

                    if "versions" in component and isinstance(
                        component["versions"], list
                    ):
                        for version in component["versions"]:
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
    all_bundles = []
    for comp_name, comp_data in vcf_components.items():
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
            if download_status not in ("DOWNLOADING"):
                bundles_to_download.append((comp_name, bundle))

    return all_success, bundles_to_download


async def initiate_bundle_downloads(
    client: VCFClient,
    bundles_to_download: List[Tuple[str, Dict[str, Any]]],
    bundle_status_map: Dict[str, Any],
) -> Dict[str, Any]:
    """Initiate downloads for bundles with PENDING status"""
    console.print(
        "\n[bold cyan]🚀 Checking bundle status and initiating downloads for PENDING bundles...[/bold cyan]"
    )
    download_results = {}
    successful_initiations = 0

    for comp_name, bundle in bundles_to_download:
        bundle_id = bundle["id"]
        bundle_status = bundle_status_map.get(bundle_id, {})
        download_status = bundle_status.get("downloadStatus", "UNKNOWN")

        if download_status == "PENDING":
            console.print(
                f"  [cyan]{comp_name}:[/cyan] {bundle_id[:40]}... [yellow]initiating download (PENDING)[/yellow]"
            )

            try:
                download_payload = {"bundleDownloadSpec": {"downloadNow": True}}
                result = await client.api_call(
                    "PATCH",
                    f"/v1/bundles/{bundle_id}",
                    download_payload,
                )
                if comp_name not in download_results:
                    download_results[comp_name] = []
                download_results[comp_name].append(
                    {
                        "bundle_id": bundle_id,
                        "status": "initiated",
                        "result": result,
                    }
                )
                successful_initiations += 1
                console.print("    [bold green]✓ Download initiated[/bold green]")
            except Exception as e:
                if comp_name not in download_results:
                    download_results[comp_name] = []
                download_results[comp_name].append(
                    {
                        "bundle_id": bundle_id,
                        "status": "error",
                        "error": str(e),
                    }
                )
                console.print(f"    [bold red]✗ Failed: {str(e)[:100]}...[/bold red]")
        else:
            console.print(
                f"  [cyan]{comp_name}:[/cyan] {bundle_id[:40]}... [dim]skipping (status: {download_status})[/dim]"
            )

    total_attempts = len(
        [
            b
            for _, b in bundles_to_download
            if bundle_status_map.get(b["id"], {}).get("downloadStatus") == "PENDING"
        ]
    )

    console.print(
        f"\n[bold cyan]📊 Download Summary:[/bold cyan] {successful_initiations}/{total_attempts} "
        "scheduled for download"
    )

    return download_results


def generate_status_table(
    vcf_components: Dict[str, Any], status_data: Dict[str, Any]
) -> Table:
    """Generate status table with current download information"""
    table = Table(title="VCF Installer - Bundle Depot Download Status")
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Product Name", style="green")
    table.add_column("Bundle ID", style="magenta", no_wrap=True)
    table.add_column("Size", style="blue", justify="right")
    table.add_column("Status", style="yellow", width=14)

    for comp_name, comp_data in vcf_components.items():
        public_name = comp_data.get("publicName", "")
        for bundle in comp_data["bundles"]:
            bundle_id = bundle["id"]
            bundle_status_info = status_data.get(bundle_id, {})
            download_status = bundle_status_info.get("downloadStatus", "UNKNOWN")
            downloaded_size = bundle_status_info.get("downloadedSize", 0)
            total_size = bundle["size"]

            if downloaded_size > 0 and download_status == "INPROGRESS":
                if total_size >= 0:
                    percentage = min(100, int((downloaded_size / total_size) * 100))
                    filled_length = int((percentage / 100) * 10)
                    progress_bar = "█" * filled_length + "░" * (10 - filled_length)
                    status_display = f"[blue]{progress_bar} {percentage}%[/blue]"
            elif download_status == "SUCCESS":
                status_display = "[green]SUCCESS[/green]"
            elif download_status == "FAILED":
                status_display = "[red]FAILED[/red]"
            elif download_status == "PENDING":
                status_display = "[grey]NOT DOWNLOADED[/grey]"
            elif download_status == "SCHEDULED":
                status_display = "[blue]SCHEDULED[/blue]"
            elif download_status == "VALIDATING":
                status_display = "[blue]VALIDATING...[/blue]"
            else:
                status_display = f"[blue]{download_status}[/blue]"

            table.add_row(
                comp_name,
                public_name,
                bundle_id,
                format_size(total_size),
                status_display,
            )

    return table


async def monitor_download_progress(
    client: VCFClient,
    vcf_components: Dict[str, Any],
    download_results: Dict[str, Any],
    version: str,
) -> None:
    """Monitor download progress for all bundles using live updating table"""
    global monitoring_active
    monitoring_active = True
    live = None
    try:
        live = Live(refresh_per_second=1, transient=False)
        live.start()

        while True:
            try:
                status_response = await client.api_call(
                    "GET",
                    f"/v1/bundles/download-status?imageType=INSTALL&releaseVersion={version}",
                )

                if isinstance(status_response, dict) and "elements" in status_response:
                    elements = status_response["elements"]
                    status_data = {}
                    for element in elements:
                        bundle_id = element.get("bundleId")
                        if bundle_id:
                            status_data[bundle_id] = element

                    table = generate_status_table(vcf_components, status_data)
                    live.update(table)

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
        if live and not live.is_started:
            try:
                live.stop()
            except Exception:
                pass
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

    if validation_id:
        console.print(
            f"[bold cyan]🔍 Monitoring existing SDDC validation with ID: {validation_id}[/bold cyan]"
        )
        sddc_val_id = validation_id
    else:
        console.print("[bold cyan]🔍 Initiating SDDC validations...[/bold cyan]")

        # Create new validation
        if global_debug:
            console.print(f"[dim]Sending validation request with JSON payload:[/dim]")
            console.print("-" * 80)
            console.print(json.dumps(vcf_json, indent=2))
            console.print("-" * 80)

        try:
            response = await client.api_call("POST", "/v1/sddcs/validations", vcf_json)
            sddc_val_id = response["id"]
        except Exception as e:
            if "403" in str(e):
                console.print("[red]Validation in progress, try again later.[/red]")
                raise typer.Exit(code=1)
            raise

    # Monitor validation progress with live display
    monitoring_active = True
    live = None

    try:
        with Live(
            generate_validation_status_display(),
            refresh_per_second=2,
            console=console,
        ) as live:
            while True:
                try:
                    sddc_val = await client.api_call(
                        "GET", f"/v1/sddcs/validations/{sddc_val_id}"
                    )

                    # Update the live display
                    live.update(generate_validation_status_display(sddc_val))

                    executionStatus = sddc_val["executionStatus"]

                    if executionStatus != "IN_PROGRESS":
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
        if live and not live.is_started:
            try:
                live.stop()
            except Exception:
                pass
        monitoring_active = False

    # Check final status
    if executionStatus == "COMPLETED":
        console.print(
            "\n[bold green]✅ SDDC validation completed successfully![/bold green]"
        )
        return
    elif executionStatus == "FAILED":
        console.print("\n[bold red]❌ SDDC validation failed[/bold red]")
        if global_debug:
            console.print(Pretty(sddc_val))
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
    global monitoring_active

    # Create new deployment if no sddc_id provided
    if not sddc_id:
        console.print("[bold cyan]🚀 Initiating SDDC deployment...[/bold cyan]")
        console.print("[cyan]Creating new SDDC deployment...[/cyan]")
        sddc = await client.api_call("POST", "/v1/sddcs", vcf_json)
        sddc_id = sddc["id"]
        console.print(f"[cyan]✓ New SDDC deployment created with ID: {sddc_id}[/cyan]")
    else:
        console.print(
            f"[cyan]Monitoring existing SDDC deployment with ID: {sddc_id}[/cyan]"
        )

    # Monitor deployment progress with live display
    monitoring_active = True
    live = None

    try:
        with Live(
            generate_deployment_status_display(),
            refresh_per_second=2,
            console=console,
        ) as live:
            while True:
                try:
                    sddc = await client.api_call("GET", f"/v1/sddcs/{sddc_id}")

                    # Update the live display
                    live.update(generate_deployment_status_display(sddc))

                    # Check if sddcSubTasks exists
                    if "sddcSubTasks" not in sddc:
                        await asyncio.sleep(5)
                        continue

                    sddc_status = sddc["status"]
                    if sddc_status != "IN_PROGRESS":
                        break

                    await asyncio.sleep(5)

                except Exception as e:
                    console.print(f"[red]Error checking deployment status: {e}[/red]")
                    await asyncio.sleep(5)
                    continue

    except KeyboardInterrupt:
        if live:
            live.stop()
        console.print(
            "\n\n⚠️ [yellow]Deployment monitoring interrupted by user (Ctrl+C)[/yellow]"
        )
        console.print("🔄 [blue]Stopping deployment monitoring...[/blue]")
    except Exception as e:
        if live:
            live.stop()
        console.print(f"\n❌ [red]Error in deployment monitoring: {e}[/red]")
    finally:
        if live and not live.is_started:
            try:
                live.stop()
            except Exception:
                pass
        monitoring_active = False

    # Check final status
    if sddc_status == "COMPLETED_WITH_SUCCESS":
        console.print(
            "\n[bold green]✅ SDDC deployment completed successfully![/bold green]"
        )
        return
    elif sddc_status == "FAILED":
        console.print("\n[bold red]❌ SDDC deployment failed[/bold red]")
        if global_debug:
            console.print(Pretty(sddc))
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
        return Text("🔍 Waiting for validation data...", style="dim italic")

    # Extract main status information
    validation_id = validation_data.get("id", "Unknown")
    description = "VCF Spec Validation Status"
    execution_status = validation_data.get("executionStatus", "UNKNOWN")
    result_status = validation_data.get("resultStatus", "UNKNOWN")
    validation_checks = validation_data.get("validationChecks", [])

    # Create the main status line
    if execution_status == "IN_PROGRESS":
        status_color = "blue"
        status_icon = "🔄"
    elif execution_status == "COMPLETED":
        status_color = "green"
        status_icon = "✅"
    elif execution_status == "FAILED":
        status_color = "red"
        status_icon = "❌"
    else:
        status_color = "yellow"
        status_icon = "⚠️"

    # Build the main status text
    main_status = Text()
    main_status.append(f"Validation ID: {validation_id}\n", style="bold cyan")
    main_status.append(f"{description}: ", style="bold white")
    main_status.append(f"{execution_status}", style=f"bold {status_color}")
    main_status.append(" ")
    main_status.append(status_icon)

    # Add result status if different from execution status
    if result_status != "UNKNOWN" and result_status != execution_status:
        main_status.append(f" (Result: {result_status})", style="dim")

    # If no validation checks, just return the main status
    if not validation_checks:
        return main_status

    # Find the longest description for alignment
    max_description_length = max(
        len(check.get("description", "")) for check in validation_checks
    )

    # Build the validation checks list with aligned status
    checks_text = Text()
    for i, check in enumerate(validation_checks):
        check_description = check.get("description", "Unknown Check")
        check_status = check.get("resultStatus", "UNKNOWN")

        # Determine color and icon for each check
        if check_status == "SUCCEEDED":
            check_color = "green"
            check_icon = "✓"
        elif check_status == "IN_PROGRESS":
            check_color = "blue"
            check_icon = "🔄"
        elif check_status == "FAILED":
            check_color = "red"
            check_icon = "✗"
        else:
            check_color = "yellow"
            check_icon = "?"

        # Add check line with aligned status
        checks_text.append(f"  • {check_description}", style="white")

        # Pad with spaces to align the status column
        padding_length = max_description_length - len(check_description) + 2
        checks_text.append(" " * padding_length, style="white")

        checks_text.append(f"{check_status}", style=f"bold {check_color}")
        checks_text.append(" ")
        checks_text.append(check_icon)

        # Add error information if available and status is not SUCCEEDED
        if check_status != "SUCCEEDED" and check.get("errorResponse"):
            error_response = check["errorResponse"]
            nested_errors = error_response.get("nestedErrors", [])
            if nested_errors:
                checks_text.append(f" (", style="dim")
                checks_text.append(
                    f"{len(nested_errors)} error(s) {nested_errors}",
                    style="red",
                )
                checks_text.append(f")", style="dim")

        if i < len(validation_checks) - 1:
            checks_text.append("\n")

    # Combine main status and checks
    full_text = Text()
    full_text.append(main_status)
    full_text.append("\n")
    full_text.append(checks_text)

    return full_text


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

    # Create the main status line
    if status == "IN_PROGRESS":
        status_color = "blue"
        status_icon = "▶"
    elif status == "COMPLETED_WITH_SUCCESS":
        status_color = "green"
        status_icon = "✅"
    elif status == "FAILED":
        status_color = "red"
        status_icon = "❌"
    else:
        status_color = "yellow"
        status_icon = "⚠️"

    # Build the main status text (status text only)
    main_status = Text()
    main_status.append(f"{name}: ", style="bold white")
    main_status.append(f"{status}", style=f"bold {status_color}")

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
            milestone_subtasks = get_sorted_milestone_subtasks(all_subtasks, n=10)

            for subtask in milestone_subtasks:
                subtask_name = subtask.get("name", "Unknown Subtask")
                all_task_names.append(f"    TASK: {subtask_name}")

    # Find the longest task name for alignment
    max_task_name_length = (
        max(len(name) for name in all_task_names) if all_task_names else 0
    )

    # Second pass: build the actual display with alignment
    for i, milestone in enumerate(milestones):
        milestone_name = milestone.get("name", "Unknown Milestone")
        milestone_status = milestone.get("status", "UNKNOWN")

        # Determine color and icon for milestone status
        if milestone_status == "COMPLETED_WITH_SUCCESS":
            milestone_color = "green"
            milestone_icon = "✓"
        elif milestone_status == "IN_PROGRESS":
            milestone_color = "blue"
            milestone_icon = "▶"
        elif milestone_status == "FAILED":
            milestone_color = "red"
            milestone_icon = "✗"
        else:
            milestone_color = "yellow"
            milestone_icon = "?"

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

            # If milestone is in progress, show its subtasks
        if milestone_status == "IN_PROGRESS":
            # Get all subtasks (including INITIALIZED)
            all_subtasks = list(sddc_subtasks)
            milestone_subtasks = get_sorted_milestone_subtasks(all_subtasks, n=10)

            for subtask in milestone_subtasks:
                subtask_name = subtask.get("name", "Unknown Subtask")
                subtask_status = subtask.get("status", "UNKNOWN")

                # Determine color and icon for subtask status
                if subtask_status == "POSTVALIDATION_COMPLETED_WITH_SUCCESS":
                    subtask_color = "green"
                    subtask_icon = "✓"
                elif "IN_PROGRESS" in subtask_status:
                    subtask_color = "blue"
                    subtask_icon = "▶"
                elif "FAILED" in subtask_status:
                    subtask_color = "red"
                    subtask_icon = "✗"
                else:
                    subtask_color = "yellow"
                    subtask_icon = "?"

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


def get_sorted_milestone_subtasks(all_subtasks, n=10):
    # Partition subtasks by whether they have a timestamp
    with_ts = [s for s in all_subtasks if s.get("updateTimestamp")]
    no_ts = [s for s in all_subtasks if not s.get("updateTimestamp")]

    # If all have no timestamp, put IN_PROGRESS first, then the rest
    if not with_ts:
        in_progress = [s for s in no_ts if s.get("status", "") == "IN_PROGRESS"]
        others = [s for s in no_ts if s not in in_progress]
        sorted_subtasks = in_progress + others
        return sorted_subtasks[:n]
    else:
        # Sort with timestamps by timestamp
        with_ts_sorted = sorted(with_ts, key=lambda s: s.get("updateTimestamp"))
        # Keep no-timestamp tasks in their original order after the completed ones
        sorted_subtasks = with_ts_sorted + no_ts
        last_n = sorted_subtasks[-n:]
        # Ensure the IN_PROGRESS task is always present in the last N
        in_progress = next(
            (s for s in all_subtasks if s.get("status", "") == "IN_PROGRESS"), None
        )
        if in_progress and in_progress not in last_n:
            # Replace the first item with the IN_PROGRESS task
            last_n = [in_progress] + last_n[1:]
        # Remove duplicates while preserving order
        seen = set()
        unique_last_n = []
        for s in last_n:
            sid = id(s)
            if sid not in seen:
                unique_last_n.append(s)
                seen.add(sid)
        return unique_last_n


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
