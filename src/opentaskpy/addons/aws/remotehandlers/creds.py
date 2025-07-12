"""AWS helper functions."""

import os
import socket
from time import time

import boto3
import opentaskpy.otflogging
from botocore.args import ClientArgsCreator
from botocore.config import Config

logger = opentaskpy.otflogging.init_logging(__name__, None, None)


def _custom_compute_socket_options(self, scoped_config, client_config=None):  # type: ignore[no-untyped-def]
    # This is a workaround for an issue in botocore - See the following PR for more details:
    # https://github.com/boto/botocore/pull/3140
    # Once this is merged, we can remove this monkey patch

    # This forces a more aggressive keepalive, as the default is too low for default
    # AWS NAT gateways where the idle timeout is set to 350 seconds

    socket_options = [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
    client_keepalive = client_config and client_config.tcp_keepalive
    scoped_keepalive = (
        scoped_config
        and self._ensure_boolean(  # pylint: disable=protected-access
            scoped_config.get("tcp_keepalive", False)
        )
    )
    # Enables TCP Keepalive if specified in client config object or shared config file.
    if not client_keepalive and not scoped_keepalive:
        socket_options.append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
        return socket_options

    seconds_in_a_minute = 60

    client_read_timeout = client_config and client_config.read_timeout
    scoped_read_timeout = scoped_config.get("read_timeout", None)

    read_timeout = (
        scoped_read_timeout if scoped_read_timeout else client_read_timeout
    ) or seconds_in_a_minute

    maximum_keepalive_probes = int(read_timeout / seconds_in_a_minute) or 1
    keep_idle = (
        seconds_in_a_minute if read_timeout > seconds_in_a_minute else read_timeout
    )

    socket_options.extend(
        [
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, keep_idle),
            (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, keep_idle),
            (
                socket.IPPROTO_TCP,
                socket.TCP_KEEPCNT,
                maximum_keepalive_probes,
            ),
        ],
    )
    return socket_options


def get_aws_client(
    client_type: str,
    credentials: dict,
    token_expiry_seconds: int | None = 900,
    assume_role_arn: str | None = None,
    config: Config | None = None,
) -> dict:
    """Get an AWS client of the specified type using the provided credentials.

    Args:
        client_type: The type of client to get
        credentials: The credentials to use
        token_expiry_seconds: The expiry time for the token (optional, defaults to 900)
        assume_role_arn: The role to assume, if using assumed role credentials (optional)
        config: The config to use for the client (optional)
    """
    if client_type == "lambda":
        # Monkey patch the socket options for lambda
        # See above for more details
        ClientArgsCreator._compute_socket_options = (  # pylint: disable=protected-access
            _custom_compute_socket_options
        )

    supported_types = ["s3", "ecs", "lambda", "logs"]
    if client_type not in supported_types:
        raise ValueError(
            f"Unsupported client type: {client_type}. Supported types are: {supported_types}"
        )

    kwargs = {}
    if os.environ.get("AWS_ENDPOINT_URL"):
        kwargs["endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")

    if assume_role_arn:
        logger.info(f"Assuming role: {assume_role_arn}")
        sts_client = boto3.client("sts", **kwargs)

        assumed_role_object = sts_client.assume_role(
            RoleArn=assume_role_arn,
            RoleSessionName=f"OTF{time()}",
            DurationSeconds=token_expiry_seconds,
        )

        credentials = assumed_role_object["Credentials"]
        # Log the assumed role access key id
        logger.info(f"Assumed role access key id: {credentials['AccessKeyId']}")

    kwargs2 = {
        "aws_access_key_id": credentials["AccessKeyId"],
        "aws_secret_access_key": credentials["SecretAccessKey"],
    }
    if "SessionToken" in credentials:
        kwargs2["aws_session_token"] = credentials["SessionToken"]

    if "region_name" in credentials:
        kwargs2["region_name"] = credentials["region_name"]

    session = boto3.session.Session(**kwargs2)

    return {
        "client": session.client(client_type, **kwargs, config=config),
        "temporary_creds": credentials if assume_role_arn else None,
    }


def set_aws_creds(obj) -> None:  # type: ignore[no-untyped-def]
    """Set AWS credentials for boto3.

    Args:
        obj: The object to set the credentials on

    """
    obj.aws_access_key_id = (
        obj.spec["protocol"]["access_key_id"]
        if "access_key_id" in obj.spec["protocol"]
        else os.environ.get("AWS_ACCESS_KEY_ID")
    )
    obj.aws_secret_access_key = (
        obj.spec["protocol"]["secret_access_key"]
        if "secret_access_key" in obj.spec["protocol"]
        else os.environ.get("AWS_SECRET_ACCESS_KEY")
    )

    obj.assume_role_arn = (
        obj.spec["protocol"]["assume_role_arn"]
        if "assume_role_arn" in obj.spec["protocol"]
        else os.environ.get("AWS_ROLE_ARN")
    )

    obj.token_expiry_seconds = obj.spec["protocol"].get("token_expiry_seconds", 900)

    obj.region_name = (
        obj.spec["protocol"]["region_name"]
        if "region_name" in obj.spec["protocol"]
        else os.environ.get("AWS_REGION")
    )

    obj.bucket_owner_full_control = obj.spec["protocol"].get(
        "bucket_owner_full_control", True
    )
