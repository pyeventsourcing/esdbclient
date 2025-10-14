from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, cast

from google.protobuf import any_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import Descriptor
from google.protobuf.descriptor_pool import DescriptorPool
from google.rpc import status_pb2

import kurrentdbclient.protos.kurrent.rpc.errors_pb2 as kurrent_rpc_errors_pb2

# from kurrentdbclient.protos.v1 import status_pb2


if TYPE_CHECKING:
    from collections.abc import Sequence

    from google.protobuf.message import Message


default_descriptor_pool = cast(Callable[..., DescriptorPool], descriptor_pool.Default)()


@dataclass
class UnpackedStatus:
    status: status_pb2.Status
    details: Sequence[Any]


def parse_status_from_string(data: bytes | str) -> status_pb2.Status:
    # Parse the Status
    status = status_pb2.Status()
    status.ParseFromString(data)
    return status


def unpack_status_details(status: status_pb2.Status) -> Sequence[Message]:
    """
    Parse a google.rpc.Status message and try to unpack each Any detail
    using known descriptors in the default pool.
    """

    unpacked_details: list[Any] = []
    if isinstance(status.details, any_pb2.Any):
        unpacked = unpack_any(status.details)
        unpacked_details.append(unpacked)
    else:
        for any_msg in status.details:
            unpacked = unpack_any(any_msg)
            unpacked_details.append(unpacked)

    return unpacked_details


def unpack_any(any_msg: any_pb2.Any) -> Message:
    type_url = any_msg.type_url

    # Extract the type name (e.g. 'google.rpc.ErrorInfo')
    type_name = type_url.split("/")[-1] if "/" in type_url else type_url

    try:
        descriptor = cast(
            Callable[[str], Descriptor], default_descriptor_pool.FindMessageTypeByName
        )(type_name)
        message_class = message_factory.GetMessageClass(descriptor)
        msg = message_class()
        any_msg.Unpack(msg)
        unpacked = msg
    except KeyError as _e:  # pragma: no cover
        # Unknown type
        unpacked = any_msg
    return unpacked


def pack_any(msg: Message) -> any_pb2.Any:
    packed = any_pb2.Any()
    packed.Pack(msg=msg)
    return packed


def extract_current_leader_from_status_string(
    status_string: str | bytes,
) -> kurrent_rpc_errors_pb2.NotLeaderNodeErrorDetails.NodeInfo | None:
    status = parse_status_from_string(status_string)
    for message in unpack_status_details(status):
        if isinstance(message, kurrent_rpc_errors_pb2.NotLeaderNodeErrorDetails):
            return message.current_leader
    return None
