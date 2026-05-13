from unittest import TestCase

import grpc
from google.rpc import error_details_pb2, status_pb2

from kurrentdbclient.protos.kurrent.rpc.errors_pb2 import NotLeaderNodeErrorDetails
from kurrentdbclient.protos.v2.streams.errors_pb2 import (
    AppendConsistencyViolationErrorDetails,
    AppendRecordSizeExceededErrorDetails,
    AppendTransactionSizeExceededErrorDetails,
    ConsistencyViolation,
    StreamAlreadyInAppendSessionErrorDetails,
    StreamRevisionConflictErrorDetails,
    StreamTombstonedErrorDetails,
)
from kurrentdbclient.unpack_error_status import (
    extract_current_leader_from_status_string,
    pack_any,
)

# Copied from an actual server response.
EXAMPLE_GRPC_STATUS_WITH_NODE_NOT_LEADER_ERROR_DETAILS_V2_BIN = (
    b"\x08\t\x12\x8d\x01The server is not the leader node and cannot handle"
    b" the request. Please retry your request against the leader node"
    b" directly at 127.0.0.1:2111\x1aE\n(type.googleapis.com/google.rpc."
    b"ErrorInfo\x12\x19\n\x0fNOT_LEADER_NODE\x12\x06server\x1as\n9type."
    b"googleapis.com/kurrent.rpc.NotLeaderNodeErrorDetails\x126\n4\n\t"
    b"127.0.0.1\x10\xbf\x10\x1a$9091e4a1-259d-4ba5-8439-69696ce67a27"
)


# Crafted to match EXAMPLE_GRPC_STATUS_WITH_NODE_NOT_LEADER_ERROR_DETAILS_V2_BIN.
def status_with_not_leader_node_error_details_v2(
    message: str,
    host: str,
    port: int,
    node_id: str,
) -> status_pb2.Status:
    return status_pb2.Status(
        code=grpc.StatusCode.FAILED_PRECONDITION.value[0],
        message=message,
        details=[
            pack_any(
                error_details_pb2.ErrorInfo(
                    reason="NOT_LEADER_NODE",
                    domain="server",
                )
            ),
            pack_any(
                NotLeaderNodeErrorDetails(
                    current_leader=NotLeaderNodeErrorDetails.NodeInfo(
                        host=host,
                        port=port,
                        node_id=node_id,
                    )
                ),
            ),
        ],
    )


def status_with_stream_revision_conflict_error_details_v2(
    message: str,
    stream: str,
    expected_revision: int,
    actual_revision: int,
) -> status_pb2.Status:
    return status_pb2.Status(
        code=grpc.StatusCode.FAILED_PRECONDITION.value[0],
        message=message,
        details=[
            pack_any(
                error_details_pb2.ErrorInfo(
                    reason="STREAM_REVISION_CONFLICT",
                    domain="streams",
                )
            ),
            pack_any(
                StreamRevisionConflictErrorDetails(
                    stream=stream,
                    expected_revision=expected_revision,
                    actual_revision=actual_revision,
                ),
            ),
        ],
    )


def status_with_stream_already_in_append_session_error_details_v2(
    message: str,
    stream: str,
) -> status_pb2.Status:
    return status_pb2.Status(
        code=grpc.StatusCode.ABORTED.value[0],
        message=message,
        details=[
            pack_any(
                error_details_pb2.ErrorInfo(
                    reason="STREAM_ALREADY_IN_APPEND_SESSION",
                    domain="streams",
                )
            ),
            pack_any(
                StreamAlreadyInAppendSessionErrorDetails(
                    stream=stream,
                ),
            ),
        ],
    )


def status_with_stream_tombstoned_error_details_v2(
    message: str,
    stream: str,
) -> status_pb2.Status:
    return status_pb2.Status(
        code=grpc.StatusCode.FAILED_PRECONDITION.value[0],
        message=message,
        details=[
            pack_any(
                error_details_pb2.ErrorInfo(
                    reason="STREAM_TOMBSTONED",
                    domain="streams",
                )
            ),
            pack_any(
                StreamTombstonedErrorDetails(
                    stream=stream,
                ),
            ),
        ],
    )


def status_with_append_record_size_exceeded_error_details_v2(
    message: str,
    stream: str,
    record_id: str,
    size: int,
    max_size: int,
) -> status_pb2.Status:
    return status_pb2.Status(
        code=grpc.StatusCode.INVALID_ARGUMENT.value[0],
        message=message,
        details=[
            pack_any(
                error_details_pb2.ErrorInfo(
                    reason="APPEND_RECORD_SIZE_EXCEEDED",
                    domain="streams",
                )
            ),
            pack_any(
                AppendRecordSizeExceededErrorDetails(
                    stream=stream,
                    record_id=record_id,
                    size=size,
                    max_size=max_size,
                ),
            ),
        ],
    )


def status_with_append_transaction_size_exceeded_error_details_v2(
    message: str,
    size: int,
    max_size: int,
) -> status_pb2.Status:
    return status_pb2.Status(
        code=grpc.StatusCode.ABORTED.value[0],
        message=message,
        details=[
            pack_any(
                error_details_pb2.ErrorInfo(
                    reason="APPEND_TRANSACTION_SIZE_EXCEEDED",
                    domain="streams",
                )
            ),
            pack_any(
                AppendTransactionSizeExceededErrorDetails(
                    size=size,
                    max_size=max_size,
                ),
            ),
        ],
    )


def status_with_some_unsupported_error_details_v2(message: str) -> status_pb2.Status:
    return status_pb2.Status(
        code=grpc.StatusCode.OK.value[0],
        message=message,
        details=[
            pack_any(
                error_details_pb2.ErrorInfo(
                    reason="ACTUALLY_OK",
                    domain="server",
                )
            ),
        ],
    )


def status_with_append_consistency_violation_error_details_v2(
    message: str,
    stream: str,
) -> status_pb2.Status:
    return status_pb2.Status(
        code=grpc.StatusCode.FAILED_PRECONDITION.value[0],
        message=message,
        details=[
            pack_any(
                error_details_pb2.ErrorInfo(
                    reason="APPEND_CONSISTENCY_VIOLATION",
                    domain="streams",
                )
            ),
            pack_any(
                AppendConsistencyViolationErrorDetails(
                    violations=[
                        ConsistencyViolation(
                            check_index=0,
                            stream_state=ConsistencyViolation.StreamStateViolation(
                                stream=stream,
                                expected_state=-4,
                                actual_state=-1,
                            ),
                        )
                    ],
                ),
            ),
        ],
    )


class TestUnpackErrorStatus(TestCase):
    def test_pack_status_with_node_not_leader_error_details(self) -> None:
        status = status_with_not_leader_node_error_details_v2(
            message=(
                "The server is not the leader node and cannot handle"
                " the request. Please retry your request against the"
                " leader node directly at 127.0.0.1:2111"
            ),
            host="127.0.0.1",
            port=2111,
            node_id="9091e4a1-259d-4ba5-8439-69696ce67a27",
        )
        self.assertEqual(
            EXAMPLE_GRPC_STATUS_WITH_NODE_NOT_LEADER_ERROR_DETAILS_V2_BIN,
            status.SerializeToString(),
        )

    def test_extract_leader_node(self) -> None:
        leader_node = extract_current_leader_from_status_string(
            EXAMPLE_GRPC_STATUS_WITH_NODE_NOT_LEADER_ERROR_DETAILS_V2_BIN
        )
        assert leader_node is not None
        self.assertIsInstance(leader_node, NotLeaderNodeErrorDetails.NodeInfo)
        self.assertEqual(leader_node.host, "127.0.0.1")
        self.assertEqual(leader_node.port, 2111)
        self.assertEqual(leader_node.node_id, "9091e4a1-259d-4ba5-8439-69696ce67a27")
        self.assertIsNone(extract_current_leader_from_status_string(b""))
