# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from esdbclient.protos.Grpc import (
    streams_pb2 as esdbclient_dot_protos_dot_Grpc_dot_streams__pb2,
)


class StreamsStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Read = channel.unary_stream(
            "/event_store.client.streams.Streams/Read",
            request_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.ReadReq.SerializeToString,
            response_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.ReadResp.FromString,
        )
        self.Append = channel.stream_unary(
            "/event_store.client.streams.Streams/Append",
            request_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.AppendReq.SerializeToString,
            response_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.AppendResp.FromString,
        )
        self.Delete = channel.unary_unary(
            "/event_store.client.streams.Streams/Delete",
            request_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.DeleteReq.SerializeToString,
            response_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.DeleteResp.FromString,
        )
        self.Tombstone = channel.unary_unary(
            "/event_store.client.streams.Streams/Tombstone",
            request_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.TombstoneReq.SerializeToString,
            response_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.TombstoneResp.FromString,
        )
        self.BatchAppend = channel.stream_stream(
            "/event_store.client.streams.Streams/BatchAppend",
            request_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.BatchAppendReq.SerializeToString,
            response_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.BatchAppendResp.FromString,
        )


class StreamsServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Read(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def Append(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def Delete(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def Tombstone(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def BatchAppend(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_StreamsServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "Read": grpc.unary_stream_rpc_method_handler(
            servicer.Read,
            request_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.ReadReq.FromString,
            response_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.ReadResp.SerializeToString,
        ),
        "Append": grpc.stream_unary_rpc_method_handler(
            servicer.Append,
            request_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.AppendReq.FromString,
            response_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.AppendResp.SerializeToString,
        ),
        "Delete": grpc.unary_unary_rpc_method_handler(
            servicer.Delete,
            request_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.DeleteReq.FromString,
            response_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.DeleteResp.SerializeToString,
        ),
        "Tombstone": grpc.unary_unary_rpc_method_handler(
            servicer.Tombstone,
            request_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.TombstoneReq.FromString,
            response_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.TombstoneResp.SerializeToString,
        ),
        "BatchAppend": grpc.stream_stream_rpc_method_handler(
            servicer.BatchAppend,
            request_deserializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.BatchAppendReq.FromString,
            response_serializer=esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.BatchAppendResp.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "event_store.client.streams.Streams", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class Streams(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Read(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/event_store.client.streams.Streams/Read",
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.ReadReq.SerializeToString,
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.ReadResp.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def Append(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_unary(
            request_iterator,
            target,
            "/event_store.client.streams.Streams/Append",
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.AppendReq.SerializeToString,
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.AppendResp.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def Delete(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/event_store.client.streams.Streams/Delete",
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.DeleteReq.SerializeToString,
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.DeleteResp.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def Tombstone(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/event_store.client.streams.Streams/Tombstone",
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.TombstoneReq.SerializeToString,
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.TombstoneResp.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def BatchAppend(
        request_iterator,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.stream_stream(
            request_iterator,
            target,
            "/event_store.client.streams.Streams/BatchAppend",
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.BatchAppendReq.SerializeToString,
            esdbclient_dot_protos_dot_Grpc_dot_streams__pb2.BatchAppendResp.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
