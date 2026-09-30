import HTTPTypes
import Logging
public import NIOCore
@_spi(HTTP3AsyncInterface) import NIOHTTP3
import NIOHTTPTypes
import NIOPosix
import NIOQUIC
import NIOQUICHelpers
import RawStructuredFieldValues

/// A single WebTransport session opened over an HTTP/3 connection.
public final actor WebTransportSession: Sendable {
    /// The QUIC stream ID of the CONNECT stream that established the WebTransport session.
    private let sessionID: QUICStreamID

    /// Used to open QUIC unidirectional and bidirectional streams
    private let h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>

    /// An asynchronous sequence of unidirectional streams opened by the server.
    /// Each one can be used to read data from the server.
    public let incomingUnidirectionalStreams: IncomingUnidirectionalStreams

    /// An asynchronous sequence of bidirectional streams opened by the server.
    /// Each one can be used to read data from the server and write data back to it.
    public let incomingBidirectionalStreams: IncomingBidirectionalStreams

    /// The channel used for sending HTTP Datagrams
    private let datagramChannel: any Channel

    /// An asynchronous sequence of incoming datagrams
    public let incomingDatagrams: AsyncStream<ByteBuffer>

    init(
        sessionID: QUICStreamID,
        h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>,
        incomingUnidirectionalStreams: IncomingUnidirectionalStreams,
        incomingBidirectionalStreams: IncomingBidirectionalStreams,
        datagramChannel: any Channel,
        incomingDatagrams: AsyncStream<ByteBuffer>
    ) {
        self.sessionID = sessionID
        self.h3Connection = h3Connection
        self.incomingUnidirectionalStreams = incomingUnidirectionalStreams
        self.incomingBidirectionalStreams = incomingBidirectionalStreams
        self.datagramChannel = datagramChannel
        self.incomingDatagrams = incomingDatagrams
    }

    /// Open a unidirectional stream that can be used to write to the server.
    ///
    /// - Parameter operation: The closure where writing operations are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public func withUnidirectionalStream<Value>(
        operation: (NIOAsyncChannelOutboundWriter<ByteBuffer>) async throws -> Value
    ) async throws -> Value {
        try await self.h3Connection.makeUnidirectionalStream().executeThenClose { _, outboundStream in
            var buffer = ByteBuffer()
            buffer.writeEncodedInteger(0x54, strategy: .quic)
            buffer.writeEncodedInteger(self.sessionID.rawValue, strategy: .quic)
            try await outboundStream.write(buffer)
            return try await operation(outboundStream)
        }
    }

    /// Open a bidirectional stream that can be used to read from and write to the server.
    ///
    /// - Parameter operation: The closure where reading and writing operations are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public func withBidirectionalStream<Value>(
        operation: (NIOAsyncChannelInboundStream<ByteBuffer>, NIOAsyncChannelOutboundWriter<ByteBuffer>) async throws -> Value
    ) async throws -> Value {
        try await self.h3Connection.makeBidirectionalStream().executeThenClose { inboundStream, outboundStream in
            var buffer = ByteBuffer()
            buffer.writeEncodedInteger(0x41, strategy: .quic)
            buffer.writeEncodedInteger(self.sessionID.rawValue, strategy: .quic)
            try await outboundStream.write(buffer)
            return try await operation(inboundStream, outboundStream)
        }
    }

    /// Send a datagram to the server.
    ///
    /// - Parameter payload: The datagram payload to send.
    public func sendDatagram(_ payload: ByteBuffer) async throws {
        try await self.datagramChannel.writeAndFlush(HTTP3Datagram(streamID: self.sessionID, payload: payload))
    }
}
