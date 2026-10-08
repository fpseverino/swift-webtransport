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
public struct WebTransportSession: Sendable {
    /// The QUIC stream ID of the CONNECT stream that established the WebTransport session.
    private let sessionID: QUICStreamID
    /// A string representing the application-specific protocol selected by the server, or `nil` if none has been selected.
    ///
    /// Client preferences for the protocol are passed to the session configuration in the ``WebTransportSession/Configuration/applicationProtocols`` option.
    public let applicationProtocol: String?
    /// Used to open QUIC unidirectional and bidirectional streams
    private let h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>
    /// Used to write capsules to the CONNECT stream that established the WebTransport session.
    private let connectStreamWriter: NIOAsyncChannelOutboundWriter<HTTPRequestPart>

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
        applicationProtocol: String?,
        h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>,
        connectStreamWriter: NIOAsyncChannelOutboundWriter<HTTPRequestPart>,
        incomingUnidirectionalStreams: IncomingUnidirectionalStreams,
        incomingBidirectionalStreams: IncomingBidirectionalStreams,
        datagramChannel: any Channel,
        incomingDatagrams: AsyncStream<ByteBuffer>
    ) {
        self.sessionID = sessionID
        self.applicationProtocol = applicationProtocol
        self.h3Connection = h3Connection
        self.incomingUnidirectionalStreams = incomingUnidirectionalStreams
        self.incomingBidirectionalStreams = incomingBidirectionalStreams
        self.connectStreamWriter = connectStreamWriter
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

    /// Indicate to the server that the client would like the transport session to start draining, prior to closing it.
    public func drain() async throws {
        try await self.connectStreamWriter.write(.body(.wtDrainSessionCapsule))
    }
}

extension ByteBuffer {
    /// The WT_DRAIN_SESSION capsule.
    static let wtDrainSessionCapsule: ByteBuffer = {
        var buffer = ByteBuffer()
        buffer.writeEncodedInteger(0x78ae, strategy: .quic)
        buffer.writeEncodedInteger(0, strategy: .quic)
        return buffer
    }()
}
