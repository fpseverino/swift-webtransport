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
public struct WebTransportSession: Sendable, ~Copyable {
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

    /// The future that will be notified when a `WT_DRAIN_SESSION` capsule is received from the server.
    private let drainingSessionFuture: EventLoopFuture<Void>

    private let flowControlHandler: FlowControlHandler
    /// An asynchronous sequence of `WT_STREAMS_BLOCKED` capsules for unidirectional streams sent by the server.
    ///
    /// Each element is the limit the server was blocked at.
    /// Receiving one means the server wants to open more streams than allowed.
    /// The limit can be raised with ``increaseMaxUnidirectionalStreams(_:)``.
    ///
    /// The sequence is always empty if Flow Control is not enabled.
    public let incomingUnidirectionalStreamsBlocked: AsyncStream<UInt64>
    /// An asynchronous sequence of `WT_STREAMS_BLOCKED` capsules for bidirectional streams sent by the server.
    ///
    /// Each element is the limit the server was blocked at.
    /// Receiving one means the server wants to open more streams than allowed.
    /// The limit can be raised with ``increaseMaxBidirectionalStreams(_:)``.
    ///
    /// The sequence is always empty if Flow Control is not enabled.
    public let incomingBidirectionalStreamsBlocked: AsyncStream<UInt64>
    /// An asynchronous sequence of `WT_DATA_BLOCKED` capsules sent by the server.
    ///
    /// Each element is the data limit the server was blocked at.
    /// Receiving one means the server wants to send more data than allowed.
    /// The limit can be raised with ``increaseMaxData(_:)``.
    ///
    /// The sequence is always empty if Flow Control is not enabled.
    public let incomingDataBlocked: AsyncStream<UInt64>

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
        drainingSessionFuture: EventLoopFuture<Void>,
        flowControlHandler: FlowControlHandler,
        incomingUnidirectionalStreamsBlocked: AsyncStream<UInt64>,
        incomingBidirectionalStreamsBlocked: AsyncStream<UInt64>,
        incomingDataBlocked: AsyncStream<UInt64>,
        incomingUnidirectionalStreams: IncomingUnidirectionalStreams,
        incomingBidirectionalStreams: IncomingBidirectionalStreams,
        datagramChannel: any Channel,
        incomingDatagrams: AsyncStream<ByteBuffer>
    ) {
        self.sessionID = sessionID
        self.applicationProtocol = applicationProtocol
        self.h3Connection = h3Connection
        self.connectStreamWriter = connectStreamWriter
        self.drainingSessionFuture = drainingSessionFuture
        self.flowControlHandler = flowControlHandler
        self.incomingUnidirectionalStreamsBlocked = incomingUnidirectionalStreamsBlocked
        self.incomingBidirectionalStreamsBlocked = incomingBidirectionalStreamsBlocked
        self.incomingDataBlocked = incomingDataBlocked
        self.incomingUnidirectionalStreams = incomingUnidirectionalStreams
        self.incomingBidirectionalStreams = incomingBidirectionalStreams
        self.datagramChannel = datagramChannel
        self.incomingDatagrams = incomingDatagrams
    }

    private func makeOutboundWriter(_ writer: NIOAsyncChannelOutboundWriter<ByteBuffer>) -> WebTransportStreamOutboundWriter {
        .init(writer: writer, flowControlHandler: self.flowControlHandler, connectStreamWriter: self.connectStreamWriter)
    }

    /// Open a unidirectional stream that can be used to write to the server.
    ///
    /// - Parameter operation: The closure where writing operations are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public func withUnidirectionalStream<Value>(
        operation: (consuming WebTransportStreamOutboundWriter) async throws -> Value
    ) async throws -> Value {
        if let maxUnidirectionalStreams = self.flowControlHandler.reserveUnidirectionalStream() {
            try await self.connectStreamWriter.write(.body(.wtUniStreamsBlockedCapsule(maxStreams: maxUnidirectionalStreams)))
            throw WebTransportError.streamLimitReached(maxStreams: numericCast(maxUnidirectionalStreams))
        }

        return try await self.h3Connection.makeUnidirectionalStream().executeThenClose { _, outboundStream in
            var buffer = ByteBuffer()
            buffer.writeEncodedInteger(0x54, strategy: .quic)
            buffer.writeEncodedInteger(self.sessionID.rawValue, strategy: .quic)
            try await outboundStream.write(buffer)
            return try await operation(self.makeOutboundWriter(outboundStream))
        }
    }

    /// Open a bidirectional stream that can be used to read from and write to the server.
    ///
    /// - Parameter operation: The closure where reading and writing operations are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public func withBidirectionalStream<Value>(
        operation: (NIOAsyncChannelInboundStream<ByteBuffer>, consuming WebTransportStreamOutboundWriter) async throws -> Value
    ) async throws -> Value {
        if let maxBidirectionalStreams = self.flowControlHandler.reserveBidirectionalStream() {
            try await self.connectStreamWriter.write(.body(.wtBiStreamsBlockedCapsule(maxStreams: maxBidirectionalStreams)))
            throw WebTransportError.streamLimitReached(maxStreams: numericCast(maxBidirectionalStreams))
        }

        return try await self.h3Connection.makeBidirectionalStream().executeThenClose { inboundStream, outboundStream in
            var buffer = ByteBuffer()
            buffer.writeEncodedInteger(0x41, strategy: .quic)
            buffer.writeEncodedInteger(self.sessionID.rawValue, strategy: .quic)
            try await outboundStream.write(buffer)
            return try await operation(inboundStream, self.makeOutboundWriter(outboundStream))
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

    /// Returns when the server indicates that it would like the transport session to start draining, prior to closing it.
    public var draining: Void {
        get async throws {
            try await self.drainingSessionFuture.getAbandoningOnCancel()
        }
    }

    /// Increase the number of unidirectional streams the server is allowed to open, sending a `WT_MAX_STREAMS` capsule.
    ///
    /// - Parameter newStreams: The number of additional unidirectional streams the server is allowed to open.
    ///
    /// - Throws: ``WebTransportError/flowControlLimitExceeded`` if the limit exceeds 2^60.
    ///
    /// > Note: This does nothing if Flow Control is not enabled.
    ///
    /// > Important: Call this method sequentially, since capsules sent concurrently could be reordered.
    public func increaseMaxUnidirectionalStreams(by newStreams: UInt64) async throws {
        guard let maxStreams = try self.flowControlHandler.increaseClientMaxUnidirectionalStreams(by: newStreams) else { return }
        try await self.connectStreamWriter.write(.body(.wtMaxUniStreamsCapsule(maxStreams: maxStreams)))
    }

    /// Increase the number of bidirectional streams the server is allowed to open, sending a `WT_MAX_STREAMS` capsule.
    ///
    /// - Parameter newStreams: The number of additional bidirectional streams the server is allowed to open.
    ///
    /// - Throws: ``WebTransportError/flowControlLimitExceeded`` if the limit exceeds 2^60.
    ///
    /// > Note: This does nothing if Flow Control is not enabled.
    ///
    /// > Important: Call this method sequentially, since capsules sent concurrently could be reordered.
    public func increaseMaxBidirectionalStreams(by newStreams: UInt64) async throws {
        guard let maxStreams = try self.flowControlHandler.increaseClientMaxBidirectionalStreams(by: newStreams) else { return }
        try await self.connectStreamWriter.write(.body(.wtMaxBiStreamsCapsule(maxStreams: maxStreams)))
    }

    /// Increase the amount of data the server is allowed to send, sending a `WT_MAX_DATA` capsule.
    ///
    /// - Parameter newData: The number of additional bytes the server is allowed to send.
    ///
    /// > Note: This does nothing if Flow Control is not enabled.
    ///
    /// > Important: Call this method sequentially, since capsules sent concurrently could be reordered.
    public func increaseMaxData(by newData: UInt64) async throws {
        guard let maxData = try self.flowControlHandler.increaseClientMaxData(by: newData) else { return }
        try await self.connectStreamWriter.write(.body(.wtMaxDataCapsule(maxData: maxData)))
    }
}
