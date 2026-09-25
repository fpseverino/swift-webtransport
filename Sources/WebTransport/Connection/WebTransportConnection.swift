import HTTPTypes
public import Logging
public import NIOCore
@_spi(HTTP3AsyncInterface) import NIOHTTP3
import NIOHTTPTypes
public import NIOPosix
import NIOQUIC
import NIOQUICHelpers
import RawStructuredFieldValues

/// A single connection to a WebTransport server.
public final actor WebTransportConnection: Sendable {
    /// The logger to use for this connection.
    private let logger: Logger

    /// The QUIC stream ID of the CONNECT stream that established the WebTransport session.
    private let sessionID: QUICStreamID

    /// Used to open QUIC streams
    private let h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>

    /// An asynchronous sequence of unidirectional streams opened by the server.
    /// Each one can be used to read data from the server.
    public let incomingUnidirectionalStreams: IncomingUnidirectionalStreams

    /// An asynchronous sequence of bidirectional streams opened by the server.
    /// Each one can be used to read data from the server and write data back to it.
    public let incomingBidirectionalStreams: IncomingBidirectionalStreams

    /// The channel used for sending HTTP Datagrams
    private let datagramChannel: any Channel

    init(
        logger: Logger,
        sessionID: QUICStreamID,
        h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>,
        incomingUnidirectionalStreams: IncomingUnidirectionalStreams,
        incomingBidirectionalStreams: IncomingBidirectionalStreams,
        datagramChannel: any Channel
    ) {
        self.logger = logger
        self.sessionID = sessionID
        self.h3Connection = h3Connection
        self.incomingUnidirectionalStreams = incomingUnidirectionalStreams
        self.incomingBidirectionalStreams = incomingBidirectionalStreams
        self.datagramChannel = datagramChannel
    }

    /// Connect to the WebTransport server and run operations using the connection, then automatically close the connection.
    ///
    /// - Parameters:
    ///   - ipAddress: The IP address of the WebTransport server.
    ///   - port: The port of the WebTransport server.
    ///   - configuration: The configuration for the WebTransport connection.
    ///   - eventLoopGroup: The `EventLoopGroup` to run the connection on.
    ///   - logger: The logger to use for the connection. Defaults to the current task-local logger.
    ///   - operation: The closure where WebTransport operations using the connection are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public static func withConnection<Value>(
        ipAddress: String,
        port: Int,
        configuration: WebTransportConnectionConfiguration,
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        logger: Logger = .current,
        operation: (WebTransportConnection) async throws -> Value
    ) async throws -> Value {
        try await withH3Connection(
            ipAddress: ipAddress,
            port: port,
            verificationConfiguration: configuration.verificationConfiguration,
            eventLoopGroup: eventLoopGroup,
            logger: logger
        ) { sessionID, responseReader, requestWriter, h3Connection, incomingUnidirectionalStreams, incomingBidirectionalStreams, datagramChannel in
            var headerSerializer = StructuredFieldValueSerializer()
            var connectRequest = HTTPRequest(
                method: .connect,
                scheme: "https",
                authority: "\(ipAddress):\(port)",
                path: configuration.urlPath,
                headerFields: try .init(parsedTrailerFields: [
                    .init(
                        name: .init("WT-Available-Protocols")!,
                        value: headerSerializer.writeListFieldValue(
                            configuration.applicationProtocols.map {
                                .item(.init(bareItem: RFC9651BareItem.string($0), parameters: [:]))
                            }
                        )
                    )
                ])
            )
            // TODO: this isn't set to "webtransport-h3" to support servers that haven't implemented newer drafts of the WebTransport protocol.
            // https://github.com/BiagioFesta/wtransport/issues/328
            connectRequest.extendedConnectProtocol = "webtransport"
            try await requestWriter.write(.head(connectRequest))

            var responseIterator = responseReader.makeAsyncIterator()
            guard
                let headResponsePart = try await responseIterator.next(),
                case .head(let response) = headResponsePart,
                response.status.kind == .successful
            else {
                throw WebTransportError.serverRejectedSession
            }

            return try await operation(
                WebTransportConnection(
                    logger: logger,
                    sessionID: sessionID,
                    h3Connection: h3Connection,
                    incomingUnidirectionalStreams: incomingUnidirectionalStreams,
                    incomingBidirectionalStreams: incomingBidirectionalStreams,
                    datagramChannel: datagramChannel
                )
            )
        }
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
