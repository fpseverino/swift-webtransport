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
    let logger: Logger
    let h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>
    nonisolated public let incomingBidirectionalStreams: AsyncStream<NIOAsyncChannel<ByteBuffer, ByteBuffer>>

    /// Initializes the WebTransport connection.
    init(
        logger: Logger,
        h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>,
        incomingBidirectionalStreams: AsyncStream<NIOAsyncChannel<ByteBuffer, ByteBuffer>>
    ) {
        self.logger = logger
        self.h3Connection = h3Connection
        self.incomingBidirectionalStreams = incomingBidirectionalStreams
    }

    /// Connect to the WebTransport server and run operations using the connection
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
            logger: logger,
            eventLoopGroup: eventLoopGroup
        ) { inbound, outbound, h3Connection, incomingBidirectionalStreams in
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
            connectRequest.extendedConnectProtocol = "webtransport-h3"
            try await outbound.write(.head(connectRequest))

            var responseIterator = inbound.makeAsyncIterator()
            guard
                let headResponsePart = try await responseIterator.next(),
                case .head(let response) = headResponsePart,
                response.status == .ok
            else {
                throw WebTransportError.serverRejectedSession
            }

            return try await operation(
                WebTransportConnection(
                    logger: logger,
                    h3Connection: h3Connection,
                    incomingBidirectionalStreams: incomingBidirectionalStreams
                )
            )
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
            buffer.writeEncodedInteger(0x00, strategy: .quic)
            try await outboundStream.write(buffer)
            return try await operation(inboundStream, outboundStream)
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
            buffer.writeEncodedInteger(0x00, strategy: .quic)
            try await outboundStream.write(buffer)
            return try await operation(outboundStream)
        }
    }
}
