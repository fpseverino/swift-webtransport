import HTTPTypes
public import Logging
public import NIOCore
@_spi(HTTP3AsyncInterface) import NIOHTTP3
import NIOHTTPTypes
public import NIOPosix
public import NIOQUIC
import NIOQUICHelpers
import RawStructuredFieldValues

/// A single connection to a WebTransport server.
public final actor WebTransportConnection: Sendable {
    private let ipAddress: String
    private let port: Int

    /// Used to open QUIC unidirectional and bidirectional streams
    private let h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>

    /// An asynchronous sequence of unidirectional streams opened by the server.
    /// Each one can be used to read data from the server.
    private let incomingUnidirectionalStreams: WebTransportSession.IncomingUnidirectionalStreams

    /// An asynchronous sequence of bidirectional streams opened by the server.
    /// Each one can be used to read data from the server and write data back to it.
    private let incomingBidirectionalStreams: WebTransportSession.IncomingBidirectionalStreams

    /// The channel used for sending HTTP Datagrams
    private let datagramChannel: any Channel

    /// An asynchronous sequence of incoming datagrams
    private let incomingDatagrams: AsyncStream<HTTP3Datagram>

    init(
        ipAddress: String,
        port: Int,
        h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>,
        incomingUnidirectionalStreams: WebTransportSession.IncomingUnidirectionalStreams,
        incomingBidirectionalStreams: WebTransportSession.IncomingBidirectionalStreams,
        datagramChannel: any Channel,
        incomingDatagrams: AsyncStream<HTTP3Datagram>
    ) {
        self.ipAddress = ipAddress
        self.port = port
        self.h3Connection = h3Connection
        self.incomingUnidirectionalStreams = incomingUnidirectionalStreams
        self.incomingBidirectionalStreams = incomingBidirectionalStreams
        self.datagramChannel = datagramChannel
        self.incomingDatagrams = incomingDatagrams
    }

    /// Connect to the WebTransport server and run operations using the connection, then automatically close the connection.
    ///
    /// - Parameters:
    ///   - ipAddress: The IP address of the WebTransport server.
    ///   - port: The port of the WebTransport server.
    ///   - verificationConfiguration: Information required to verify the server identity.
    ///   - eventLoopGroup: The `EventLoopGroup` to run the connection on.
    ///   - logger: The logger to use for the connection. Defaults to the current task-local logger.
    ///   - operation: The closure where WebTransport operations using the connection are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public static func withConnection<Value>(
        ipAddress: String,
        port: Int,
        verificationConfiguration: VerificationConfiguration,
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        logger: Logger = .current,
        operation: (WebTransportConnection) async throws -> Value
    ) async throws -> Value {
        try await withH3Connection(
            ipAddress: ipAddress,
            port: port,
            verificationConfiguration: verificationConfiguration,
            eventLoopGroup: eventLoopGroup,
            logger: logger,
            body: operation
        )
    }

    /// Create a new WebTransport session and run operations using it, then automatically terminate the session.
    ///
    /// - Parameters:
    ///   - ipAddress: The IP address of the WebTransport server.
    ///   - port: The port of the WebTransport server.
    ///   - configuration: The configuration for the WebTransport session.
    ///   - eventLoopGroup: The `EventLoopGroup` to run the connection on.
    ///   - logger: The logger to use for the session. Defaults to the current task-local logger.
    ///   - operation: The closure where WebTransport operations using the session are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public func withSession<Value>(
        configuration: WebTransportSession.Configuration,
        operation: (WebTransportSession) async throws -> Value
    ) async throws -> Value {
        let asyncChannel = try await self.h3Connection.makeRequestStream()
        return try await asyncChannel.executeThenClose { responseReader, requestWriter in
            var headerSerializer = StructuredFieldValueSerializer()
            var connectRequest = HTTPRequest(
                method: .connect,
                scheme: "https",
                authority: "\(self.ipAddress):\(self.port)",
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
                WebTransportSession(
                    sessionID: QUICStreamID(rawValue: try await asyncChannel.channel.getOption(.quicStreamID).get()),
                    h3Connection: self.h3Connection,
                    incomingUnidirectionalStreams: self.incomingUnidirectionalStreams,
                    incomingBidirectionalStreams: self.incomingBidirectionalStreams,
                    datagramChannel: self.datagramChannel,
                    incomingDatagrams: self.incomingDatagrams
                )
            )
        }
    }
}
