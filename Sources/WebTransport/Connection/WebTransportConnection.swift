import HTTP3
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
public struct WebTransportConnection: Sendable {
    private let ipAddress: String
    private let port: Int

    /// Used to open QUIC unidirectional and bidirectional streams
    private let h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>
    private let serverSettingsFuture: EventLoopFuture<HTTP3Settings>

    private let incomingUnidirectionalStreams: IncomingUnidirectionalStreams
    private let incomingBidirectionalStreams: IncomingBidirectionalStreams
    /// The channel used for sending HTTP Datagrams
    private let datagramChannel: any Channel
    private let incomingDatagrams: IncomingDatagrams

    init(
        ipAddress: String,
        port: Int,
        h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>,
        serverSettingsFuture: EventLoopFuture<HTTP3Settings>,
        incomingUnidirectionalStreams: IncomingUnidirectionalStreams,
        incomingBidirectionalStreams: IncomingBidirectionalStreams,
        datagramChannel: any Channel,
        incomingDatagrams: IncomingDatagrams
    ) {
        self.ipAddress = ipAddress
        self.port = port
        self.h3Connection = h3Connection
        self.serverSettingsFuture = serverSettingsFuture
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
    ///   - configuration: The configuration for the WebTransport session.
    ///   - operation: The closure where WebTransport operations using the session are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public func withSession<Value>(
        configuration: WebTransportSession.Configuration,
        operation: (WebTransportSession) async throws -> Value
    ) async throws -> Value {
        guard try await self.serverSettingsFuture.get().serverSupportsWebTransport else {
            throw WebTransportError.serverDoesNotSupportWebTransport
        }

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

            let sessionID = QUICStreamID(rawValue: try await asyncChannel.channel.getOption(.quicStreamID).get())

            let (incomingUniStreams, incomingUniStreamsContinuation) = WebTransportSession.IncomingUnidirectionalStreams.makeStream()
            self.incomingUnidirectionalStreams.addSession(id: sessionID, continuation: incomingUniStreamsContinuation)

            let (incomingBiStreams, incomingBiStreamsContinuation) = WebTransportSession.IncomingBidirectionalStreams.makeStream()
            self.incomingBidirectionalStreams.addSession(id: sessionID, continuation: incomingBiStreamsContinuation)

            let (incomingDatagrams, incomingDatagramsContinuation) = AsyncStream<ByteBuffer>.makeStream()
            self.incomingDatagrams.addSession(id: sessionID, continuation: incomingDatagramsContinuation)

            return try await operation(
                WebTransportSession(
                    sessionID: sessionID,
                    h3Connection: self.h3Connection,
                    incomingUnidirectionalStreams: incomingUniStreams,
                    incomingBidirectionalStreams: incomingBiStreams,
                    datagramChannel: self.datagramChannel,
                    incomingDatagrams: incomingDatagrams
                )
            )
        }
    }
}
