import HTTP3
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
public struct WebTransportConnection: Sendable, ~Copyable {
    private let ipAddress: String
    private let port: Int

    /// Used to open QUIC unidirectional and bidirectional streams
    private let h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>
    private let serverSettingsFuture: EventLoopFuture<HTTP3Settings>
    private let flowControlSettings: Configuration.FlowControlSettings

    private let incomingStreamsAndDatagrams: IncomingStreamsAndDatagrams
    /// The channel used for sending HTTP Datagrams
    private let datagramChannel: any Channel

    init(
        ipAddress: String,
        port: Int,
        h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>,
        serverSettingsFuture: EventLoopFuture<HTTP3Settings>,
        flowControlSettings: Configuration.FlowControlSettings,
        incomingStreamsAndDatagrams: IncomingStreamsAndDatagrams,
        datagramChannel: any Channel
    ) {
        self.ipAddress = ipAddress
        self.port = port
        self.h3Connection = h3Connection
        self.serverSettingsFuture = serverSettingsFuture
        self.flowControlSettings = flowControlSettings
        self.incomingStreamsAndDatagrams = incomingStreamsAndDatagrams
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
        configuration: Configuration,
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        logger: Logger = .current,
        operation: (consuming WebTransportConnection) async throws -> Value
    ) async throws -> Value {
        try await withH3Connection(
            ipAddress: ipAddress,
            port: port,
            configuration: configuration,
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
        operation: (consuming WebTransportSession) async throws -> Value
    ) async throws -> Value {
        let serverSettings: HTTP3Settings = try await self.serverSettingsFuture.get()
        guard serverSettings.serverSupportsWebTransport else {
            throw WebTransportError.serverDoesNotSupportWebTransport
        }

        // Flow Control is enabled only if both endpoints declared their intent to use it.
        let isFlowControlEnabled = self.flowControlSettings.enablesFlowControl && serverSettings.enablesWebTransportFlowControl

        // Without Flow Control, a client MUST NOT establish more than one simultaneous session.
        try self.incomingStreamsAndDatagrams.reserveSession(allowSimultaneous: isFlowControlEnabled)
        // If the session is successfully established, this will be updated with the actual Session ID.
        var activeSessionID: QUICStreamID?
        defer { self.incomingStreamsAndDatagrams.removeSession(id: activeSessionID) }

        let flowControlHandler = FlowControlHandler(
            initialServerMaxUnidirectionalStreams: serverSettings.other
                .first { $0.identifier == .webTransportInitialMaximumStreamsUnidirectional }?.value ?? 0,
            initialServerMaxBidirectionalStreams: serverSettings.other
                .first { $0.identifier == .webTransportInitialMaximumStreamsBidirectional }?.value ?? 0,
            initialServerMaxData: serverSettings.other
                .first { $0.identifier == .webTransportInitialMaximumData }?.value ?? 0,
            initialClientMaxUnidirectionalStreams: numericCast(self.flowControlSettings.initialMaximumUnidirectionalStreams),
            initialClientMaxBidirectionalStreams: numericCast(self.flowControlSettings.initialMaximumBidirectionalStreams),
            initialClientMaxData: numericCast(self.flowControlSettings.initialMaximumData)
        )

        let (incomingUniStreamsBlocked, uniStreamsBlockedContinuation) = AsyncStream<UInt64>.makeStream()
        let (incomingBiStreamsBlocked, biStreamsBlockedContinuation) = AsyncStream<UInt64>.makeStream()
        let (incomingDataBlocked, dataBlockedContinuation) = AsyncStream<UInt64>.makeStream()
        let serverBlockedCapsules = ServerBlockedCapsules(
            unidirectionalStreamsContinuation: uniStreamsBlockedContinuation,
            bidirectionalStreamsContinuation: biStreamsBlockedContinuation,
            dataContinuation: dataBlockedContinuation
        )
        defer { serverBlockedCapsules.finish() }

        let drainingSessionPromise = self.datagramChannel.eventLoop.makePromise(of: Void.self)
        defer { drainingSessionPromise.succeed() }

        let asyncChannel = try await self.h3Connection.makeRequestStream(
            drainingSessionPromise: drainingSessionPromise,
            flowControlHandler: flowControlHandler,
            serverBlockedCapsules: serverBlockedCapsules
        )
        return try await asyncChannel.executeThenClose { responseReader, requestWriter in
            var headerFields: HTTPFields = [:]
            if !configuration.applicationProtocols.isEmpty {
                var headerSerializer = StructuredFieldValueSerializer()
                headerFields.append(
                    HTTPField(
                        name: .init("WT-Available-Protocols")!,
                        value: try headerSerializer.writeListFieldValue(
                            configuration.applicationProtocols.map {
                                .item(.init(bareItem: RFC9651BareItem.string($0), parameters: [:]))
                            }
                        )
                    )
                )
            }
            var connectRequest = HTTPRequest(
                method: .connect,
                scheme: "https",
                authority: "\(self.ipAddress):\(self.port)",
                path: configuration.urlPath,
                headerFields: headerFields
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

            let applicationProtocol: String?
            if let headerValue = response.headerFields[.init("WT-Protocol")!] {
                var headerParser = StructuredFieldValueParser(Array(headerValue.utf8))
                if let item = try? headerParser.parseItemFieldValue(), case .string(let value) = item.rfc9651BareItem {
                    applicationProtocol = value
                } else {
                    applicationProtocol = nil
                }
            } else {
                applicationProtocol = nil
            }

            if let applicationProtocol {
                guard configuration.applicationProtocols.contains(applicationProtocol) else {
                    throw WebTransportError.applicationProtocolNegotiationFailed
                }
            }

            let sessionID = QUICStreamID(rawValue: try await asyncChannel.channel.getOption(.quicStreamID).get())
            activeSessionID = sessionID

            let (incomingUniStreams, incomingUniStreamsContinuation) = WebTransportSession.IncomingUnidirectionalStreams.makeStream()
            let (incomingBiStreams, incomingBiStreamsContinuation) = WebTransportSession.IncomingBidirectionalStreams.makeStream()
            let (incomingDatagrams, incomingDatagramsContinuation) = AsyncStream<ByteBuffer>.makeStream()
            defer {
                incomingUniStreamsContinuation.finish()
                incomingBiStreamsContinuation.finish()
                incomingDatagramsContinuation.finish()
            }
            self.incomingStreamsAndDatagrams.addSession(
                id: sessionID,
                unidirectionalStreamsContinuation: incomingUniStreamsContinuation,
                bidirectionalStreamsContinuation: incomingBiStreamsContinuation,
                datagramsContinuation: incomingDatagramsContinuation
            )

            return try await operation(
                WebTransportSession(
                    sessionID: sessionID,
                    applicationProtocol: applicationProtocol,
                    h3Connection: self.h3Connection,
                    connectStreamWriter: requestWriter,
                    drainingSessionFuture: drainingSessionPromise.futureResult,
                    flowControlHandler: flowControlHandler,
                    incomingUnidirectionalStreamsBlocked: incomingUniStreamsBlocked,
                    incomingBidirectionalStreamsBlocked: incomingBiStreamsBlocked,
                    incomingDataBlocked: incomingDataBlocked,
                    incomingUnidirectionalStreams: incomingUniStreams,
                    incomingBidirectionalStreams: incomingBiStreams,
                    datagramChannel: self.datagramChannel,
                    incomingDatagrams: incomingDatagrams
                )
            )
        }
    }
}

extension WebTransportSession {
    /// Connect to the WebTransport server,
    /// create a single WebTransport session and run operations using it,
    /// then automatically terminate the session and close the connection.
    ///
    /// - Parameters:
    ///   - ipAddress: The IP address of the WebTransport server.
    ///   - port: The port of the WebTransport server.
    ///   - connectionConfiguration: The configuration for the WebTransport connection.
    ///   - sessionConfiguration: The configuration for the WebTransport session.
    ///   - eventLoopGroup: The `EventLoopGroup` to run the connection on.
    ///   - logger: The logger to use for the connection. Defaults to the current task-local logger.
    ///   - operation: The closure where WebTransport operations using the session are performed.
    ///
    /// - Returns: The value returned by the `operation` closure.
    public static func withSession<Value>(
        ipAddress: String,
        port: Int,
        connectionConfiguration: WebTransportConnection.Configuration,
        sessionConfiguration: WebTransportSession.Configuration,
        eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
        logger: Logger = .current,
        operation: (consuming WebTransportSession) async throws -> Value
    ) async throws -> Value {
        try await WebTransportConnection.withConnection(
            ipAddress: ipAddress,
            port: port,
            configuration: connectionConfiguration,
            eventLoopGroup: eventLoopGroup,
            logger: logger
        ) { connection in
            try await connection.withSession(configuration: sessionConfiguration, operation: operation)
        }
    }
}
