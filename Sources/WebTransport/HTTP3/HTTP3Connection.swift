import HTTP3
import Logging
import NIOCore
@_spi(HTTP3AsyncInterface) import NIOHTTP3
import NIOPosix
import NIOQUIC

/// Connect to an HTTP/3 server,
/// run the provided closure passing to it the connection,
/// and then automatically close the connection.
///
/// - Parameters:
///   - ipAddress: The IP address of the HTTP/3 server (that also supports WebTransport).
///   - port: The port of the HTTP/3 server.
///   - verificationConfiguration: Information required to verify the server identity.
///   - eventLoopGroup: The `EventLoopGroup` to run the connection on.
///   - logger: The logger to use for the connection.
///   - body: The closure where WebTransport operations using the connection are performed.
///
/// - Returns: The value returned by the `body` closure.
func withH3Connection<Value>(
    ipAddress: String,
    port: Int,
    configuration: WebTransportConnection.Configuration,
    eventLoopGroup: any EventLoopGroup,
    logger: Logger,
    body: (WebTransportConnection) async throws -> Value
) async throws -> Value {
    let serverSettingsPromise = eventLoopGroup.any().makePromise(of: HTTP3Settings.self)
    // If the connection fails before the server settings arrive, the promise must still be completed;
    // this is a no-op when it has already been completed.
    defer { serverSettingsPromise.fail(ChannelError.ioOnClosedChannel) }

    let incomingUnidirectionalStreams = WebTransportConnection.IncomingUnidirectionalStreams()
    let incomingBidirectionalStreams = WebTransportConnection.IncomingBidirectionalStreams()
    let incomingDatagrams = WebTransportConnection.IncomingDatagrams()

    let (quicChannel, connectionCreator) = try await DatagramBootstrap(group: eventLoopGroup)
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .bind(host: "127.0.0.1", port: 0) { channel in
            channel.eventLoop.makeCompletedFuture {
                let connectionCreator = try channel.makeConnectionCreator(
                    configuration: configuration,
                    logger: logger,
                    serverSettingsPromise: serverSettingsPromise,
                    incomingUnidirectionalStreams: incomingUnidirectionalStreams,
                    incomingBidirectionalStreams: incomingBidirectionalStreams,
                    incomingDatagrams: incomingDatagrams
                )
                return (channel, NIOLoopBound(connectionCreator, eventLoop: channel.eventLoop))
            }
        }

    let multiplexer = HTTP3ClientConnectionMultiplexer<
        TestHTTP3SingleConnectionCreator,
        NIOQUIC.QUICStreamCreator
    >(
        eventLoop: quicChannel.eventLoop,
        createNewConnection: connectionCreator
    )

    let h3Connection = try await multiplexer.concurrencyView.createConnection(
        serverName: "127.0.0.1",
        remoteAddress: .init(ipAddress: ipAddress, port: port),
        inboundPushStreamInitializer: { _ in fatalError("Push streams not supported") }
    )

    let connectionChannel = try await quicChannel.eventLoop.flatSubmit {
        connectionCreator.value.connectionChannelPromise.futureResult
    }.get()

    do {
        let value = try await body(
            WebTransportConnection(
                ipAddress: ipAddress,
                port: port,
                h3Connection: h3Connection,
                serverSettingsFuture: serverSettingsPromise.futureResult,
                incomingUnidirectionalStreams: incomingUnidirectionalStreams,
                incomingBidirectionalStreams: incomingBidirectionalStreams,
                datagramChannel: connectionChannel,
                incomingDatagrams: incomingDatagrams
            )
        )

        do {
            try await quicChannel.close()
            try await connectionChannel.close()
        } catch ChannelError.alreadyClosed {}

        return value
    } catch {
        do {
            try await quicChannel.close()
            try await connectionChannel.close()
        } catch ChannelError.alreadyClosed {}
        throw error
    }
}
