import HTTP3
import HTTPTypes
import Logging
import NIOCore
@_spi(HTTP3AsyncInterface) import NIOHTTP3
import NIOHTTPTypes
import NIOPosix
import NIOQUIC
import NIOQUICHelpers

/// Connect to an HTTP/3 server,
/// run the provided closure passing to it all the necessary objects for a WebTransport session,
/// and then automatically close the connection.
///
/// - Parameters:
///   - ipAddress: The IP address of the HTTP/3 server (that also supports WebTransport).
///   - port: The port of the HTTP/3 server.
///   - verificationConfiguration: Information required to verify the server identity.
///   - eventLoopGroup: The `EventLoopGroup` to run the connection on.
///   - logger: The logger to use for the connection.
///   - body: The closure where WebTransport operations using all the necessary objects are performed.
///
/// - Returns: The value returned by the `body` closure.
func withH3Connection<Value>(
    ipAddress: String,
    port: Int,
    verificationConfiguration: VerificationConfiguration,
    eventLoopGroup: any EventLoopGroup,
    logger: Logger,
    body: (
        QUICStreamID,
        NIOAsyncChannelInboundStream<HTTPResponsePart>,
        NIOAsyncChannelOutboundWriter<HTTPRequestPart>,
        HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>,
        WebTransportConnection.IncomingUnidirectionalStreams,
        WebTransportConnection.IncomingBidirectionalStreams,
        any Channel
    ) async throws -> Value
) async throws -> Value {
    let (incomingUnidirectionalStreams, incomingUnidirectionalStreamsContinuation) = WebTransportConnection.IncomingUnidirectionalStreams.makeStream()
    let (incomingBidirectionalStreams, incomingBidirectionalStreamsContinuation) = WebTransportConnection.IncomingBidirectionalStreams.makeStream()
    let (quicChannel, connectionCreator) = try await DatagramBootstrap(group: eventLoopGroup)
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .bind(host: "127.0.0.1", port: 0) { channel in
            channel.eventLoop.makeCompletedFuture {
                let (quicHandler, _) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
                    channel: channel,
                    quicConfiguration: QUICConfiguration.client(
                        verificationConfiguration: verificationConfiguration,
                        applicationProtocols: ["h3"]
                    ),
                    logger: logger,
                    inboundStreamChannelInitializer: { channel -> EventLoopFuture<Never> in
                        channel.eventLoop.makeCompletedFuture { fatalError() }
                    }
                )
                try channel.pipeline.syncOperations.addHandler(quicHandler)
                let connectionCreator = TestHTTP3SingleConnectionCreator(
                    quicHandler: quicHandler,
                    connectionInitializer: { connectionChannel, streamCreator in
                        connectionChannel.eventLoop.makeCompletedFuture {
                            let h3Handler = HTTP3ConnectionHandler.client(
                                eventLoop: connectionChannel.eventLoop,
                                configuration: .defaults,
                                settings: HTTP3Settings(h3Datagram: true),
                                streamCreator: streamCreator,
                                logger: logger,
                                inboundPushStreamInitializer: { _ in fatalError() },
                                internalInboundStreamInitializer: { streamChannel, _, streamType in
                                    guard
                                        case .unknown(let raw) = streamType,
                                        raw == 0x54
                                    else {
                                        return streamChannel.eventLoop.makeSucceededVoidFuture()
                                    }
                                    incomingUnidirectionalStreamsContinuation.yield(
                                        try! NIOAsyncChannel<ByteBuffer, Never>(
                                            wrappingChannelSynchronously: streamChannel,
                                            configuration: .init(isOutboundHalfClosureEnabled: true)
                                        )
                                    )
                                    return streamChannel.eventLoop.makeSucceededVoidFuture()
                                }
                            )
                            try connectionChannel.pipeline.syncOperations.addHandler(h3Handler)
                            return connectionChannel
                        }
                    },
                    inboundStreamInitializer: { streamChannel in
                        let quicStreamID =
                            if let sync = streamChannel.syncOptions {
                                try! sync.getOption(.quicStreamID)
                            } else {
                                try! streamChannel.getOption(.quicStreamID).wait()
                            }
                        switch QUICStreamID(rawValue: quicStreamID).type {
                        case .serverInitiatedBidirectional:
                            incomingBidirectionalStreamsContinuation.yield(
                                try! NIOAsyncChannel<ByteBuffer, ByteBuffer>(
                                    wrappingChannelSynchronously: streamChannel,
                                    configuration: .init(isOutboundHalfClosureEnabled: true)
                                )
                            )
                            return streamChannel.eventLoop.makeSucceededVoidFuture()
                        case .serverInitiatedUnidirectional, .clientInitiatedUnidirectional, .clientInitiatedBidirectional:
                            break
                        }
                        return streamChannel.parent!.pipeline.handler(type: HTTP3ConnectionHandler<NIOQUIC.QUICStreamCreator>.self)
                            .flatMap { http3Handler in
                                http3Handler.inboundStreamReceived(streamChannel)
                            }
                    },
                    connectionState: ConnectionChannelState()
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

    let connectionChannel = try await quicChannel.eventLoop.flatSubmit { () -> EventLoopFuture<any Channel> in
        guard let connectionChannelFuture = connectionCreator.value.connectionState.future else {
            return quicChannel.eventLoop.makeFailedFuture(ChannelError.operationUnsupported)
        }
        return connectionChannelFuture
    }.get()

    do {
        let asyncChannel = try await h3Connection.makeRequestStream()
        let value = try await asyncChannel.executeThenClose {
            try await body(
                QUICStreamID(rawValue: try await asyncChannel.channel.getOption(.quicStreamID).get()),
                $0,
                $1,
                h3Connection,
                incomingUnidirectionalStreams,
                incomingBidirectionalStreams,
                connectionChannel
            )
        }

        do {
            try await quicChannel.close()
            try await connectionChannel.close()
        } catch ChannelError.alreadyClosed {
            ()
        }

        return value
    } catch {
        do {
            try await quicChannel.close()
            try await connectionChannel.close()
        } catch ChannelError.alreadyClosed {
            ()
        }

        throw error
    }
}

struct TestHTTP3SingleConnectionCreator: HTTP3ConnectionCreator {
    let quicHandler: QUICHandler<QUICStreamChannels>
    let connectionInitializer: @Sendable (any Channel, NIOQUIC.QUICStreamCreator) -> EventLoopFuture<any Channel>
    let inboundStreamInitializer: @Sendable (any Channel) -> EventLoopFuture<Void>

    var connectionEstablished: Bool = false
    let connectionState: ConnectionChannelState

    func createNewConnection(
        serverName: String,
        remoteAddress: SocketAddress,
        connectionInitializer h3ConnectionInitializer: @escaping @Sendable (any Channel) -> EventLoopFuture<Void>
    ) -> EventLoopFuture<any Channel> {
        guard self.connectionEstablished == false else {
            fatalError("This connection creator only supports creating one connection.")
        }

        let connectionChannelFuture = self.quicHandler.createOutboundConnection(
            serverName: serverName,
            remoteAddress: remoteAddress,
            connectionInitializer: { [connectionInitializer] connectionChannel, streamCreator in
                connectionInitializer(connectionChannel, streamCreator).flatMap { newConnectionChannel in
                    h3ConnectionInitializer(newConnectionChannel)
                }
            },
            inboundStreamInitializer: self.inboundStreamInitializer
        ).map { connectionChannel, _ in
            connectionChannel
        }

        self.connectionState.future = connectionChannelFuture

        return connectionChannelFuture
    }
}

final class ConnectionChannelState: @unchecked Sendable {
    var future: EventLoopFuture<any Channel>?
}

extension HTTP3ClientConnection {
    /// Opens a single request stream on this connection wrapped in a `NIOAsyncChannel`.
    /// The stream is closed by the caller using `executeThenClose`.
    fileprivate func makeRequestStream() async throws -> NIOAsyncChannel<HTTPResponsePart, HTTPRequestPart> {
        try await self.concurrencyView.createRequestStream {
            let streamChannel = $0.channel
            return streamChannel.eventLoop.makeCompletedFuture {
                try NIOAsyncChannel<HTTPResponsePart, HTTPRequestPart>(
                    wrappingChannelSynchronously: streamChannel,
                    configuration: .init(isOutboundHalfClosureEnabled: true)
                )
            }
        }
    }

    /// > Note: This requires changes in NIOHTTP3 to expose the `streamCreator`.
    func makeUnidirectionalStream() async throws -> NIOAsyncChannel<Never, ByteBuffer> {
        try await self.h3Handler.eventLoop.flatSubmit {
            self.h3Handler.value.coordinator.streamCreator.createUnidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: Never.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
        }.get()
    }

    /// > Note: This requires changes in NIOHTTP3 to expose the `streamCreator`.
    func makeBidirectionalStream() async throws -> NIOAsyncChannel<ByteBuffer, ByteBuffer> {
        try await self.h3Handler.eventLoop.flatSubmit {
            self.h3Handler.value.coordinator.streamCreator.createBidirectionalStream { streamInitializer in
                streamInitializer.channel.eventLoop.makeCompletedFuture {
                    try NIOAsyncChannel(
                        wrappingChannelSynchronously: streamInitializer.channel,
                        configuration: .init(
                            isOutboundHalfClosureEnabled: true,
                            inboundType: ByteBuffer.self,
                            outboundType: ByteBuffer.self
                        )
                    )
                }
            }
        }.get()
    }
}
