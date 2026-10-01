import HTTP3
import Logging
import NIOCore
@_spi(HTTP3AsyncInterface) import NIOHTTP3
import NIOHTTPTypes
import NIOQUIC
import NIOQUICHelpers

struct TestHTTP3SingleConnectionCreator: HTTP3ConnectionCreator {
    let quicHandler: QUICHandler<QUICStreamChannels>
    let connectionInitializer: @Sendable (any Channel, NIOQUIC.QUICStreamCreator) -> EventLoopFuture<any Channel>
    let inboundStreamInitializer: @Sendable (any Channel) -> EventLoopFuture<Void>

    var connectionEstablished: Bool = false
    let connectionChannelPromise: EventLoopPromise<any Channel>

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

        // Fulfill the promise once the connection has been established.
        connectionChannelFuture.cascade(to: self.connectionChannelPromise)

        return connectionChannelFuture
    }
}

extension Channel {
    func makeConnectionCreator(
        verificationConfiguration: VerificationConfiguration,
        logger: Logger,
        serverSettingsPromise: EventLoopPromise<HTTP3Settings>,
        incomingUnidirectionalStreams: WebTransportConnection.IncomingUnidirectionalStreams,
        incomingBidirectionalStreams: WebTransportConnection.IncomingBidirectionalStreams,
        incomingDatagrams: WebTransportConnection.IncomingDatagrams
    ) throws -> TestHTTP3SingleConnectionCreator {
        let (quicHandler, _) = try QUICHandler.makeHandlerAndConnectionMultiplexer(
            channel: self,
            quicConfiguration: QUICConfiguration.client(
                verificationConfiguration: verificationConfiguration,
                applicationProtocols: ["h3"]
            ),
            logger: logger,
            inboundStreamChannelInitializer: { channel -> EventLoopFuture<Never> in
                channel.eventLoop.makeCompletedFuture { fatalError() }
            }
        )
        try self.pipeline.syncOperations.addHandler(quicHandler)
        let connectionCreator = TestHTTP3SingleConnectionCreator(
            quicHandler: quicHandler,
            connectionInitializer: { connectionChannel, streamCreator in
                connectionChannel.eventLoop.makeCompletedFuture {
                    let h3Handler = HTTP3ConnectionHandler.client(
                        eventLoop: connectionChannel.eventLoop,
                        configuration: .defaults,
                        settings: try .init(parsing: [
                            .init(identifier: .h3Datagram, value: 1),
                            // TODO: remove after WebTransport RFC is published
                            .init(identifier: .webTransportEnabled, value: 1),
                            // TODO: let the user set these
                            .init(identifier: .webTransportInitialMaximumStreamsUnidirectional, value: 100),
                            .init(identifier: .webTransportInitialMaximumStreamsBidirectional, value: 100),
                            .init(identifier: .webTransportInitialMaximumData, value: 1 << 20),
                        ]),
                        streamCreator: streamCreator,
                        logger: logger,
                        inboundPushStreamInitializer: { _ in fatalError() },
                        internalInboundStreamInitializer: { streamChannel, _, streamType in
                            streamChannel.eventLoop.makeCompletedFuture {
                                switch streamType {
                                case .control:
                                    try streamChannel.pipeline.syncOperations.addHandler(
                                        ServerHTTP3SettingsChannelHandler(serverSettingsPromise: serverSettingsPromise)
                                    )
                                case .unknown(let raw) where raw == 0x54:
                                    try streamChannel.pipeline.syncOperations.addHandler(
                                        IncomingUnidirectionalStreamsChannelHandler(incomingUnidirectionalStreams: incomingUnidirectionalStreams)
                                    )
                                case .push, .qpackEncoder, .qpackDecoder, .unknown:
                                    break
                                }
                            }
                        }
                    )
                    try connectionChannel.pipeline.syncOperations.addHandler(h3Handler)
                    try connectionChannel.pipeline.syncOperations.addHandler(
                        IncomingDatagramsChannelHandler(incomingDatagrams: incomingDatagrams)
                    )
                    return connectionChannel
                }
            },
            inboundStreamInitializer: { streamChannel in
                streamChannel.getOption(.quicStreamID).flatMap { quicStreamID in
                    switch QUICStreamID(rawValue: quicStreamID).type {
                    case .serverInitiatedBidirectional:
                        streamChannel.eventLoop.makeCompletedFuture {
                            try streamChannel.pipeline.syncOperations.addHandler(
                                IncomingBidirectionalStreamsChannelHandler(incomingBidirectionalStreams: incomingBidirectionalStreams)
                            )
                        }
                    case .serverInitiatedUnidirectional, .clientInitiatedUnidirectional, .clientInitiatedBidirectional:
                        streamChannel.parent!.pipeline.handler(type: HTTP3ConnectionHandler<NIOQUIC.QUICStreamCreator>.self)
                            .flatMap { http3Handler in
                                http3Handler.inboundStreamReceived(streamChannel)
                            }
                    }
                }
            },
            connectionChannelPromise: self.eventLoop.makePromise()
        )
        return connectionCreator
    }
}

extension HTTP3ClientConnection {
    /// Opens a single request stream on this connection wrapped in a `NIOAsyncChannel`.
    /// The stream is closed by the caller using `executeThenClose`.
    func makeRequestStream() async throws -> NIOAsyncChannel<HTTPResponsePart, HTTPRequestPart> {
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
        try await self.concurrencyView.streamCreator.createUnidirectionalStream { streamInitializer in
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
        }.get()
    }

    /// > Note: This requires changes in NIOHTTP3 to expose the `streamCreator`.
    func makeBidirectionalStream() async throws -> NIOAsyncChannel<ByteBuffer, ByteBuffer> {
        try await self.concurrencyView.streamCreator.createBidirectionalStream { streamInitializer in
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
        }.get()
    }
}
