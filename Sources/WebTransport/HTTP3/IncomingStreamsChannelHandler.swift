import NIOCore
import NIOQUICHelpers

final class IncomingUnidirectionalStreamsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    let incomingUnidirectionalStreams: WebTransportConnection.IncomingStreamsAndDatagrams
    private var didReadSessionID = false

    init(incomingUnidirectionalStreams: WebTransportConnection.IncomingStreamsAndDatagrams) {
        self.incomingUnidirectionalStreams = incomingUnidirectionalStreams
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !self.didReadSessionID else {
            context.fireChannelRead(data)
            return
        }
        var buffer = self.unwrapInboundIn(data)
        guard let sessionID = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic) else {
            context.fireChannelRead(data)
            return
        }
        self.didReadSessionID = true

        let asyncChannel: NIOAsyncChannel<ByteBuffer, Never>
        do {
            asyncChannel = try NIOAsyncChannel<ByteBuffer, Never>(
                wrappingChannelSynchronously: context.channel,
                configuration: .init(isOutboundHalfClosureEnabled: true)
            )
        } catch {
            context.fireErrorCaught(error)
            return
        }
        self.incomingUnidirectionalStreams.yieldUnidirectionalStream(asyncChannel, sessionID: QUICStreamID(rawValue: sessionID))

        if buffer.readableBytes > 0 {
            context.fireChannelRead(self.wrapInboundOut(buffer))
        }
    }
}

final class IncomingBidirectionalStreamsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    let incomingBidirectionalStreams: WebTransportConnection.IncomingStreamsAndDatagrams
    private var didReadSessionID = false

    init(incomingBidirectionalStreams: WebTransportConnection.IncomingStreamsAndDatagrams) {
        self.incomingBidirectionalStreams = incomingBidirectionalStreams
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        guard !self.didReadSessionID else {
            context.fireChannelRead(data)
            return
        }
        var buffer = self.unwrapInboundIn(data)
        guard
            let signalValue = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic),
            signalValue == 0x41,
            let sessionID = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic)
        else {
            context.fireChannelRead(data)
            return
        }
        self.didReadSessionID = true

        let asyncChannel: NIOAsyncChannel<ByteBuffer, ByteBuffer>
        do {
            asyncChannel = try NIOAsyncChannel<ByteBuffer, ByteBuffer>(
                wrappingChannelSynchronously: context.channel,
                configuration: .init(isOutboundHalfClosureEnabled: true)
            )
        } catch {
            context.fireErrorCaught(error)
            return
        }
        self.incomingBidirectionalStreams.yieldBidirectionalStream(asyncChannel, sessionID: QUICStreamID(rawValue: sessionID))

        if buffer.readableBytes > 0 {
            context.fireChannelRead(self.wrapInboundOut(buffer))
        }
    }
}
