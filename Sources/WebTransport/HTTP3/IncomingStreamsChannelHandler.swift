import NIOCore
import NIOQUICHelpers

final class IncomingUnidirectionalStreamsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    let incomingUnidirectionalStreams: WebTransportConnection.IncomingUnidirectionalStreams
    private var didReadSessionID = false

    init(incomingUnidirectionalStreams: WebTransportConnection.IncomingUnidirectionalStreams) {
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
        self.incomingUnidirectionalStreams.yieldStream(
            try! NIOAsyncChannel<ByteBuffer, Never>(
                wrappingChannelSynchronously: context.channel,
                configuration: .init(isOutboundHalfClosureEnabled: true)
            ),
            sessionID: QUICStreamID(rawValue: sessionID)
        )
        if buffer.readableBytes > 0 {
            context.fireChannelRead(self.wrapInboundOut(buffer))
        }
    }
}

final class IncomingBidirectionalStreamsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias InboundOut = ByteBuffer

    let incomingBidirectionalStreams: WebTransportConnection.IncomingBidirectionalStreams
    private var didReadSessionID = false

    init(incomingBidirectionalStreams: WebTransportConnection.IncomingBidirectionalStreams) {
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
        self.incomingBidirectionalStreams.yieldStream(
            try! NIOAsyncChannel<ByteBuffer, ByteBuffer>(
                wrappingChannelSynchronously: context.channel,
                configuration: .init(isOutboundHalfClosureEnabled: true)
            ),
            sessionID: QUICStreamID(rawValue: sessionID)
        )
        if buffer.readableBytes > 0 {
            context.fireChannelRead(self.wrapInboundOut(buffer))
        }
    }
}
