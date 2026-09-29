import NIOCore
import NIOHTTP3

final class IncomingDatagramsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP3Datagram

    let incomingDatagramsContinuation: AsyncStream<HTTP3Datagram>.Continuation

    init(incomingDatagramsContinuation: AsyncStream<HTTP3Datagram>.Continuation) {
        self.incomingDatagramsContinuation = incomingDatagramsContinuation
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.incomingDatagramsContinuation.yield(self.unwrapInboundIn(data))
        context.fireChannelRead(data)
    }
}
