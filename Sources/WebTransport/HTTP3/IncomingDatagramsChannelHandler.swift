import NIOCore
import NIOHTTP3

final class IncomingDatagramsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP3Datagram

    let incomingDatagrams: WebTransportConnection.IncomingStreamsAndDatagrams

    init(incomingDatagrams: WebTransportConnection.IncomingStreamsAndDatagrams) {
        self.incomingDatagrams = incomingDatagrams
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.incomingDatagrams.yieldDatagram(self.unwrapInboundIn(data))
        context.fireChannelRead(data)
    }
}
