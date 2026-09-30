import NIOCore
import NIOHTTP3

final class IncomingDatagramsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP3Datagram

    let incomingDatagrams: WebTransportConnection.IncomingDatagrams

    init(incomingDatagrams: WebTransportConnection.IncomingDatagrams) {
        self.incomingDatagrams = incomingDatagrams
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        self.incomingDatagrams.yieldDatagram(self.unwrapInboundIn(data))
        context.fireChannelRead(data)
    }
}
