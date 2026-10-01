import HTTP3
import NIOCore

final class ServerHTTP3SettingsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP3Frame

    private let settingsPromise: EventLoopPromise<HTTP3Settings>
    private var receivedSettings = false

    init(settingsPromise: EventLoopPromise<HTTP3Settings>) {
        self.settingsPromise = settingsPromise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = self.unwrapInboundIn(data)
        if case .settings(let settingsFrame) = frame, !self.receivedSettings {
            self.receivedSettings = true
            self.settingsPromise.succeed(settingsFrame.settings)
        }
        context.fireChannelRead(data)
    }
}
