import HTTP3
import NIOCore

final class ServerHTTP3SettingsChannelHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP3Frame

    private let serverSettingsPromise: EventLoopPromise<HTTP3Settings>
    private var receivedSettings = false

    init(serverSettingsPromise: EventLoopPromise<HTTP3Settings>) {
        self.serverSettingsPromise = serverSettingsPromise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = self.unwrapInboundIn(data)
        if case .settings(let settingsFrame) = frame, !self.receivedSettings {
            self.receivedSettings = true
            self.serverSettingsPromise.succeed(settingsFrame.settings)
        }
        context.fireChannelRead(data)
    }
}
