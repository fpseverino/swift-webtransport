import HTTP3
import NIOCore

/// A channel handler responsible for intercepting the server's HTTP/3 control stream.
final class ServerControlStreamChannelHandler: ChannelInboundHandler {
    typealias InboundIn = HTTP3Frame

    private let serverSettingsPromise: EventLoopPromise<HTTP3Settings>
    private var receivedSettings = false

    /// Initializes a new ``ServerControlStreamChannelHandler`` instance.
    ///
    /// - Parameters:
    ///   - serverSettingsPromise: A promise that will be fulfilled with the server's HTTP/3 settings once received.
    init(
        serverSettingsPromise: EventLoopPromise<HTTP3Settings>
    ) {
        self.serverSettingsPromise = serverSettingsPromise
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = self.unwrapInboundIn(data)
        if case .settings(let settingsFrame) = frame, !self.receivedSettings {
            self.receivedSettings = true
            self.serverSettingsPromise.succeed(settingsFrame.settings)
        } else if case .goaway = frame {
            // TODO: disallow opening new sessions after receiving GOAWAY
        }
        context.fireChannelRead(data)
    }
}
