import NIOCore
import NIOHTTPTypes

/// A channel handler that intercepts HTTP/3 capsules.
final class CapsulesChannelHandler: ChannelInboundHandler {
    typealias InboundIn = HTTPResponsePart
    typealias InboundOut = HTTPResponsePart

    private let flowControlHandler: FlowControlHandler
    private let serverBlockedCapsules: ServerBlockedCapsules

    init(flowControlHandler: FlowControlHandler, serverBlockedCapsules: ServerBlockedCapsules) {
        self.flowControlHandler = flowControlHandler
        self.serverBlockedCapsules = serverBlockedCapsules
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let responsePart = self.unwrapInboundIn(data)
        guard case .body(var buffer) = responsePart else {
            context.fireChannelRead(data)
            return
        }

        let capsuleType = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic)
        _ = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic)  // Capsule Length
        switch capsuleType {
        case 0x190B_4D40:
            // `WT_MAX_STREAMS` (unidirectional)
            guard let maxUnidirectionalStreams = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic) else {
                context.fireChannelRead(data)
                return
            }
            self.flowControlHandler.updateServerMaxUnidirectionalStreams(maxUnidirectionalStreams)
        case 0x190B_4D3F:
            // `WT_MAX_STREAMS` (bidirectional)
            guard let maxBidirectionalStreams = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic) else {
                context.fireChannelRead(data)
                return
            }
            self.flowControlHandler.updateServerMaxBidirectionalStreams(maxBidirectionalStreams)
        case 0x190B_4D3D:
            // `WT_MAX_DATA`
            guard let maxData = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic) else {
                context.fireChannelRead(data)
                return
            }
            self.flowControlHandler.updateServerMaxData(maxData)
        case 0x190B_4D44:
            // `WT_STREAMS_BLOCKED` (unidirectional)
            guard let limit = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic) else {
                context.fireChannelRead(data)
                return
            }
            if self.flowControlHandler.isEnabled { self.serverBlockedCapsules.unidirectionalStreamsContinuation.yield(limit) }
        case 0x190B_4D43:
            // `WT_STREAMS_BLOCKED` (bidirectional)
            guard let limit = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic) else {
                context.fireChannelRead(data)
                return
            }
            if self.flowControlHandler.isEnabled { self.serverBlockedCapsules.bidirectionalStreamsContinuation.yield(limit) }
        case 0x190B_4D41:
            // `WT_DATA_BLOCKED`
            guard let limit = buffer.readEncodedInteger(as: UInt64.self, strategy: .quic) else {
                context.fireChannelRead(data)
                return
            }
            if self.flowControlHandler.isEnabled { self.serverBlockedCapsules.dataContinuation.yield(limit) }
        case .some:
            context.fireChannelRead(data)
        case .none:
            // TODO: throw an error since on the CONNECT stream we expect only capsules
            context.fireChannelRead(data)
        }
    }
}

/// The continuations used to report the `WT_STREAMS_BLOCKED` and `WT_DATA_BLOCKED` capsules sent by the server.
struct ServerBlockedCapsules: Sendable {
    let unidirectionalStreamsContinuation: AsyncStream<UInt64>.Continuation
    let bidirectionalStreamsContinuation: AsyncStream<UInt64>.Continuation
    let dataContinuation: AsyncStream<UInt64>.Continuation

    func finish() {
        self.unidirectionalStreamsContinuation.finish()
        self.bidirectionalStreamsContinuation.finish()
        self.dataContinuation.finish()
    }
}
