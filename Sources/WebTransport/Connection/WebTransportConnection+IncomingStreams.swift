import NIOCore
import NIOQUICHelpers
import Synchronization

extension WebTransportConnection {
    final class IncomingUnidirectionalStreams: Sendable {
        private let sessionStreamsMap: Mutex<[QUICStreamID: WebTransportSession.IncomingUnidirectionalStreams.Continuation]>

        init() {
            self.sessionStreamsMap = Mutex([:])
        }

        func addSession(id: QUICStreamID, continuation: WebTransportSession.IncomingUnidirectionalStreams.Continuation) {
            self.sessionStreamsMap.withLock { $0[id] = continuation }
        }

        func yieldStream(_ stream: NIOAsyncChannel<ByteBuffer, Never>, sessionID: QUICStreamID) {
            self.sessionStreamsMap.withLock { _ = $0[sessionID]?.yield(stream) }
        }
    }

    final class IncomingBidirectionalStreams: Sendable {
        private let sessionStreamsMap: Mutex<[QUICStreamID: WebTransportSession.IncomingBidirectionalStreams.Continuation]>

        init() {
            self.sessionStreamsMap = Mutex([:])
        }

        func addSession(id: QUICStreamID, continuation: WebTransportSession.IncomingBidirectionalStreams.Continuation) {
            self.sessionStreamsMap.withLock { $0[id] = continuation }
        }

        func yieldStream(_ stream: NIOAsyncChannel<ByteBuffer, ByteBuffer>, sessionID: QUICStreamID) {
            self.sessionStreamsMap.withLock { _ = $0[sessionID]?.yield(stream) }
        }
    }
}
