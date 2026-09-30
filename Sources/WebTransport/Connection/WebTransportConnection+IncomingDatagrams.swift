import NIOCore
import NIOHTTP3
import NIOQUICHelpers
import Synchronization

extension WebTransportConnection {
    final class IncomingDatagrams: Sendable {
        private let sessionDatagramsMap: Mutex<[QUICStreamID: AsyncStream<ByteBuffer>.Continuation]>

        init() {
            self.sessionDatagramsMap = Mutex([:])
        }

        func addSession(id: QUICStreamID, continuation: AsyncStream<ByteBuffer>.Continuation) {
            self.sessionDatagramsMap.withLock { $0[id] = continuation }
        }

        func yieldDatagram(_ datagram: HTTP3Datagram) {
            self.sessionDatagramsMap.withLock { _ = $0[datagram.streamID]?.yield(datagram.payload) }
        }
    }
}
