import NIOCore
import NIOHTTP3
import NIOQUICHelpers
import Synchronization

extension WebTransportConnection {
    final class IncomingStreamsAndDatagrams: Sendable {
        struct SessionStreamsAndDatagrams {
            let incomingUnidirectionalStreamsContinuation: WebTransportSession.IncomingUnidirectionalStreams.Continuation
            let incomingBidirectionalStreamsContinuation: WebTransportSession.IncomingBidirectionalStreams.Continuation
            let incomingDatagramsContinuation: AsyncStream<ByteBuffer>.Continuation
        }

        private let sessionMap: Mutex<[QUICStreamID: SessionStreamsAndDatagrams]>

        init() {
            self.sessionMap = Mutex([:])
        }

        func addSession(
            id: QUICStreamID,
            unidirectionalStreamsContinuation: WebTransportSession.IncomingUnidirectionalStreams.Continuation,
            bidirectionalStreamsContinuation: WebTransportSession.IncomingBidirectionalStreams.Continuation,
            datagramsContinuation: AsyncStream<ByteBuffer>.Continuation
        ) {
            self.sessionMap.withLock {
                $0[id] = SessionStreamsAndDatagrams(
                    incomingUnidirectionalStreamsContinuation: unidirectionalStreamsContinuation,
                    incomingBidirectionalStreamsContinuation: bidirectionalStreamsContinuation,
                    incomingDatagramsContinuation: datagramsContinuation
                )
            }
        }

        func yieldUnidirectionalStream(_ stream: NIOAsyncChannel<ByteBuffer, Never>, sessionID: QUICStreamID) {
            self.sessionMap.withLock { _ = $0[sessionID]?.incomingUnidirectionalStreamsContinuation.yield(stream) }
        }

        func yieldBidirectionalStream(_ stream: NIOAsyncChannel<ByteBuffer, ByteBuffer>, sessionID: QUICStreamID) {
            self.sessionMap.withLock { _ = $0[sessionID]?.incomingBidirectionalStreamsContinuation.yield(stream) }
        }

        func yieldDatagram(_ datagram: HTTP3Datagram) {
            self.sessionMap.withLock { _ = $0[datagram.streamID]?.incomingDatagramsContinuation.yield(datagram.payload) }
        }
    }
}
