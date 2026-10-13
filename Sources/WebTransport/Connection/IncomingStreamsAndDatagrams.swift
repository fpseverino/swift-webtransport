import NIOCore
import NIOHTTP3
import NIOQUICHelpers
import Synchronization

extension WebTransportConnection {
    final class IncomingStreamsAndDatagrams: Sendable {
        private struct SessionStreamsAndDatagrams {
            let incomingUnidirectionalStreamsContinuation: WebTransportSession.IncomingUnidirectionalStreams.Continuation
            let incomingBidirectionalStreamsContinuation: WebTransportSession.IncomingBidirectionalStreams.Continuation
            let incomingDatagramsContinuation: AsyncStream<ByteBuffer>.Continuation
        }

        private struct State {
            /// Number of sessions whose CONNECT request has been started but not yet accepted by the server.
            var connectingSessionsCount = 0
            var sessions: [QUICStreamID: SessionStreamsAndDatagrams] = [:]
        }

        private let state = Mutex(State())

        /// Atomically reserves a slot for a new session.
        ///
        /// - Parameter allowSimultaneous: A Boolean value indicating whether multiple simultaneous sessions are allowed.
        ///
        /// - Throws: ``WebTransportError/simultaneousSessionsNotAllowed-enum.case``
        ///     if multiple simultaneous sessions aren't allowed and one already exists.
        func reserveSession(allowSimultaneous: Bool) throws(WebTransportError) {
            try self.state.withLock { state throws(WebTransportError) in
                guard allowSimultaneous || state.connectingSessionsCount + state.sessions.count == 0 else {
                    throw WebTransportError.simultaneousSessionsNotAllowed
                }
                state.connectingSessionsCount += 1
            }
        }

        /// Releases a session slot. Pass `nil` if the session was never activated.
        func removeSession(id: QUICStreamID?) {
            self.state.withLock {
                if let id, $0.sessions.removeValue(forKey: id) != nil { return }
                $0.connectingSessionsCount -= 1
            }
        }

        func addSession(
            id: QUICStreamID,
            unidirectionalStreamsContinuation: WebTransportSession.IncomingUnidirectionalStreams.Continuation,
            bidirectionalStreamsContinuation: WebTransportSession.IncomingBidirectionalStreams.Continuation,
            datagramsContinuation: AsyncStream<ByteBuffer>.Continuation
        ) {
            self.state.withLock {
                $0.connectingSessionsCount -= 1
                $0.sessions[id] = SessionStreamsAndDatagrams(
                    incomingUnidirectionalStreamsContinuation: unidirectionalStreamsContinuation,
                    incomingBidirectionalStreamsContinuation: bidirectionalStreamsContinuation,
                    incomingDatagramsContinuation: datagramsContinuation
                )
            }
        }

        func yieldUnidirectionalStream(_ stream: NIOAsyncChannel<ByteBuffer, Never>, sessionID: QUICStreamID) {
            self.state.withLock { _ = $0.sessions[sessionID]?.incomingUnidirectionalStreamsContinuation.yield(stream) }
        }

        func yieldBidirectionalStream(_ stream: NIOAsyncChannel<ByteBuffer, ByteBuffer>, sessionID: QUICStreamID) {
            self.state.withLock { _ = $0.sessions[sessionID]?.incomingBidirectionalStreamsContinuation.yield(stream) }
        }

        func yieldDatagram(_ datagram: HTTP3Datagram) {
            self.state.withLock { _ = $0.sessions[datagram.streamID]?.incomingDatagramsContinuation.yield(datagram.payload) }
        }
    }
}
