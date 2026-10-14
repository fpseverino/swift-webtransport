import NIOCore
import NIOHTTP3
import NIOQUICHelpers
import Synchronization

extension WebTransportConnection {
    /// Keeps track of all sessions opened on a WebTransport connection,
    /// enforces Flow Control for opening sessions,
    /// and multiplexes incoming streams and datagrams on the connection to the corresponding sessions.
    final class IncomingStreamsAndDatagrams: Sendable {
        private struct SessionStreamsAndDatagrams {
            let incomingUnidirectionalStreamsContinuation: WebTransportSession.IncomingUnidirectionalStreams.Continuation
            let incomingBidirectionalStreamsContinuation: WebTransportSession.IncomingBidirectionalStreams.Continuation
            let incomingDatagramsContinuation: AsyncStream<ByteBuffer>.Continuation
        }

        private struct State {
            /// Number of sessions whose CONNECT request has been started but not yet accepted by the server.
            var connectingSessionsCount = 0
            /// Maps Session IDs to their corresponding incoming streams and datagrams continuations.
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

        /// Releases a session slot.
        /// 
        /// - Parameter id: The ID of the session to remove. Pass `nil` if the session was never activated.
        func removeSession(id: QUICStreamID?) {
            self.state.withLock {
                if let id, $0.sessions.removeValue(forKey: id) != nil { return }
                $0.connectingSessionsCount -= 1
            }
        }

        /// Adds a session (that was previously reserved with ``reserveSession(allowSimultaneous:)``)
        /// with the specified continuations for incoming streams and datagrams.
        ///
        /// - Parameters:
        ///   - id: The ID of the session to add (that was previously reserved).
        ///   - unidirectionalStreamsContinuation: The continuation for incoming unidirectional streams.
        ///   - bidirectionalStreamsContinuation: The continuation for incoming bidirectional streams.
        ///   - datagramsContinuation: The continuation for incoming datagrams.
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

        /// Yields an incoming unidirectional stream to the corresponding session.
        ///
        /// - Parameters:
        ///   - stream: The incoming unidirectional stream.
        ///   - sessionID: The ID of the session to which the stream belongs.
        func yieldUnidirectionalStream(_ stream: NIOAsyncChannel<ByteBuffer, Never>, sessionID: QUICStreamID) {
            self.state.withLock { _ = $0.sessions[sessionID]?.incomingUnidirectionalStreamsContinuation.yield(stream) }
        }

        /// Yields an incoming bidirectional stream to the corresponding session.
        ///
        /// - Parameters:
        ///   - stream: The incoming bidirectional stream.
        ///   - sessionID: The ID of the session to which the stream belongs.
        func yieldBidirectionalStream(_ stream: NIOAsyncChannel<ByteBuffer, ByteBuffer>, sessionID: QUICStreamID) {
            self.state.withLock { _ = $0.sessions[sessionID]?.incomingBidirectionalStreamsContinuation.yield(stream) }
        }

        /// Yields an incoming datagram to the corresponding session.
        ///
        /// The Session ID is inferred from the datagram's stream ID.
        ///
        /// - Parameters:
        ///   - datagram: The incoming datagram.
        func yieldDatagram(_ datagram: HTTP3Datagram) {
            self.state.withLock { _ = $0.sessions[datagram.streamID]?.incomingDatagramsContinuation.yield(datagram.payload) }
        }
    }
}
