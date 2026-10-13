/// Errors returned by the WebTransport client.
@nonexhaustive
public enum WebTransportError: Error, Sendable, Equatable {
    /// The server response to the HTTP CONNECT request was not 2xx.
    case serverRejectedSession
    /// The server SETTINGS do not enable WebTransport over HTTP/3.
    case serverDoesNotSupportWebTransport
    /// The client received a `WT-Protocol` value that was not included in its `WT-Available-Protocols` list
    case applicationProtocolNegotiationFailed
    /// Flow Control is not enabled, and a ``WebTransportSession`` is already open on the connection.
    ///
    /// Flow Control is enabled only if both the client and the server set at least one of the
    /// ``WebTransportConnection/Configuration/FlowControlSettings`` with a value other than "0".
    /// Without it, only one simultaneous session is allowed per connection.
    case simultaneousSessionsNotAllowed
    /// The limit of maximum allowed streams within the session has been reached.
    case streamLimitReached(maxStreams: Int)
    /// Writing the data would exceed the maximum amount of data allowed within the session.
    ///
    /// The write is rejected as a whole, so nothing was sent.
    /// It can be retried once the server raises the limit with a `WT_MAX_DATA` capsule.
    case dataLimitReached(maxData: Int)
    /// One of the ``WebTransportSession`` `increaseMax...` methods was called with a value that would exceed the allowed limit.
    ///
    /// Stream limits cannot exceed 2^60.
    case flowControlLimitExceeded
}
