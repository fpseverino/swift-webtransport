/// Errors returned by a WebTransport connection.
public struct WebTransportError: Error, Sendable, Equatable {
    public struct ErrorType: Sendable, Hashable, CustomStringConvertible, Equatable {
        enum Base: String, Sendable, Equatable {
            /// The server response to the HTTP CONNECT request was not 2xx.
            case serverRejectedSession
        }

        let base: Base

        private init(_ base: Base) {
            self.base = base
        }

        /// The server response to the HTTP CONNECT request was not 2xx.
        public static let serverRejectedSession = ErrorType(.serverRejectedSession)

        public var description: String { self.base.rawValue }
    }

    private struct Backing: Sendable, Equatable {
        fileprivate let errorType: ErrorType

        init(errorType: ErrorType) {
            self.errorType = errorType
        }

        static func == (lhs: Backing, rhs: Backing) -> Bool {
            lhs.errorType == rhs.errorType
        }
    }

    private let backing: Backing

    public var errorType: ErrorType { self.backing.errorType }

    private init(backing: Backing) {
        self.backing = backing
    }

    private init(errorType: ErrorType) {
        self.backing = .init(errorType: errorType)
    }

    /// The server response to the HTTP CONNECT request was not 2xx.
    public static let serverRejectedSession = WebTransportError(errorType: .serverRejectedSession)

    public static func == (lhs: WebTransportError, rhs: WebTransportError) -> Bool {
        lhs.backing == rhs.backing
    }
}

extension WebTransportError: CustomStringConvertible {
    public var description: String {
        "WebTransportError(errorType: \(self.errorType))"
    }
}
