extension WebTransportSession {
    /// A configuration object that defines how to create a WebTransport session.
    public struct Configuration: Sendable {
        /// The list of supported application protocols.
        public var applicationProtocols: [String]

        /// The URL path to connect to on the server.
        public var urlPath: String

        /// Creates a new WebTransport session configuration.
        ///
        /// - Parameters:
        ///   - applicationProtocols: The list of supported application protocols.
        ///   - urlPath: The URL path to connect to on the server. Defaults to `"/"`.
        public init(
            applicationProtocols: [String] = [],
            urlPath: String = "/"
        ) {
            self.applicationProtocols = applicationProtocols
            self.urlPath = urlPath
        }
    }
}
