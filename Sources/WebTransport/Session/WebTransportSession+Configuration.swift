public import NIOQUIC

extension WebTransportSession {
    /// A configuration object that defines how to create a WebTransport session.
    public struct Configuration: Sendable {
        /// Information required to verify the server identity.
        public var verificationConfiguration: VerificationConfiguration

        /// The list of supported application protocols.
        public var applicationProtocols: [String]

        /// The URL path to connect to on the server.
        public var urlPath: String

        /// Creates a new WebTransport session configuration.
        ///
        /// - Parameters:
        ///   - verificationConfiguration: Information required to verify the server identity.
        ///   - applicationProtocols: The list of supported application protocols.
        ///   - urlPath: The URL path to connect to on the server. Defaults to `"/"`.
        public init(
            verificationConfiguration: VerificationConfiguration,
            applicationProtocols: [String] = [],
            urlPath: String = "/"
        ) {
            self.verificationConfiguration = verificationConfiguration
            self.applicationProtocols = applicationProtocols
            self.urlPath = urlPath
        }
    }
}
