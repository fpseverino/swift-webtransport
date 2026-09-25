public import NIOQUIC

/// A configuration object that defines how to connect to a WebTransport server.
public struct WebTransportConnectionConfiguration: Sendable {
    /// Information required to verify the server identity.
    public var verificationConfiguration: VerificationConfiguration

    /// The list of supported application protocols.
    public var applicationProtocols: [String]

    /// The URL path to connect to on the server.
    public var urlPath: String

    /// Creates a new WebTransport connection configuration.
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
