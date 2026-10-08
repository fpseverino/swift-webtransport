public import NIOQUIC

extension WebTransportConnection {
    /// A configuration object that defines how to create a WebTransport connection.
    public struct Configuration: Sendable {
        /// Initial flow control limits.
        ///
        /// To establish more than one simultaneous WebTransport session,
        /// at least one of these limits must be set with any value other than "0".
        public struct FlowControlSettings: Sendable {
            /// The initial value for the unidirectional max stream limit.
            public var initialMaximumUnidirectionalStreams: Int

            /// The initial value for the bidirectional max stream limit.
            public var initialMaximumBidirectionalStreams: Int

            /// The initial value for the session data limit.
            public var initialMaximumData: Int

            /// Creates new Flow Control settings.
            ///
            /// - Parameters:
            ///   - initialMaximumUnidirectionalStreams: The initial value for the unidirectional max stream limit. The default value is "0".
            ///   - initialMaximumBidirectionalStreams: The initial value for the bidirectional max stream limit. The default value is "0".
            ///   - initialMaximumData: The initial value for the session data limit. The default value is "0".
            public init(
                initialMaximumUnidirectionalStreams: Int = 0,
                initialMaximumBidirectionalStreams: Int = 0,
                initialMaximumData: Int = 0
            ) {
                self.initialMaximumUnidirectionalStreams = initialMaximumUnidirectionalStreams
                self.initialMaximumBidirectionalStreams = initialMaximumBidirectionalStreams
                self.initialMaximumData = initialMaximumData
            }
        }

        /// Initial flow control limits.
        public var flowControlSettings: FlowControlSettings

        /// Information required to verify the server identity.
        public var verificationConfiguration: VerificationConfiguration

        /// Creates a new WebTransport connection configuration.
        ///
        /// - Parameters:
        ///   - flowControlSettings: The initial flow control limits.
        ///   - verificationConfiguration: Information required to verify the server identity.
        public init(
            flowControlSettings: FlowControlSettings = .init(),
            verificationConfiguration: VerificationConfiguration
        ) {
            self.flowControlSettings = flowControlSettings
            self.verificationConfiguration = verificationConfiguration
        }
    }
}
