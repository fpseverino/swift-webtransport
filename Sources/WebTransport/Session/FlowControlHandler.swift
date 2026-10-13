import Synchronization

final class FlowControlHandler: Sendable {
    let isEnabled: Bool

    private struct State {
        // The limits set by the server.
        var serverMaxUnidirectionalStreams: UInt64
        var serverMaxBidirectionalStreams: UInt64
        var serverMaxData: UInt64

        // The counts of streams opened and data sent by the client.
        var openedUnidirectionalStreams = 0
        var openedBidirectionalStreams = 0
        var dataSent: UInt64 = 0

        // The limits set by the client.
        var clientMaxUnidirectionalStreams: UInt64
        var clientMaxBidirectionalStreams: UInt64
        var clientMaxData: UInt64
    }

    private let state: Mutex<State>

    init(
        initialServerMaxUnidirectionalStreams: UInt64,
        initialServerMaxBidirectionalStreams: UInt64,
        initialServerMaxData: UInt64,
        initialClientMaxUnidirectionalStreams: UInt64,
        initialClientMaxBidirectionalStreams: UInt64,
        initialClientMaxData: UInt64
    ) {
        // If both endpoints take at least one of these actions, flow control is
        // enabled, and the limits described in the entirety of Section 5 apply.
        self.isEnabled =
            (initialServerMaxUnidirectionalStreams > 0 || initialServerMaxBidirectionalStreams > 0 || initialServerMaxData > 0)
            && (initialClientMaxUnidirectionalStreams > 0 || initialClientMaxBidirectionalStreams > 0 || initialClientMaxData > 0)
        self.state = .init(
            .init(
                serverMaxUnidirectionalStreams: initialServerMaxUnidirectionalStreams,
                serverMaxBidirectionalStreams: initialServerMaxBidirectionalStreams,
                serverMaxData: initialServerMaxData,
                clientMaxUnidirectionalStreams: initialClientMaxUnidirectionalStreams,
                clientMaxBidirectionalStreams: initialClientMaxBidirectionalStreams,
                clientMaxData: initialClientMaxData
            )
        )
    }

    // MARK: - Server Limits

    func updateServerMaxUnidirectionalStreams(_ newValue: UInt64) {
        guard self.isEnabled else { return }
        self.state.withLock {
            // TODO: throw WT_FLOW_CONTROL_ERROR
            precondition(newValue >= $0.serverMaxUnidirectionalStreams)
            $0.serverMaxUnidirectionalStreams = newValue
        }
    }

    func updateServerMaxBidirectionalStreams(_ newValue: UInt64) {
        guard self.isEnabled else { return }
        self.state.withLock {
            // TODO: throw WT_FLOW_CONTROL_ERROR
            precondition(newValue >= $0.serverMaxBidirectionalStreams)
            $0.serverMaxBidirectionalStreams = newValue
        }
    }

    func updateServerMaxData(_ newValue: UInt64) {
        guard self.isEnabled else { return }
        self.state.withLock {
            // TODO: throw WT_FLOW_CONTROL_ERROR
            precondition(newValue >= $0.serverMaxData)
            $0.serverMaxData = max($0.serverMaxData, newValue)
        }
    }

    /// Atomically consumes one unidirectional stream from the limit.
    ///
    /// - Returns: `nil` if the stream may be opened, otherwise the limit that was reached.
    func reserveUnidirectionalStream() -> UInt64? {
        guard self.isEnabled else { return nil }
        return self.state.withLock {
            guard $0.openedUnidirectionalStreams < $0.serverMaxUnidirectionalStreams else { return $0.serverMaxUnidirectionalStreams }
            $0.openedUnidirectionalStreams += 1
            return nil
        }
    }

    /// Atomically consumes one bidirectional stream from the limit.
    ///
    /// - Returns: `nil` if the stream may be opened, otherwise the limit that was reached.
    func reserveBidirectionalStream() -> UInt64? {
        guard self.isEnabled else { return nil }
        return self.state.withLock {
            guard $0.openedBidirectionalStreams < $0.serverMaxBidirectionalStreams else { return $0.serverMaxBidirectionalStreams }
            $0.openedBidirectionalStreams += 1
            return nil
        }
    }

    /// Atomically consumes `count` bytes from the session data limit.
    ///
    /// The whole amount is either consumed or rejected,
    /// so that the stream body bytes counted here always match what is actually written.
    ///
    /// - Returns: `nil` if the data may be sent, otherwise the limit that was reached.
    func reserveData(_ count: UInt64) -> UInt64? {
        guard self.isEnabled else { return nil }
        return self.state.withLock {
            let (total, overflow) = $0.dataSent.addingReportingOverflow(count)
            guard !overflow, total <= $0.serverMaxData else { return $0.serverMaxData }
            $0.dataSent = total
            return nil
        }
    }

    // MARK: - Client Limits

    /// Atomically raises a limit advertised to the server by the given value.
    ///
    /// - Parameters:
    ///   - keyPath: The key path to the limit to be increased.
    ///   - valueToAdd: The value to add to the current limit.
    ///   - maximum: The maximum allowed value for the limit.
    ///
    /// - Returns: The new limit if it must be sent to the server,
    ///     `nil` if it equals the current one or Flow Control is not enabled,
    ///     in which case the server would ignore the capsule.
    ///
    /// - Throws: ``WebTransportError/flowControlLimitExceeded`` if the limit exceeds `maximum`.
    private func increase(
        _ keyPath: WritableKeyPath<State, UInt64>,
        by valueToAdd: UInt64,
        maximum: UInt64
    ) throws -> UInt64? {
        guard self.isEnabled, valueToAdd > 0 else { return nil }
        return try self.state.withLock {
            let currentValue = $0[keyPath: keyPath]
            let (newValue, overflow) = currentValue.addingReportingOverflow(valueToAdd)
            guard !overflow, newValue <= maximum else {
                throw WebTransportError.flowControlLimitExceeded
            }
            $0[keyPath: keyPath] = newValue
            return newValue
        }
    }

    private static let maxStreamsLimit: UInt64 = 1 << 60

    /// Raises the limit for the maximum number of unidirectional streams advertised to the server by the given value.
    ///
    /// - Parameter valueToAdd: The value to add to the current limit.
    /// - Throws: ``WebTransportError/flowControlLimitExceeded`` if the new limit exceeds 2^60.
    /// - Returns: The new limit if it must be sent to the server, `nil` if it equals the current one or Flow Control is not enabled.
    func increaseClientMaxUnidirectionalStreams(by valueToAdd: UInt64) throws -> UInt64? {
        try self.increase(\.clientMaxUnidirectionalStreams, by: valueToAdd, maximum: Self.maxStreamsLimit)
    }

    /// Raises the limit for the maximum number of bidirectional streams advertised to the server by the given value.
    ///
    /// - Parameter valueToAdd: The value to add to the current limit.
    /// - Throws: ``WebTransportError/flowControlLimitExceeded`` if the new limit exceeds 2^60.
    /// - Returns: The new limit if it must be sent to the server, `nil` if it equals the current one or Flow Control is not enabled.
    func increaseClientMaxBidirectionalStreams(by valueToAdd: UInt64) throws -> UInt64? {
        try self.increase(\.clientMaxBidirectionalStreams, by: valueToAdd, maximum: Self.maxStreamsLimit)
    }

    /// Raises the limit for the maximum amount of data advertised to the server by the given value.
    ///
    /// - Parameter valueToAdd: The value to add to the current limit.
    /// - Throws: ``WebTransportError/flowControlLimitExceeded`` if the new limit exceeds the maximum allowed value.
    /// - Returns: The new limit if it must be sent to the server, `nil` if it equals the current one or Flow Control is not enabled.
    func increaseClientMaxData(by valueToAdd: UInt64) throws -> UInt64? {
        try self.increase(\.clientMaxData, by: valueToAdd, maximum: .max)
    }
}
