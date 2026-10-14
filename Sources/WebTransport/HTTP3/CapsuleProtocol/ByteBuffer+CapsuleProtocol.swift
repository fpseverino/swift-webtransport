import NIOCore

extension ByteBuffer {
    /// The `WT_DRAIN_SESSION` capsule.
    static let wtDrainSessionCapsule: ByteBuffer = {
        var buffer = ByteBuffer()
        buffer.writeEncodedInteger(0x78ae, strategy: .quic)
        buffer.writeEncodedInteger(0, strategy: .quic)
        return buffer
    }()

    /// Creates a capsule with the given type and value.
    /// - Parameters:
    ///   - type: The type of the capsule.
    ///   - value: The value to be encoded in the capsule.
    /// - Returns: A `ByteBuffer` containing the encoded capsule.
    private static func capsule(type: UInt64, value: UInt64) -> ByteBuffer {
        var buffer = ByteBuffer()
        buffer.writeEncodedInteger(type, strategy: .quic)
        buffer.writeLengthPrefixed(strategy: .quic) {
            $0.writeEncodedInteger(value, strategy: .quic)
        }
        return buffer
    }

    /// The `WT_MAX_STREAMS` capsule for bidirectional streams.
    ///
    /// - Parameter maxStreams: A count of the cumulative number of bidirectional streams
    ///     that can be opened over the lifetime of the session. This value cannot exceed 2^60.
    static func wtMaxBiStreamsCapsule(maxStreams: UInt64) -> ByteBuffer {
        Self.capsule(type: 0x190B_4D3F, value: maxStreams)
    }

    /// The `WT_MAX_STREAMS` capsule for unidirectional streams.
    ///
    /// - Parameter maxStreams: A count of the cumulative number of unidirectional streams
    ///     that can be opened over the lifetime of the session. This value cannot exceed 2^60.
    static func wtMaxUniStreamsCapsule(maxStreams: UInt64) -> ByteBuffer {
        Self.capsule(type: 0x190B_4D40, value: maxStreams)
    }

    /// The `WT_MAX_DATA` capsule.
    ///
    /// - Parameter maxData: The maximum amount of data that can be sent on the entire session, in units of bytes.
    static func wtMaxDataCapsule(maxData: UInt64) -> ByteBuffer {
        Self.capsule(type: 0x190B_4D3D, value: maxData)
    }

    /// The `WT_STREAMS_BLOCKED` capsule for bidirectional streams.
    ///
    /// - Parameter maxStreams: The maximum number of bidirectional streams allowed at the time the capsule was sent.
    static func wtBiStreamsBlockedCapsule(maxStreams: UInt64) -> ByteBuffer {
        Self.capsule(type: 0x190B_4D43, value: maxStreams)
    }

    /// The `WT_STREAMS_BLOCKED` capsule for unidirectional streams.
    ///
    /// - Parameter maxStreams: The maximum number of unidirectional streams allowed at the time the capsule was sent.
    static func wtUniStreamsBlockedCapsule(maxStreams: UInt64) -> ByteBuffer {
        Self.capsule(type: 0x190B_4D44, value: maxStreams)
    }

    /// The `WT_DATA_BLOCKED` capsule.
    ///
    /// - Parameter maxData: The session-level data limit at which blocking occurred.
    static func wtDataBlockedCapsule(maxData: UInt64) -> ByteBuffer {
        Self.capsule(type: 0x190B_4D41, value: maxData)
    }
}
