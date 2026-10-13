import HTTPTypes
public import NIOCore
import NIOHTTPTypes

/// Writes data to a WebTransport stream, enforcing the session-level data limit set by the server.
///
/// The limit is shared by all the streams of the session and is raised by the server through `WT_MAX_DATA` capsules.
/// If a write would exceed it, a `WT_DATA_BLOCKED` capsule is sent to the server
/// and ``WebTransportError/dataLimitReached(maxData:)`` is thrown without sending anything.
public struct WebTransportStreamOutboundWriter: Sendable, ~Copyable {
    private let writer: NIOAsyncChannelOutboundWriter<ByteBuffer>
    private let flowControlHandler: FlowControlHandler
    /// Used to write capsules to the CONNECT stream that established the WebTransport session.
    private let connectStreamWriter: NIOAsyncChannelOutboundWriter<HTTPRequestPart>

    init(
        writer: NIOAsyncChannelOutboundWriter<ByteBuffer>,
        flowControlHandler: FlowControlHandler,
        connectStreamWriter: NIOAsyncChannelOutboundWriter<HTTPRequestPart>
    ) {
        self.writer = writer
        self.flowControlHandler = flowControlHandler
        self.connectStreamWriter = connectStreamWriter
    }

    /// Write a chunk of data to the stream.
    ///
    /// - Parameter data: The data to write.
    ///
    /// - Throws: ``WebTransportError/dataLimitReached(maxData:)`` if the write would exceed the session data limit.
    public func write(_ data: ByteBuffer) async throws {
        try await self.reserve(UInt64(data.readableBytes))
        try await self.writer.write(data)
    }

    /// Write a sequence of chunks of data to the stream.
    ///
    /// The sequence is accepted or rejected as a whole.
    ///
    /// - Parameter sequence: The data to write.
    ///
    /// - Throws: ``WebTransportError/dataLimitReached(maxData:)`` if the write would exceed the session data limit.
    // TODO: Check if is necessary that the sequence conforms to Sendable
    public func write<Writes: Sequence & Sendable>(contentsOf sequence: Writes) async throws where Writes.Element == ByteBuffer {
        try await self.reserve(sequence.reduce(0) { $0 + UInt64($1.readableBytes) })
        try await self.writer.write(contentsOf: sequence)
    }

    /// Write an asynchronous sequence of chunks of data to the stream.
    ///
    /// If the writing of any chunk that comes in asynchronously would exceed the session data limit, an error is thrown.
    ///
    /// - Parameter sequence: The asynchronous sequence of data chunks to write.
    ///
    /// - Throws: ``WebTransportError/dataLimitReached(maxData:)`` if writing any chunk that comes in asynchronously would exceed the limit.
    public func write<Writes: AsyncSequence>(contentsOf sequence: Writes) async throws where Writes.Element == ByteBuffer {
        for try await data in sequence {
            try await self.write(data)
        }
    }

    /// Finish the writer, closing the outbound side of the stream and triggering a half closure of the channel.
    public func finish() {
        self.writer.finish()
    }

    /// Reserve space for a write operation, enforcing the session-level data limit.
    ///
    /// - Parameter count: The number of bytes to reserve.
    ///
    /// - Throws: ``WebTransportError/dataLimitReached(maxData:)`` if the reservation would exceed the session data limit.
    private func reserve(_ count: UInt64) async throws {
        if let maxData = self.flowControlHandler.reserveData(count) {
            try await self.connectStreamWriter.write(.body(.wtDataBlockedCapsule(maxData: maxData)))
            throw WebTransportError.dataLimitReached(maxData: numericCast(maxData))
        }
    }
}
