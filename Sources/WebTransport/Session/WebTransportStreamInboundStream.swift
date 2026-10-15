public import NIOCore

/// The inbound message asynchronous sequence of a WebTransport stream.
///
/// This is a unicast `AsyncSequence` that allows a single iterator to be created.
public struct WebTransportStreamInboundStream: AsyncSequence, Sendable {
    public typealias Element = ByteBuffer

    @usableFromInline
    let backing: NIOAsyncChannelInboundStream<ByteBuffer>

    @inlinable
    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(backing: self.backing.makeAsyncIterator())
    }

    public struct AsyncIterator: AsyncIteratorProtocol, ~Sendable {
        @usableFromInline
        var backing: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator

        @inlinable
        init(backing: NIOAsyncChannelInboundStream<ByteBuffer>.AsyncIterator) {
            self.backing = backing
        }

        @inlinable
        @concurrent
        public mutating func next() async throws -> Element? {
            try await self.backing.next()
        }

        @inlinable
        public mutating func next(isolation actor: isolated (any Actor)?) async throws -> Element? {
            try await self.backing.next(isolation: actor)
        }
    }
}
