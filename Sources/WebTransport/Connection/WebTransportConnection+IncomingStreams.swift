public import NIOCore

extension WebTransportConnection {
    public struct IncomingBidirectionalStreams: AsyncSequence, Sendable {
        public typealias Element = NIOAsyncChannel<ByteBuffer, ByteBuffer>

        @usableFromInline
        typealias BaseAsyncSequence = AsyncStream<Element>
        typealias Continuation = BaseAsyncSequence.Continuation

        @usableFromInline
        let base: AsyncStream<Element>

        static func makeStream() -> (Self, Self.Continuation) {
            let (stream, continuation) = BaseAsyncSequence.makeStream()
            return (.init(base: stream), continuation)
        }

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(base: self.base.makeAsyncIterator())
        }

        public struct AsyncIterator: AsyncIteratorProtocol {
            @usableFromInline
            var base: BaseAsyncSequence.AsyncIterator

            @concurrent
            @inlinable
            public mutating func next() async -> Element? {
                await self.base.next()
            }

            @inlinable
            public mutating func next(isolation actor: isolated (any Actor)?) async -> Element? {
                await self.base.next(isolation: actor)
            }
        }
    }
}

@available(*, unavailable)
extension WebTransportConnection.IncomingBidirectionalStreams.AsyncIterator: Sendable {}
