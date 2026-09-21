public import NIOCore

extension WebTransportConnection {
    public struct IncomingBidirectionalStreams: AsyncSequence, Sendable {
        public typealias Element = NIOAsyncChannel<ByteBuffer, ByteBuffer>

        private let stream: AsyncStream<Element>

        init(stream: AsyncStream<Element>) {
            self.stream = stream
        }

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(iterator: self.stream.makeAsyncIterator())
        }

        public struct AsyncIterator: AsyncIteratorProtocol {
            private var iterator: AsyncStream<Element>.Iterator

            init(iterator: AsyncStream<Element>.Iterator) {
                self.iterator = iterator
            }

            @concurrent
            public mutating func next() async -> Element? {
                await self.iterator.next()
            }

            public mutating func next(isolation actor: isolated (any Actor)?) async throws -> Element? {
                await self.iterator.next(isolation: actor)
            }
        }
    }
}

@available(*, unavailable)
extension WebTransportConnection.IncomingBidirectionalStreams.AsyncIterator: Sendable {}
