public import NIOCore
import NIOHTTPTypes

extension WebTransportSession {
    /// An asynchronous sequence of unidirectional streams opened by the server.
    /// Each one can be used to read data from the server.
    public struct IncomingUnidirectionalStreams: AsyncSequence, Sendable {
        public typealias Element = IncomingUnidirectionalStream

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

        public struct AsyncIterator: AsyncIteratorProtocol, ~Sendable {
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

    /// A unidirectional stream opened by the server.
    /// It can be used to read data from the server.
    public struct IncomingUnidirectionalStream: Sendable {
        let channel: NIOAsyncChannel<ByteBuffer, Never>

        /// Provides access to the incoming unidirectional stream.
        ///
        /// - Parameter operation: The closure where read operations are performed.
        ///
        /// - Returns: The value returned by the `operation` closure.
        public func withStream<Value>(operation: (WebTransportStreamInboundStream) async throws -> Value) async throws -> Value {
            try await self.channel.executeThenClose { inbound in
                try await operation(.init(backing: inbound))
            }
        }
    }
}

extension WebTransportSession {
    /// An asynchronous sequence of bidirectional streams opened by the server.
    /// Each one can be used to read data from the server and write data back to it.
    public struct IncomingBidirectionalStreams: AsyncSequence, Sendable {
        public typealias Element = IncomingBidirectionalStream

        @usableFromInline
        typealias BaseAsyncSequence = AsyncStream<NIOAsyncChannel<ByteBuffer, ByteBuffer>>
        typealias Continuation = BaseAsyncSequence.Continuation

        @usableFromInline
        let base: AsyncStream<NIOAsyncChannel<ByteBuffer, ByteBuffer>>

        let flowControlHandler: FlowControlHandler
        let connectStreamWriter: NIOAsyncChannelOutboundWriter<HTTPRequestPart>

        static func makeStream(
            flowControlHandler: FlowControlHandler,
            connectStreamWriter: NIOAsyncChannelOutboundWriter<HTTPRequestPart>
        ) -> (Self, Self.Continuation) {
            let (stream, continuation) = BaseAsyncSequence.makeStream()
            return (.init(base: stream, flowControlHandler: flowControlHandler, connectStreamWriter: connectStreamWriter), continuation)
        }

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(
                base: self.base.makeAsyncIterator(),
                flowControlHandler: self.flowControlHandler,
                connectStreamWriter: self.connectStreamWriter
            )
        }

        public struct AsyncIterator: AsyncIteratorProtocol, ~Sendable {
            @usableFromInline
            var base: BaseAsyncSequence.AsyncIterator

            let flowControlHandler: FlowControlHandler
            let connectStreamWriter: NIOAsyncChannelOutboundWriter<HTTPRequestPart>

            @concurrent
            public mutating func next() async -> Element? {
                guard let next = await self.base.next() else { return nil }
                return .init(channel: next, flowControlHandler: self.flowControlHandler, connectStreamWriter: self.connectStreamWriter)
            }

            public mutating func next(isolation actor: isolated (any Actor)?) async -> Element? {
                guard let next = await self.base.next(isolation: actor) else { return nil }
                return .init(channel: next, flowControlHandler: self.flowControlHandler, connectStreamWriter: self.connectStreamWriter)
            }
        }
    }

    /// A bidirectional stream opened by the server.
    /// It can be used to read data from the server and send data back to it.
    public struct IncomingBidirectionalStream: Sendable {
        let channel: NIOAsyncChannel<ByteBuffer, ByteBuffer>

        let flowControlHandler: FlowControlHandler
        let connectStreamWriter: NIOAsyncChannelOutboundWriter<HTTPRequestPart>

        /// Provides access to the incoming bidirectional stream.
        ///
        /// - Parameter operation: The closure where read and write operations are performed.
        ///
        /// - Returns: The value returned by the `operation` closure.
        public func withStream<Value>(
            operation: (WebTransportStreamInboundStream, consuming WebTransportStreamOutboundWriter) async throws -> Value
        ) async throws -> Value {
            try await self.channel.executeThenClose { inbound, outbound in
                try await operation(
                    .init(backing: inbound),
                    .init(
                        writer: outbound,
                        flowControlHandler: self.flowControlHandler,
                        connectStreamWriter: self.connectStreamWriter
                    )
                )
            }
        }
    }
}

extension WebTransportSession {
    /// An asynchronous sequence of incoming datagrams sent from the server.
    public struct IncomingDatagrams: AsyncSequence, Sendable {
        public typealias Element = ByteBuffer

        @usableFromInline
        let backing: AsyncStream<ByteBuffer>

        @inlinable
        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(backing: self.backing.makeAsyncIterator())
        }

        public struct AsyncIterator: AsyncIteratorProtocol, ~Sendable {
            @usableFromInline
            var backing: AsyncStream<ByteBuffer>.AsyncIterator

            @inlinable
            init(backing: AsyncStream<ByteBuffer>.AsyncIterator) {
                self.backing = backing
            }

            @inlinable
            @concurrent
            public mutating func next() async -> Element? {
                await self.backing.next()
            }

            @inlinable
            public mutating func next(isolation actor: isolated (any Actor)?) async -> Element? {
                await self.backing.next(isolation: actor)
            }
        }
    }
}
