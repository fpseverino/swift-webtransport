import HTTPTypes
import NIOCore
import NIOHTTP3
import NIOHTTPTypes
import NIOQUIC
import Testing
import WebTransport

@Suite("WebTransportSession Tests")
struct WebTransportSessionTests {
    @Test("Application Protocol Negotiation", arguments: TestWTServer.allCases)
    func applicationProtocolNegotiation(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test", "webtransport-test-2"], urlPath: "/webtransport")
        ) { session in
            switch server {
            case .go:
                #expect(session.applicationProtocol == "webtransport-test")
            case .rust:
                // The Rust server does not negotiate the application protocol
                #expect(session.applicationProtocol == nil)
            }
        }

        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)),
            sessionConfiguration: .init(urlPath: "/webtransport")
        ) { session in
            #expect(session.applicationProtocol == nil)
        }

        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)),
            sessionConfiguration: .init(applicationProtocols: ["unknown"], urlPath: "/webtransport")
        ) { session in
            #expect(session.applicationProtocol == nil)
        }
    }

    @Test("Open Streams", arguments: TestWTServer.allCases)
    func openStreams(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test", "webtransport-test-2"], urlPath: "/webtransport")
        ) { session in
            try await withThrowingTaskGroup { group in
                // Write on a unidirectional stream
                group.addTask {
                    try await session.withUnidirectionalStream { outbound in
                        try await outbound.write(ByteBuffer(string: "Hello, WebTransport!"))
                        outbound.finish()
                    }
                }

                // Write and read on an "echo" bidirectional stream
                group.addTask {
                    try await session.withBidirectionalStream { inbound, outbound in
                        let payload = ByteBuffer(string: "Hello, World!")
                        try await outbound.write(payload)
                        var inboundStreamIterator = inbound.makeAsyncIterator()
                        #expect(try await inboundStreamIterator.next() == payload)
                    }
                }

                // Write an `AsyncSequence` of messages on a bidirectional stream
                group.addTask {
                    try await session.withBidirectionalStream { inbound, outbound in
                        let (messagesStream, messageStreamContinuation) = AsyncStream.makeStream(of: ByteBuffer.self)
                        try await withThrowingTaskGroup { streamGroup in
                            streamGroup.addTask {
                                for try await message in inbound {
                                    guard message.readableBytes < 16 else { break }
                                    messageStreamContinuation.yield(ByteBuffer(bytes: message.readableBytesView + message.readableBytesView))
                                }
                                messageStreamContinuation.finish()
                            }

                            streamGroup.addTask {
                                try await outbound.write(ByteBuffer(string: "a"))
                                try await outbound.write(contentsOf: messagesStream)
                            }

                            try await streamGroup.waitForAll()
                        }
                    }
                }

                // Write a `Sequence` of messages on a unidirectional stream
                group.addTask {
                    try await session.withUnidirectionalStream { outbound in
                        let messages: [ByteBuffer] = [
                            .init(string: "Hello"),
                            .init(string: "Ciao"),
                            .init(string: "Hola"),
                        ]
                        try await outbound.write(contentsOf: messages)
                    }
                }

                try await group.waitForAll()
            }
        }
    }

    @Test("Incoming Streams", arguments: TestWTServer.allCases)
    func incomingStreams(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test", "webtransport-test-2"], urlPath: "/webtransport")
        ) { session in
            try await withThrowingTaskGroup { group in
                group.addTask {
                    try await session.withUnidirectionalStream { outbound in
                        try await outbound.write(ByteBuffer(string: "open"))
                    }

                    for await stream in session.incomingUnidirectionalStreams {
                        try await stream.executeThenClose { inbound in
                            for try await message in inbound {
                                #expect(message == ByteBuffer(string: "opened"))
                                break
                            }
                        }
                        break
                    }
                }

                group.addTask {
                    try await session.withBidirectionalStream { inbound, outbound in
                        try await outbound.write(ByteBuffer(string: "open"))
                    }

                    for await stream in session.incomingBidirectionalStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            var iterator = inbound.makeAsyncIterator()
                            #expect(try await iterator.next() == ByteBuffer(string: "opened"))
                            for message in ["Hello again!", "Hi, mom!", "Bye bye!"] {
                                let payload = ByteBuffer(string: message)
                                try await outbound.write(payload)
                                #expect(try await iterator.next() == payload)
                            }
                        }
                        break
                    }
                }

                try await group.waitForAll()
            }
        }
    }

    @Test("Datagrams", arguments: TestWTServer.allCases)
    func datagrams(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test", "webtransport-test-2"], urlPath: "/webtransport")
        ) { session in
            let payload = ByteBuffer(string: "Hello, datagrams!")

            try await session.sendDatagram(payload)

            for await datagram in session.incomingDatagrams {
                #expect(datagram == payload)
                break
            }
        }
    }

    @Test("Draining", arguments: TestWTServer.allCases)
    func draining(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test", "webtransport-test-2"], urlPath: "/webtransport")
        ) { session in
            try await session.drain()
            try await withThrowingTaskGroup { group in
                group.addTask { try await session.draining }
                // No supported test server supports session draining
                try await Task.sleep(for: .seconds(1))
                group.cancelAll()
            }
        }
    }

    @Test("Multiple Sessions", arguments: TestWTServer.allServersSupportingFlowControl)
    func multipleSessions(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            configuration: .init(
                flowControlSettings: .init(
                    initialMaximumUnidirectionalStreams: 100,
                    initialMaximumBidirectionalStreams: 100,
                    initialMaximumData: 1 << 20
                ),
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
            )
        ) { connection in
            try await withThrowingTaskGroup { group in
                group.addTask {
                    try await connection.withSession(
                        configuration: .init(applicationProtocols: ["webtransport-test"], urlPath: "/webtransport")
                    ) { session in
                        try await session.sendDatagram(ByteBuffer(string: "Hello from the first session!"))
                        for await datagram in session.incomingDatagrams {
                            #expect(datagram == ByteBuffer(string: "Hello from the first session!"))
                            break
                        }
                    }
                }

                group.addTask {
                    try await connection.withSession(
                        configuration: .init(applicationProtocols: ["webtransport-test-2"], urlPath: "/webtransport")
                    ) { session in
                        try await session.sendDatagram(ByteBuffer(string: "Hello from the second session!"))
                        for await datagram in session.incomingDatagrams {
                            #expect(datagram == ByteBuffer(string: "Hello from the second session!"))
                            break
                        }
                    }
                }

                try await group.waitForAll()
            }
        }
    }

    @Test("Maximum Streams", arguments: TestWTServer.allServersSupportingFlowControl)
    func maximumStreams(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            // Servers only enable session Flow Control (and send `WT_MAX_*` capsules) if clients advertise non-zero limits too.
            connectionConfiguration: .init(
                flowControlSettings: .init(
                    initialMaximumUnidirectionalStreams: 100,
                    initialMaximumBidirectionalStreams: 100,
                    initialMaximumData: 1 << 20
                ),
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
            ),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test"], urlPath: "/webtransport")
        ) { session in
            let (openedBiStreams, openedBiStreamsContinuation) = AsyncStream.makeStream(of: Int.self)
            let (openedUniStreams, openedUniStreamsContinuation) = AsyncStream.makeStream(of: Int.self)

            try await withThrowingTaskGroup { group in
                // Supported test servers are configured to allow a maximum of 10 bidirectional and 10 unidirectional streams per session.
                for count in 1...10 {
                    group.addTask {
                        try await session.withBidirectionalStream { _, _ in
                            openedBiStreamsContinuation.yield(count)
                            try await Task.sleep(for: .seconds(10))
                        }
                    }

                    group.addTask {
                        try await session.withUnidirectionalStream { _ in
                            openedUniStreamsContinuation.yield(count)
                            try await Task.sleep(for: .seconds(10))
                        }
                    }
                }

                for await count in openedBiStreams { if count >= 10 { break } }

                await #expect(throws: WebTransportError.streamLimitReached(maxStreams: 10)) {
                    try await session.withBidirectionalStream { _, _ in }
                }

                for await count in openedUniStreams { if count >= 10 { break } }

                await #expect(throws: WebTransportError.streamLimitReached(maxStreams: 10)) {
                    try await session.withUnidirectionalStream { _ in }
                }

                // Closing the streams makes the server release credit and send `WT_MAX_STREAMS` capsules,
                // which raise the limits so that new streams can be opened again.
                group.cancelAll()
                // Give the server some time to process the stream closures and send `WT_MAX_STREAMS` capsules.
                try await Task.sleep(for: .seconds(1))

                await #expect(throws: Never.self) { try await session.withBidirectionalStream { _, _ in } }
                await #expect(throws: Never.self) { try await session.withUnidirectionalStream { _ in } }
            }
        }
    }

    @Test("Maximum Data", arguments: TestWTServer.allServersSupportingFlowControl)
    func maximumData(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            // Servers only enable session Flow Control (and send `WT_MAX_*` capsules) if clients advertise non-zero limits too.
            configuration: .init(
                flowControlSettings: .init(
                    initialMaximumUnidirectionalStreams: 100,
                    initialMaximumBidirectionalStreams: 100,
                    initialMaximumData: 1 << 20
                ),
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
            )
        ) { connection in
            // Supported test servers are configured to allow a maximum of 1 KiB of stream data per session.
            let maxData = 1 << 10
            let chunk = ByteBuffer(repeating: UInt8(ascii: "a"), count: 128)

            try await withThrowingTaskGroup { group in
                group.addTask {
                    try await connection.withSession(configuration: .init(urlPath: "/webtransport")) { session in
                        try await session.withUnidirectionalStream { outbound in
                            // Nothing has been consumed by the server yet, so it cannot have raised the limit.
                            await #expect(throws: WebTransportError.dataLimitReached(maxData: maxData)) {
                                try await outbound.write(ByteBuffer(repeating: UInt8(ascii: "a"), count: maxData + 1))
                            }

                            for _ in 0..<(maxData / chunk.readableBytes) {
                                try await outbound.write(chunk)
                            }

                            // The whole limit is now consumed
                            await #expect(throws: WebTransportError.dataLimitReached(maxData: maxData)) {
                                try await outbound.write(chunk)
                            }

                            // Give the server some time to read the data and raise the limit with a `WT_MAX_DATA` capsule.
                            try await Task.sleep(for: .seconds(1))

                            await #expect(throws: Never.self) {
                                try await outbound.write(chunk)
                            }
                        }
                    }
                }

                group.addTask {
                    try await connection.withSession(configuration: .init(urlPath: "/webtransport")) { session in
                        try await session.withBidirectionalStream { _, outbound in
                            // Nothing has been consumed by the server yet, so it cannot have raised the limit.
                            await #expect(throws: WebTransportError.dataLimitReached(maxData: maxData)) {
                                try await outbound.write(ByteBuffer(repeating: UInt8(ascii: "a"), count: maxData + 1))
                            }

                            for _ in 0..<(maxData / chunk.readableBytes) {
                                try await outbound.write(chunk)
                            }

                            // The whole limit is now consumed
                            await #expect(throws: WebTransportError.dataLimitReached(maxData: maxData)) {
                                try await outbound.write(chunk)
                            }

                            // Give the server some time to read the data and raise the limit with a `WT_MAX_DATA` capsule.
                            try await Task.sleep(for: .seconds(1))

                            await #expect(throws: Never.self) {
                                try await outbound.write(chunk)
                            }
                        }
                    }
                }

                try await group.waitForAll()
            }
        }
    }

    @Test("Multiple Sessions require Flow Control", arguments: TestWTServer.allServersSupportingFlowControl)
    func multipleSessionsRequireFlowControl(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            // The client does not enable Flow Control, so only one simultaneous session is allowed.
            configuration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath))
        ) { connection in
            let configuration = WebTransportSession.Configuration(applicationProtocols: ["webtransport-test"], urlPath: "/webtransport")

            try await connection.withSession(configuration: configuration) { _ in
                _ = await #expect(throws: WebTransportError.simultaneousSessionsNotAllowed) {
                    try await connection.withSession(configuration: configuration) { _ in }
                }
            }

            // Once the first session is over, a new one can be established.
            await #expect(throws: Never.self) {
                try await connection.withSession(configuration: configuration) { _ in }
            }
        }
    }

    @Test("No Limits without Flow Control", arguments: TestWTServer.allServersSupportingFlowControl)
    func noLimitsWithoutFlowControl(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            // The client does not enable Flow Control, so the limits advertised by the server don't apply.
            connectionConfiguration: .init(verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test"], urlPath: "/webtransport")
        ) { session in
            // The supported test servers allow 10 streams of each type and 1 KiB of data per session when Flow Control is enabled.
            for _ in 1...11 {
                try await session.withBidirectionalStream { _, _ in }
                try await session.withUnidirectionalStream { _ in }
            }

            try await session.withUnidirectionalStream { outbound in
                try await outbound.write(ByteBuffer(repeating: UInt8(ascii: "a"), count: 2 << 10))
            }

            try await session.withBidirectionalStream { _, outbound in
                try await outbound.write(ByteBuffer(repeating: UInt8(ascii: "a"), count: 2 << 10))
            }
        }
    }

    @Test("Update Max Data", arguments: TestWTServer.allServersSupportingFlowControl)
    func updateMaxData(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(
                flowControlSettings: .init(
                    initialMaximumUnidirectionalStreams: 10,
                    initialMaximumBidirectionalStreams: 10,
                    initialMaximumData: 16
                ),
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
            ),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test"], urlPath: "/webtransport")
        ) { session in
            await #expect(throws: WebTransportError.flowControlLimitExceeded) { try await session.increaseMaxData(by: .max) }

            let payload = ByteBuffer(repeating: UInt8(ascii: "a"), count: 32)
            try await session.withBidirectionalStream { inbound, outbound in
                // The echo is larger than the 16 bytes the server is allowed to send, so it blocks.
                try await outbound.write(payload)

                var blockedIterator = session.incomingDataBlocked.makeAsyncIterator()
                #expect(await blockedIterator.next() == 16)

                try await session.increaseMaxData(by: 1 << 10)

                var received = 0
                for try await message in inbound {
                    received += message.readableBytes
                    if received == payload.readableBytes { break }
                }
            }
        }
    }

    @Test("Update Max Streams", arguments: TestWTServer.allServersSupportingFlowControl)
    func updateMaxStreams(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            connectionConfiguration: .init(
                flowControlSettings: .init(
                    initialMaximumUnidirectionalStreams: 0,
                    initialMaximumBidirectionalStreams: 0,
                    initialMaximumData: 1 << 20
                ),
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
            ),
            sessionConfiguration: .init(applicationProtocols: ["webtransport-test"], urlPath: "/webtransport")
        ) { session in
            await #expect(throws: WebTransportError.flowControlLimitExceeded) { try await session.increaseMaxBidirectionalStreams(by: .max) }

            // Ask the server to open two streams of each type. Only the first of each can be opened until the limits are raised.
            for _ in 1...2 {
                try await session.withUnidirectionalStream { try await $0.write(ByteBuffer(string: "open")) }
                try await session.withBidirectionalStream { _, outbound in try await outbound.write(ByteBuffer(string: "open")) }
            }

            var uniStreams = session.incomingUnidirectionalStreams.makeAsyncIterator()
            var biStreams = session.incomingBidirectionalStreams.makeAsyncIterator()

            try await session.increaseMaxUnidirectionalStreams(by: 1)
            try await session.increaseMaxBidirectionalStreams(by: 1)

            #expect(await uniStreams.next() != nil)
            #expect(await biStreams.next() != nil)

            try await session.increaseMaxUnidirectionalStreams(by: 1)
            try await session.increaseMaxBidirectionalStreams(by: 1)

            #expect(await uniStreams.next() != nil)
            #expect(await biStreams.next() != nil)
        }
    }
}
