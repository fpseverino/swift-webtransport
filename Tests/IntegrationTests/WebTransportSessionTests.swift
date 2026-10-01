import HTTPTypes
import NIOCore
import NIOHTTP3
import NIOHTTPTypes
import NIOQUIC
import Testing
import WebTransport

@Suite("WebTransportSession Tests")
struct WebTransportSessionTests {
    @Test("Open Streams", arguments: TestWTServer.allCases)
    func openStreams(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
        ) { connection in
            try await connection.withSession(
                configuration: .init(applicationProtocols: ["webtransport-test", "webtransport-test-2"], urlPath: "/webtransport")
            ) { session in
                try await withThrowingTaskGroup { group in
                    group.addTask {
                        try await session.withUnidirectionalStream { outbound in
                            try await outbound.write(ByteBuffer(string: "Hello, WebTransport!"))
                        }
                    }

                    group.addTask {
                        try await session.withBidirectionalStream { inbound, outbound in
                            let payload = ByteBuffer(string: "Hello from client!")
                            try await outbound.write(payload)
                            var inboundStreamIterator = inbound.makeAsyncIterator()
                            #expect(try await inboundStreamIterator.next() == payload)
                        }
                    }

                    group.addTask {
                        try await session.withBidirectionalStream { inbound, outbound in
                            let firstPayload = ByteBuffer(string: "Hello, World!")
                            try await outbound.write(firstPayload)
                            var inboundStreamIterator = inbound.makeAsyncIterator()
                            #expect(try await inboundStreamIterator.next() == firstPayload)

                            let secondPayload = ByteBuffer(string: "Hello, Swift!")
                            try await outbound.write(secondPayload)
                            #expect(try await inboundStreamIterator.next() == secondPayload)
                        }
                    }

                    group.addTask {
                        try await session.withUnidirectionalStream { outbound in
                            try await outbound.write(ByteBuffer(string: "Hello from unidirectional stream!"))
                        }
                    }

                    try await group.waitForAll()
                }
            }
        }
    }

    @Test("Incoming Streams", arguments: TestWTServer.allCases)
    func incomingStreams(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
        ) { connection in
            try await connection.withSession(
                configuration: .init(applicationProtocols: ["webtransport-test", "webtransport-test-2"], urlPath: "/webtransport")
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
    }

    @Test("Datagrams", arguments: TestWTServer.allCases)
    func datagrams(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
        ) { connection in
            try await connection.withSession(
                configuration: .init(applicationProtocols: ["webtransport-test", "webtransport-test-2"], urlPath: "/webtransport")
            ) { session in
                let payload = ByteBuffer(string: "Hello, datagrams!")

                try await session.sendDatagram(payload)

                for await datagram in session.incomingDatagrams {
                    #expect(datagram == payload)
                    break
                }
            }
        }
    }

    @Test("Multiple Sessions", arguments: TestWTServer.allServersSupportingFlowControl)
    func multipleSessions(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath)
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
}
