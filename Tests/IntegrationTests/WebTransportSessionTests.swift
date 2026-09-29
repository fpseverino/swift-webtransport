import Foundation
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
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            configuration: .init(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath),
                applicationProtocols: ["webtransport-test", "webtransport-test-2"],
                urlPath: "/webtransport"
            )
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

    @Test("Incoming Streams", arguments: TestWTServer.allCases)
    func incomingStreams(server: TestWTServer) async throws {
        try await WebTransportSession.withSession(
            ipAddress: "127.0.0.1",
            port: server.port,
            configuration: .init(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath),
                applicationProtocols: ["webtransport-test", "webtransport-test-2"],
                urlPath: "/webtransport"
            )
        ) { session in
            try await withThrowingTaskGroup { group in
                group.addTask {
                    try await session.withUnidirectionalStream { outbound in
                        try await outbound.write(ByteBuffer(string: "open"))
                    }

                    for await stream in await session.incomingUnidirectionalStreams {
                        try await stream.executeThenClose { inbound in
                            for try await message in inbound {
                                print(String(buffer: message))
                                // TODO: remove the Session ID from the start of Unidirectional Streams
                                #expect(message == ByteBuffer(string: "\u{00}opened"))
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

                    for await stream in await session.incomingBidirectionalStreams {
                        try await stream.executeThenClose { inbound, outbound in
                            for try await message in inbound {
                                print(String(buffer: message))
                                // TODO: remove Stream Type, Signal Value (0x41) and Session ID from the start of Bidirectional Streams
                                #expect(message == ByteBuffer(string: "\u{40}\u{41}\u{00}opened"))
                                break
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
            configuration: .init(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath),
                applicationProtocols: ["webtransport-test", "webtransport-test-2"],
                urlPath: "/webtransport"
            )
        ) { session in
            let payload = ByteBuffer(string: "Hello, datagrams!")

            try await session.sendDatagram(payload)

            for await datagram in await session.incomingDatagrams {
                #expect(datagram.payload == payload)
                break
            }
        }
    }
}

/// Represents the different WebTransport test servers used in integration tests.
enum TestWTServer: CaseIterable {
    /// Written in Go with `webtransport-go`. Resides in the `go-server` directory.
    case go
    /// Written in Rust with `wtransport`. Resides in the `rust-server` directory.
    case rust

    /// The port on which the test server is listening.
    var port: Int {
        switch self {
        case .go: 6121
        case .rust: 4433
        }
    }

    /// Points to default location of the PEM file written by the server.
    var trustRootsFilePath: String {
        switch self {
        case .go: Self.baseURL.appending(path: "go-server/cert.pem").path()
        case .rust: Self.baseURL.appending(path: "rust-server/cert.pem").path()
        }
    }

    /// The base URL of the `swift-webtransport` project.
    static let baseURL = URL(filePath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
}
