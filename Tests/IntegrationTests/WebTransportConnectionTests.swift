import Foundation
import HTTPTypes
import NIOCore
import NIOHTTP3
import NIOHTTPTypes
import NIOQUIC
import Testing

@testable import WebTransport

@Suite("WebTransportConnection Tests")
struct WebTransportConnectionTests {
    @Test("Open Streams", arguments: TestWTServer.allCases)
    func openStreams(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            configuration: .init(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath),
                applicationProtocols: ["webtransport-test", "webtransport-test-2"],
                urlPath: "/webtransport"
            )
        ) { connection in
            try await connection.withUnidirectionalStream { outbound in
                try await outbound.write(ByteBuffer(string: "Hello, WebTransport!"))
            }

            try await connection.withBidirectionalStream { inbound, outbound in
                try await outbound.write(ByteBuffer(string: "Hello from client!"))
                var inboundStreamIterator = inbound.makeAsyncIterator()
                let inboundStreamData = try await inboundStreamIterator.next()
                print("Received inbound stream data: \(String(buffer: inboundStreamData ?? ByteBuffer(string: "error.")))")
            }

            try await connection.withBidirectionalStream { inbound, outbound in
                try await outbound.write(ByteBuffer(string: "Hello, Swift!"))
                var inboundStreamIterator = inbound.makeAsyncIterator()
                let inboundStreamData = try await inboundStreamIterator.next()
                print("Received inbound stream data: \(String(buffer: inboundStreamData ?? ByteBuffer(string: "error.")))")
            }

            try await connection.withUnidirectionalStream { outbound in
                try await outbound.write(ByteBuffer(string: "Hello from unidirectional stream!"))
            }
        }
    }

    @Test("Incoming Streams", arguments: TestWTServer.allCases)
    func incomingStreams(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            configuration: .init(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath),
                applicationProtocols: ["webtransport-test", "webtransport-test-2"],
                urlPath: "/webtransport"
            )
        ) { connection in
            try await connection.withUnidirectionalStream { outbound in
                try await outbound.write(ByteBuffer(string: "open"))
            }

            for await stream in await connection.incomingUnidirectionalStreams {
                try await stream.executeThenClose { inbound in
                    for try await message in inbound {
                        print("Received incoming stream message: \(String(buffer: message))")
                        break
                    }
                }
                break
            }

            try await connection.withBidirectionalStream { inbound, outbound in
                try await outbound.write(ByteBuffer(string: "open"))
            }

            for await stream in await connection.incomingBidirectionalStreams {
                try await stream.executeThenClose { inbound, outbound in
                    for try await message in inbound {
                        print("Received incoming stream message: \(String(buffer: message))")
                        break
                    }
                }
                break
            }
        }
    }

    @Test("Datagrams", arguments: TestWTServer.allCases)
    func datagrams(server: TestWTServer) async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: server.port,
            configuration: .init(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: server.trustRootsFilePath),
                applicationProtocols: ["webtransport-test", "webtransport-test-2"],
                urlPath: "/webtransport"
            )
        ) { connection in
            try await connection.sendDatagram(ByteBuffer(string: "Hello, datagrams!"))

            for await datagram in await connection.incomingDatagrams {
                print("Received incoming datagram: \(String(buffer: datagram.payload))")
                break
            }
        }
    }
}

enum TestWTServer: CaseIterable {
    case go
    case rust

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

    static let baseURL = URL(filePath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
}
