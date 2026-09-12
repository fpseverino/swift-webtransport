import Foundation
import HTTPTypes
import NIOCore
import NIOHTTPTypes
import NIOQUIC
import Testing

@testable import WebTransport

@Suite("WebTransportConnection Tests")
struct WebTransportConnectionTests {
    /// Points to the PEM written by `server/main.go` (run with `go run . -cert-out=<path>` if moved).
    static let trustRootsFilePath = URL(filePath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .appending(path: "server/cert.pem")
        .path()

    @Test
    func example() async throws {
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: 6121,
            configuration: .init(
                verificationConfiguration: .x509Certificates(trustRootsFilePath: Self.trustRootsFilePath),
                applicationProtocols: ["webtransport-test", "webtransport-test-2"],
                urlPath: "/webtransport"
            )
        ) { connection in
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
}
