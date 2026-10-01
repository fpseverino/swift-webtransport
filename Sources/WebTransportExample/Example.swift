import Foundation
import Logging
import NIOCore
import NIOQUIC
import WebTransport

@main
struct Example {
    static func main() async throws {
        /// Points to default location of the PEM file written by the Go server.
        let trustRootsFilePath = URL(filePath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appending(path: "go-server/cert.pem")
            .path()

        // Establish a WebTransport connection to the server.
        try await WebTransportConnection.withConnection(
            ipAddress: "127.0.0.1",
            port: 6121,
            verificationConfiguration: .x509Certificates(trustRootsFilePath: trustRootsFilePath)
        ) { connection in
            Logger.current.info("Established WebTransport connection to the server")

            /// Common configuration for the WebTransport sessions.
            let sessionConfiguration = WebTransportSession.Configuration(
                applicationProtocols: ["webtransport-test", "webtransport-test-2"],
                urlPath: "/webtransport"
            )

            // Create a WebTransport session where the server will open a unidirectional stream.
            try await connection.withSession(configuration: sessionConfiguration) { session in
                Logger.current.info("Session established with the server")

                // Initiate a unidirectional stream to the server to request it to open a stream.
                try await session.withUnidirectionalStream { outbound in
                    try await outbound.write(ByteBuffer(string: "open"))
                    Logger.current.info("Client asked the server to open a unidirectional stream")
                    outbound.finish()
                }

                // Await the incoming unidirectional stream from the server.
                for await stream in session.incomingUnidirectionalStreams {
                    try await stream.executeThenClose { inbound in
                        for try await buffer in inbound {
                            Logger.current.info("Client received on unidirectional stream: '\(String(buffer: buffer))'")
                            break
                        }
                    }
                    break
                }
            }

            // Create a WebTransport session where the client will open a bidirectional stream.
            try await connection.withSession(configuration: sessionConfiguration) { session in
                Logger.current.info("Session established with the server")

                // Initiate a bidirectional stream with the server.
                try await session.withBidirectionalStream { inbound, outbound in
                    let request = "echo"
                    try await outbound.write(ByteBuffer(string: request))
                    Logger.current.info("Client sent on bidirectional stream: '\(request)'")
                    outbound.finish()

                    for try await buffer in inbound {
                        Logger.current.info("Client received on bidirectional stream: '\(String(buffer: buffer))'")
                        break
                    }
                }
            }

            // Create a WebTransport session where datagrams will be exchanged.
            try await connection.withSession(configuration: sessionConfiguration) { session in
                Logger.current.info("Session established with the server")

                let request = "echo"
                try await session.sendDatagram(ByteBuffer(string: request))
                Logger.current.info("Client sent datagram with payload: '\(request)'")

                for try await datagram in session.incomingDatagrams {
                    Logger.current.info("Client received datagram with payload: '\(String(buffer: datagram))'")
                    break
                }
            }
        }
    }
}
