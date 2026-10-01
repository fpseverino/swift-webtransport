import Foundation

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

    /// Returns all test servers that support multiple sessions on a single connection.
    static var allServersSupportingFlowControl: [TestWTServer] {
        [.go]
    }
}
