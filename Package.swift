// swift-tools-version: 6.3
import PackageDescription

let package = Package(
    name: "swift-webtransport",
    platforms: [
        .macOS(.v26)
    ],
    products: [
        .library(name: "WebTransport", targets: ["WebTransport"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-http-types.git", from: "1.8.0"),
        .package(url: "https://github.com/apple/swift-http-structured-headers.git", from: "1.2.0"),
        // NIO
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.102.0"),
        .package(url: "https://github.com/apple/swift-nio-extras.git", from: "1.35.1"),
        // This fork publicly exposes the `QUICStreamCreator` of the `HTTP3ClientConnection`,
        // so we can create QUIC streams for WebTransport from the connection after the HTTP/3 handshake.
        .package(url: "https://github.com/fpseverino/swift-nio-http3.git", branch: "webtransport"),
        .package(url: "https://github.com/apple/swift-nio-quic.git", .upToNextMinor(from: "0.2.0")),
        .package(url: "https://github.com/apple/swift-nio-quic-helpers.git", .upToNextMinor(from: "0.1.0")),
        // Observability
        .package(url: "https://github.com/apple/swift-log.git", from: "1.15.0"),
    ],
    targets: [
        .target(
            name: "WebTransport",
            dependencies: [
                .product(name: "HTTPTypes", package: "swift-http-types"),
                .product(name: "RawStructuredFieldValues", package: "swift-http-structured-headers"),
                // NIO
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOHTTPTypes", package: "swift-nio-extras"),
                .product(name: "NIOHTTP3", package: "swift-nio-http3"),
                .product(name: "NIOQUIC", package: "swift-nio-quic"),
                .product(name: "NIOQUICHelpers", package: "swift-nio-quic-helpers"),
                // Observability
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "WebTransportTests",
            dependencies: [
                .target(name: "WebTransport")
            ],
            swiftSettings: swiftSettings
        ),
    ]
)

var swiftSettings: [SwiftSetting] {
    [
        .strictMemorySafety(),
        .enableExperimentalFeature("SuppressedAssociatedTypesWithDefaults"),
        .enableExperimentalFeature("LifetimeDependence"),
        .enableExperimentalFeature("Lifetimes"),
        .enableUpcomingFeature("LifetimeDependence"),
        .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
        .enableUpcomingFeature("InferIsolatedConformances"),
        .enableUpcomingFeature("ExistentialAny"),
        .enableUpcomingFeature("MemberImportVisibility"),
        .enableUpcomingFeature("InternalImportsByDefault"),
        .enableUpcomingFeature("ImmutableWeakCaptures"),
    ]
}
