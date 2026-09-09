import HTTPTypes
public import Logging
public import NIOCore
@_spi(HTTP3AsyncInterface) import NIOHTTP3
import NIOHTTPTypes
public import NIOPosix
import NIOQUIC
import NIOQUICHelpers
import RawStructuredFieldValues

public func withWebTransportConnection(
    host: String,
    port: Int,
    path: String = "/",
    applicationProtocols: [String] = [],
    trustRootsFilePath: String,
    logger: Logger = .current,
    eventLoopGroup: any EventLoopGroup = MultiThreadedEventLoopGroup.singleton,
    body: sending (WebTransportConnection) async throws -> Void
) async throws {
    try await withH3Connection(
        host: host,
        port: port,
        trustRootsFilePath: trustRootsFilePath,
        logger: logger,
        eventLoopGroup: eventLoopGroup
    ) { inbound, outbound, h3Connection in
        var headerSerializer = StructuredFieldValueSerializer()
        var connectRequest = HTTPRequest(
            method: .connect,
            scheme: "https",
            authority: "\(host):\(port)",
            path: path,
            headerFields: try .init(parsedTrailerFields: [
                .init(
                    name: .init("WT-Available-Protocols")!,
                    value: headerSerializer.writeListFieldValue(
                        applicationProtocols.map {
                            .item(.init(bareItem: RFC9651BareItem.string($0), parameters: [:]))
                        }
                    )
                )
            ])
        )
        connectRequest.extendedConnectProtocol = "webtransport-h3"
        try await outbound.write(.head(connectRequest))

        var responseIterator = inbound.makeAsyncIterator()
        guard
            let headResponsePart = try await responseIterator.next(),
            case .head(let response) = headResponsePart,
            response.status == .ok
        else {
            fatalError("Failed to establish WebTransport connection. Expected HTTP 200 OK response.")
        }
        print("Response headers: \(response.headerFields)")

        try await body(WebTransportConnection(h3Connection: h3Connection))
    }
}

public actor WebTransportConnection: Sendable {
    let h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>

    init(h3Connection: HTTP3ClientConnection<Never, NIOQUIC.QUICStreamCreator>) {
        self.h3Connection = h3Connection
    }

    public func withBidirectionalStream(
        body: (NIOAsyncChannelInboundStream<ByteBuffer>, NIOAsyncChannelOutboundWriter<ByteBuffer>) async throws -> Void
    ) async throws {
        try await self.h3Connection.makeBidirectionalStream().executeThenClose { inboundStream, outboundStream in
            var buffer = ByteBuffer()
            buffer.writeEncodedInteger(0x41, strategy: .quic)
            buffer.writeEncodedInteger(0x00, strategy: .quic)
            try await outboundStream.write(buffer)
            try await body(inboundStream, outboundStream)
        }
    }

    public func withUnidirectionalStream(
        body: (NIOAsyncChannelOutboundWriter<ByteBuffer>) async throws -> Void
    ) async throws {
        try await self.h3Connection.makeUnidirectionalStream().executeThenClose { _, outboundStream in
            var buffer = ByteBuffer()
            buffer.writeEncodedInteger(0x54, strategy: .quic)
            buffer.writeEncodedInteger(0x00, strategy: .quic)
            try await outboundStream.write(buffer)
            try await body(outboundStream)
        }
    }
}
