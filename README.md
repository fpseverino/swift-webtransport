# Swift WebTransport

**Swift WebTransport** is a project being developed as part of Francesco Paolo Severino's curricular internship at the _University of Naples Federico II_, under the supervision of Prof. Simon Pietro Romano.

The project aims to implement the **WebTransport** protocol in the **Swift** programming language, based on the [SwiftNIO](https://github.com/apple/swift-nio) network application framework.

## WebTransport protocol

WebTransport is a novel browser API that enables browsers to establish stream-multiplexed connections to a server.
It is layered atop **QUIC** and **HTTP/3**, offering a fallback mechanism on HTTP/2 for scenarios where QUIC might be blocked.
Conceptually, WebTransport can be compared to **WebSocket** but utilizes QUIC instead of TCP, providing benefits such as stream multiplexing and support for datagrams—features that enhance performance and efficiency for real-time communication.
Despite these conceptual similarities, WebTransport and WebSocket differ significantly in their underlying protocols.

### RFCs

The [WebTransport working group](https://datatracker.ietf.org/wg/webtrans/about/) of the IETF so far has published the following RFCs:

- [draft-ietf-webtrans-overview-13](https://datatracker.ietf.org/doc/draft-ietf-webtrans-overview/): this document defines the overall requirements on the protocols used in WebTransport, as well as the common features of the protocols, support for some of which is optional.
- [draft-ietf-webtrans-http3-16](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/): this document defines the WebTransport protocol over HTTP/3, which is the most commonly implemented and deployed protocol for WebTransport. This project will mainly focus on the implementation of this protocol.
- [draft-ietf-webtrans-http2-15](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http2/): this document describes a protocol that can provide the capabilities of WebTransport over HTTP/2. This protocol enables the use of WebTransport when a UDP-based protocol is not available. The implementation of this protocol is a **non-goal** of this project.

## Client and Server

WebTransport is a client-server protocol, just like WebSocket.

In the Swift on Server ecosystem, web frameworks such as [Vapor](https://vapor.codes) and [Hummingbird](https://hummingbird.codes) have developed their own WebSocket implementations, based on [SwiftNIO WebSocket](https://swiftpackageindex.com/apple/swift-nio/documentation/niowebsocket), respectively [WebSocketKit](https://github.com/vapor/websocket-kit) and [swift-websocket](https://github.com/hummingbird-project/swift-websocket).
They both implement a WebSocket client and an abstraction of a WebSocket server that they integrate into their own HTTP frameworks.

There is however an effort by the [Swift Server Workgroup (SSWG)](https://www.swift.org/sswg/) to build a [common HTTP server](https://github.com/swift-server/swift-http-server) for the ecosystem that will be shared by all the frameworks, and there's an [ongoing discussion](https://github.com/swift-server/swift-http-server/issues/100) about adding WebSocket support to the HTTP server.

Since this project strives to offer a WebTransport implementation that follows modern Swift best practices and that can be genuinely useful for the Swift on Server community, integrating WebTransport support into the common HTTP server, just like WebSocket, would be desirable.

That however requires coordination and approval from the SSWG, so the **goal** of this project is to implement a fully independently usable WebTransport client, modularizing the core logic and types of the protocol into a separate module that can be shared by both the client implemented in this project and, in the future, by the common HTTP server.

### Package structure

Swift WebTransport's package structure would therefore look very similar to the one of `swift-websocket`, with:

- `WebTransportClient`, a module that implements the WebTransport client that developers can use in their applications
- `WebTransportCore`, a module that implements the basic types, logic, and SwiftNIO handlers that can be shared by both `WebTransportClient` and the common HTTP server

## Dependencies

Swift WebTransport will be a SwiftNIO-based project, intended primarily for server-side Swift applications. The `WebTransportClient` should be usable in any Swift application, including iOS and macOS applications.

The project will thus depend on the following packages from the SwiftNIO project:

- [apple/swift-nio](https://github.com/apple/swift-nio): the core SwiftNIO package, which provides the fundamental building blocks for asynchronous event-driven network applications in Swift
- [apple/swift-nio-extras](https://github.com/apple/swift-nio-extras): useful additions around SwiftNIO that might be helpful
- [apple/swift-nio-transport-services](https://github.com/apple/swift-nio-transport-services): provides first-class support for macOS, iOS, tvOS, and watchOS

To provide support for observability (logging, metrics, and tracing), the project will also depend on the following packages:

- [apple/swift-log](https://github.com/apple/swift-log) for logging
- [apple/swift-metrics](https://github.com/apple/swift-metrics) for metrics
- [apple/swift-distributed-tracing](https://github.com/apple/swift-distributed-tracing) for tracing

To integrate Swift WebTransport with the wider Swift on Server ecosystem, the project will also depend on the following packages:

- [apple/swift-http-types](https://github.com/apple/swift-http-types): version-independent HTTP currency types, designed for both clients and servers
- [swift-server/swift-service-lifecycle](https://github.com/swift-server/swift-service-lifecycle): a package that provides, among other things, utilities for cleanly shutting down an application if the current task is shutting down gracefully
- [apple/swift-configuration](https://github.com/apple/swift-configuration): a library for reading configuration in applications and libraries

Then to work with QUIC and HTTP/3, the project will depend (directly or transitively) on the following packages from the SwiftNIO QUIC and HTTP/3 stack:

- [apple/swift-tls](https://github.com/apple/swift-tls): a minimal, Swift-native implementation of the TLS 1.3 handshake to support the QUIC transport protocol
- [apple/swift-network-evolution](https://github.com/apple/swift-network-evolution): this package provides access to the implementations of core networking objects and protocol stack architecture from [Network.framework](https://developer.apple.com/documentation/network) that is available on Apple platforms, and is intended to be used by cross-platform projects like SwiftNIO to be able to access a Swift-based implementation of protocols like QUIC
- [apple/swift-nio-quic-helpers](https://github.com/apple/swift-nio-quic-helpers): QUIC supporting types for SwiftNIO
- [apple/swift-nio-quic](https://github.com/apple/swift-nio-quic): QUIC support for SwiftNIO
- [apple/swift-nio-http3](https://github.com/apple/swift-nio-http3): HTTP/3 support for SwiftNIO

While it's not a primary goal of this project, the implementation of the WebTransport protocol might require support for the Capsule Protocol, which has been recently implemented in the [apple/swift-http-api-proposal](https://github.com/apple/swift-http-api-proposal) repository. If this is the case, the project will also depend on this package.

Also, while the future goal is for the common HTTP server to depend in some shape or form on the `WebTransportCore` module, this project might also depend on the common HTTP server package to test the integration of the WebTransport protocol with the HTTP server.

### HTTP Datagrams and Capsule Protocol

As the `draft-ietf-webtrans-http3-16` states in Section 3.1:

```
WebTransport over HTTP/3 requires support for HTTP/3 datagrams and
the Capsule Protocol [...]
```

Apple has informally stated that they are working on introducing support for HTTP Datagrams in FY26 Q4.
[apple/swift-http-api-proposal](https://github.com/apple/swift-http-api-proposal) already provides an implementation of the Capsule Protocol, but it's not been evaluated yet whether it is sufficient to support the WebTransport protocol.

The goal is to initially attempt to create a bare-bones implementation of the WebTransport protocol without support for HTTP Datagrams and the Capsule Protocol, but if it turns out that they are required even for a minimal implementation, it will become a goal of the project to help with the open-source development of HTTP Datagrams and potentially improve the implementation of the Capsule Protocol in the `swift-http-api-proposal` package to support WebTransport.

## Work plan

The work plan for the Swift WebTransport project is as follows:

1. Implement a minimal WebTransport client that can establish a connection to a WebTransport server and send/receive data over the connection, just to prove that the protocol can be implemented with the currently available SwiftNIO QUIC and HTTP/3 stack.
    - If it turns out that HTTP Datagrams and the Capsule Protocol or any other dependencies are required even for a minimal implementation, help with the open-source development of HTTP Datagrams and potentially improve the implementation of the Capsule Protocol in the `swift-http-api-proposal` package to support WebTransport.
2. Refactor the implementation of the client to follow modern Swift Concurrency and SwiftNIO best practices.
3. Move the core logic and types of the protocol that can be shared by both the client and the common HTTP server into a separate module, `WebTransportCore`.
4. Implement all the optional features that can be implemented, such as HTTP Datagrams, Capsule Protocol, and any other features that are required to fully support the WebTransport protocol specification.
5. Develop a test implementation of the common HTTP server that integrates the `WebTransportCore` module and implements a WebTransport server, to test the integration of the WebTransport protocol with the HTTP server.
