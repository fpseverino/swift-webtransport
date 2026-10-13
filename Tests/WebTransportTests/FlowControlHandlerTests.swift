import Testing

@testable import WebTransport

@Suite("FlowControlHandler Tests")
struct FlowControlHandlerTests {
    @Test("Enforces Server Limits when Enabled")
    func enabled() {
        let flowControl = FlowControlHandler(
            initialServerMaxUnidirectionalStreams: 1,
            initialServerMaxBidirectionalStreams: 1,
            initialServerMaxData: 10,
            initialClientMaxUnidirectionalStreams: 1,
            initialClientMaxBidirectionalStreams: 1,
            initialClientMaxData: 10
        )

        // If these methods return nil, it means the stream or data may be sent.
        // If they return a number, it means the limit has been reached, and the number represents the limit that was hit.
        #expect(flowControl.reserveUnidirectionalStream() == nil)
        #expect(flowControl.reserveUnidirectionalStream() == 1)
        #expect(flowControl.reserveBidirectionalStream() == nil)
        #expect(flowControl.reserveBidirectionalStream() == 1)
        #expect(flowControl.reserveData(10) == nil)
        #expect(flowControl.reserveData(1) == 10)

        // These methods are called when the server increases its limits.
        flowControl.updateServerMaxUnidirectionalStreams(2)
        flowControl.updateServerMaxBidirectionalStreams(2)
        flowControl.updateServerMaxData(20)
        // After updating the server limits, the client should be able to reserve additional streams and data.
        #expect(flowControl.reserveUnidirectionalStream() == nil)
        #expect(flowControl.reserveBidirectionalStream() == nil)
        #expect(flowControl.reserveData(10) == nil)
    }

    @Test("Ignores Server Limits and Capsules when Disabled")
    func disabled() {
        let flowControl = FlowControlHandler(
            initialServerMaxUnidirectionalStreams: 0,
            initialServerMaxBidirectionalStreams: 0,
            initialServerMaxData: 0,
            initialClientMaxUnidirectionalStreams: 0,
            initialClientMaxBidirectionalStreams: 0,
            initialClientMaxData: 0
        )

        // A capsule received while Flow Control is disabled must be ignored, not turned into a limit.
        flowControl.updateServerMaxUnidirectionalStreams(1)
        flowControl.updateServerMaxBidirectionalStreams(1)
        flowControl.updateServerMaxData(1)

        for _ in 1...10 {
            #expect(flowControl.reserveUnidirectionalStream() == nil)
            #expect(flowControl.reserveBidirectionalStream() == nil)
            #expect(flowControl.reserveData(1 << 20) == nil)
        }
    }

    @Test("Increasing Client Limits")
    func increasingClientLimits() throws {
        let flowControl = FlowControlHandler(
            initialServerMaxUnidirectionalStreams: 5,
            initialServerMaxBidirectionalStreams: 5,
            initialServerMaxData: 100,
            initialClientMaxUnidirectionalStreams: 5,
            initialClientMaxBidirectionalStreams: 5,
            initialClientMaxData: 100
        )

        #expect(try flowControl.increaseClientMaxData(by: 100) == 200)
        #expect(try flowControl.increaseClientMaxData(by: 0) == nil)
        #expect(throws: WebTransportError.flowControlLimitExceeded) { try flowControl.increaseClientMaxData(by: .max) }

        #expect(try flowControl.increaseClientMaxUnidirectionalStreams(by: 6) == 11)
        #expect(throws: WebTransportError.flowControlLimitExceeded) { try flowControl.increaseClientMaxUnidirectionalStreams(by: .max) }
        #expect(try flowControl.increaseClientMaxBidirectionalStreams(by: (1 << 60) - 5) == (1 << 60))
        #expect(throws: WebTransportError.flowControlLimitExceeded) {
            try flowControl.increaseClientMaxBidirectionalStreams(by: (1 << 60) + 1)
        }
    }

    @Test("Client Limits are not sent when Disabled")
    func clientLimitsDisabled() throws {
        let flowControl = FlowControlHandler(
            initialServerMaxUnidirectionalStreams: 0,
            initialServerMaxBidirectionalStreams: 0,
            initialServerMaxData: 0,
            initialClientMaxUnidirectionalStreams: 0,
            initialClientMaxBidirectionalStreams: 0,
            initialClientMaxData: 0
        )

        #expect(try flowControl.increaseClientMaxData(by: 100) == nil)
        #expect(try flowControl.increaseClientMaxUnidirectionalStreams(by: 100) == nil)
        #expect(try flowControl.increaseClientMaxBidirectionalStreams(by: 100) == nil)
    }
}
