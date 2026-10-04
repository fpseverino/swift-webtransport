import Testing
import WebTransport

@Suite("WebTransport Tests")
struct WebTransportTests {
    @Test("WebTransport Error")
    func webTransportError() {
        #expect(WebTransportError.serverRejectedSession.description == "WebTransportError(errorType: serverRejectedSession)")
        #expect(WebTransportError.serverDoesNotSupportWebTransport.description == "WebTransportError(errorType: serverDoesNotSupportWebTransport)")
        #expect(WebTransportError.serverRejectedSession != .serverDoesNotSupportWebTransport)
    }
}
