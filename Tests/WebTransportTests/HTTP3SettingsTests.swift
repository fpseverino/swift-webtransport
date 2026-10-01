import HTTP3
import Testing

@testable import WebTransport

@Suite("HTTP3Settings Tests")
struct HTTP3SettingsTests {
    @Test("Validate server WebTransport SETTINGS", .disabled("https://github.com/BiagioFesta/wtransport/issues/332"))
    func validateServerWebTransportSettings() throws {
        let supportedSettings = try HTTP3Settings(parsing: [
            .init(identifier: .h3Datagram, value: 1),
            .init(identifier: .webTransportEnabled, value: 1),
        ])
        #expect(supportedSettings.serverSupportsWebTransport)

        let missingWebTransportSetting = try HTTP3Settings(parsing: [
            .init(identifier: .h3Datagram, value: 1)
        ])
        #expect(!missingWebTransportSetting.serverSupportsWebTransport)

        let missingDatagramSetting = try HTTP3Settings(parsing: [
            .init(identifier: .webTransportEnabled, value: 1)
        ])
        #expect(!missingDatagramSetting.serverSupportsWebTransport)

        let disabledWebTransportSetting = try HTTP3Settings(parsing: [
            .init(identifier: .h3Datagram, value: 1),
            .init(identifier: .webTransportEnabled, value: 0),
        ])
        #expect(!disabledWebTransportSetting.serverSupportsWebTransport)
    }
}
