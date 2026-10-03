import HTTP3

extension HTTP3Setting.Identifier {
    /// Corresponds to `SETTINGS_WT_ENABLED`.
    ///
    /// The default value is zero.
    ///
    /// See [draft-ietf-webtrans-http3-16 § 9.2](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/)
    static var webTransportEnabled: HTTP3Setting.Identifier {
        HTTP3Setting.Identifier(extensionSetting: 0x2c7c_f000)!
    }

    /// Corresponds to `SETTINGS_WT_INITIAL_MAX_STREAMS_UNI`.
    ///
    /// The default value is zero.
    ///
    /// See [draft-ietf-webtrans-http3-16 § 9.2](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/)
    static var webTransportInitialMaximumStreamsUnidirectional: HTTP3Setting.Identifier {
        HTTP3Setting.Identifier(extensionSetting: 0x2b64)!
    }

    /// Corresponds to `SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI`.
    ///
    /// The default value is zero.
    ///
    /// See [draft-ietf-webtrans-http3-16 § 9.2](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/)
    static var webTransportInitialMaximumStreamsBidirectional: HTTP3Setting.Identifier {
        HTTP3Setting.Identifier(extensionSetting: 0x2b65)!
    }

    /// Corresponds to `SETTINGS_WT_INITIAL_MAX_DATA`.
    ///
    /// The default value is zero.
    ///
    /// See [draft-ietf-webtrans-http3-16 § 9.2](https://datatracker.ietf.org/doc/draft-ietf-webtrans-http3/)
    static var webTransportInitialMaximumData: HTTP3Setting.Identifier {
        HTTP3Setting.Identifier(extensionSetting: 0x2b61)!
    }
}

extension HTTP3Settings {
    var serverSupportsWebTransport: Bool {
        guard self.h3Datagram else { return false }

        // TODO: Also require SETTINGS_ENABLE_CONNECT_PROTOCOL when swift-nio-http3 exposes remote settings
        // https://github.com/apple/swift-nio-http3/pull/65
        // https://github.com/apple/swift-nio-http3/pull/69
        // TODO: The Rust server does not yet send the SETTINGS_WT_ENABLED setting, so for now we don't check it.
        // https://github.com/BiagioFesta/wtransport/issues/332

        // return self.other.contains {
        //     $0.identifier == .webTransportEnabled && $0.value == 1
        // }
        return true
    }
}
