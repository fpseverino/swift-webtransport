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
