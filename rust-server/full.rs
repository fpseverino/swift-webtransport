use anyhow::Context;
use anyhow::Result;
use http::HttpServer;
use rcgen::BasicConstraints;
use rcgen::CertificateParams;
use rcgen::DistinguishedName;
use rcgen::DnType;
use rcgen::ExtendedKeyUsagePurpose;
use rcgen::IsCa;
use rcgen::KeyPair;
use rcgen::KeyUsagePurpose;
use rcgen::PKCS_ECDSA_P256_SHA256;
use time::Duration;
use time::OffsetDateTime;
use tracing::error;
use tracing::info;
use tracing::info_span;
use tracing::Instrument;
use webtransport::WebTransportServer;
use wtransport::tls::Certificate;
use wtransport::tls::CertificateChain;
use wtransport::tls::PrivateKey;
use wtransport::tls::Sha256Digest;
use wtransport::tls::Sha256DigestFmt;
use wtransport::Identity;

const CERT_OUT_PATH: &str = "cert.pem";

#[tokio::main]
async fn main() -> Result<()> {
    utils::init_logging();

    let identity = self_signed_identity().context("Cannot create self-signed identity")?;
    let cert_digest = identity.certificate_chain().as_slice()[0].hash();

    // Non-browser clients (e.g. the Swift client) validate the certificate chain rather than
    // pinning a hash, so write it out as PEM to be used as a trusted root (it is self-signed).
    let cert_pem = identity.certificate_chain().as_slice()[0].to_pem();
    std::fs::write(CERT_OUT_PATH, cert_pem).context("Cannot write certificate PEM")?;
    info!("Server certificate written to: {}", CERT_OUT_PATH);

    let webtransport_server = WebTransportServer::new(identity)?;
    let http_server = HttpServer::new(&cert_digest, webtransport_server.local_port()).await?;

    info!(
        "Open the browser and go to: http://127.0.0.1:{}",
        http_server.local_port()
    );

    tokio::select! {
        result = http_server.serve() => {
            error!("HTTP server: {:?}", result);
        }
        result = webtransport_server.serve() => {
            error!("WebTransport server: {:?}", result);
        }
    }

    Ok(())
}

// Mirrors go-server/main.go's certificate template so both servers produce a
// certificate type the Swift client accepts as a trust root (an ECDSA P-256 CA cert).
fn self_signed_identity() -> Result<Identity> {
    let key_pair =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).context("Cannot generate key pair")?;

    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CommonName, "localhost");

    let not_before = OffsetDateTime::now_utc();
    let mut params = CertificateParams::new(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ])
    .context("Invalid subject alternative names")?;
    params.distinguished_name = distinguished_name;
    params.not_before = not_before;
    params.not_after = not_before + Duration::days(13);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    // The cert is self-signed and used directly as a trust anchor by non-browser clients,
    // which requires it to be marked as a CA (RFC 5280 §4.2.1.9).
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

    let cert = params
        .self_signed(&key_pair)
        .context("Cannot self-sign certificate")?;

    Ok(Identity::new(
        CertificateChain::single(
            Certificate::from_der(cert.der().to_vec()).context("Invalid certificate DER")?,
        ),
        PrivateKey::from_der_pkcs8(key_pair.serialize_der()),
    ))
}


mod webtransport {
    use super::*;
    use std::time::Duration;
    use wtransport::endpoint::endpoint_side::Server;
    use wtransport::endpoint::IncomingSession;
    use wtransport::Endpoint;
    use wtransport::ServerConfig;

    pub struct WebTransportServer {
        endpoint: Endpoint<Server>,
    }

    impl WebTransportServer {
        const PORT: u16 = 4433;

        pub fn new(identity: Identity) -> Result<Self> {
            let config = ServerConfig::builder()
                .with_bind_default(Self::PORT)
                .with_identity(identity)
                .keep_alive_interval(Some(Duration::from_secs(3)))
                .build();

            let endpoint = Endpoint::server(config)?;

            Ok(Self { endpoint })
        }

        pub fn local_port(&self) -> u16 {
            self.endpoint.local_addr().unwrap().port()
        }

        pub async fn serve(self) -> Result<()> {
            info!("Server running on port {}", self.local_port());

            for id in 0..i32::MAX {
                let incoming_session = self.endpoint.accept().await;

                tokio::spawn(
                    Self::handle_incoming_session(incoming_session)
                        .instrument(info_span!("Connection", id)),
                );
            }

            Ok(())
        }

        async fn handle_incoming_session(incoming_session: IncomingSession) {
            async fn handle_incoming_session_impl(incoming_session: IncomingSession) -> Result<()> {
                let mut buffer = vec![0; 65536].into_boxed_slice();

                info!("Waiting for session request...");

                let session_request = incoming_session.await?;

                info!(
                    "New session: Authority: '{}', Path: '{}'",
                    session_request.authority(),
                    session_request.path()
                );

                let connection = session_request.accept().await?;

                info!("Waiting for data from client...");

                loop {
                    tokio::select! {
                        stream = connection.accept_bi() => {
                            let mut stream = stream?;
                            info!("Accepted BI stream");

                            let Some(bytes_read) = stream.1.read(&mut buffer).await? else {
                                continue;
                            };

                            let str_data = std::str::from_utf8(&buffer[..bytes_read])?;

                            info!("Received (bi) '{str_data}' from client");

                            if str_data == "open" {
                                let mut opened_stream = connection.open_bi().await?.await?;
                                opened_stream.0.write_all(b"opened").await?;
                            } else {
                                stream.0.write_all(&buffer[..bytes_read]).await?;
                            }

                            // Drain to EOF so dropping the stream doesn't implicitly send STOP_SENDING.
                            while stream.1.read(&mut buffer).await?.is_some() {}
                        }
                        stream = connection.accept_uni() => {
                            let mut stream = stream?;
                            info!("Accepted UNI stream");

                            let Some(bytes_read) = stream.read(&mut buffer).await? else {
                                continue;
                            };

                            let str_data = std::str::from_utf8(&buffer[..bytes_read])?;

                            info!("Received (uni) '{str_data}' from client");

                            if str_data == "open" {
                                let mut opened_stream = connection.open_uni().await?.await?;
                                opened_stream.write_all(b"opened").await?;
                            }

                            // Drain to EOF so dropping the stream doesn't implicitly send STOP_SENDING.
                            while stream.read(&mut buffer).await?.is_some() {}
                        }
                        dgram = connection.receive_datagram() => {
                            let dgram = dgram?;
                            let str_data = std::str::from_utf8(&dgram)?;

                            info!("Received (dgram) '{str_data}' from client");

                            connection.send_datagram(dgram.payload())?;
                        }
                    }
                }
            }

            let result = handle_incoming_session_impl(incoming_session).await;
            info!("Result: {:?}", result);
        }
    }
}

mod http {
    use super::*;
    use axum::http::header::CONTENT_TYPE;
    use axum::response::Html;
    use axum::routing::get;
    use axum::serve;
    use axum::serve::Serve;
    use axum::Router;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    pub struct HttpServer {
        serve: Serve<Router, Router>,
        local_port: u16,
    }

    impl HttpServer {
        const PORT: u16 = 8080;

        pub async fn new(cert_digest: &Sha256Digest, webtransport_port: u16) -> Result<Self> {
            let router = Self::build_router(cert_digest, webtransport_port);

            let listener =
                TcpListener::bind(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), Self::PORT))
                    .await
                    .context("Cannot bind TCP listener for HTTP server")?;

            let local_port = listener
                .local_addr()
                .context("Cannot get local port")?
                .port();

            Ok(HttpServer {
                serve: serve(listener, router),
                local_port,
            })
        }

        pub fn local_port(&self) -> u16 {
            self.local_port
        }

        pub async fn serve(self) -> Result<()> {
            info!("Server running on port {}", self.local_port());

            self.serve.await.context("HTTP server error")?;

            Ok(())
        }

        fn build_router(cert_digest: &Sha256Digest, webtransport_port: u16) -> Router {
            let cert_digest = cert_digest.fmt(Sha256DigestFmt::BytesArray);

            let root = move || async move {
                Html(
                    http_data::INDEX_DATA
                        .replace("${WEBTRANSPORT_PORT}", &webtransport_port.to_string()),
                )
            };

            let style =
                move || async move { ([(CONTENT_TYPE, "text/css")], http_data::STYLE_DATA) };

            let client = move || async move {
                (
                    [(CONTENT_TYPE, "application/javascript")],
                    http_data::CLIENT_DATA.replace("${CERT_DIGEST}", &cert_digest),
                )
            };

            Router::new()
                .route("/", get(root))
                .route("/style.css", get(style))
                .route("/client.js", get(client))
        }
    }
}

mod utils {
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::EnvFilter;

    pub fn init_logging() {
        let env_filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .from_env_lossy();

        tracing_subscriber::fmt()
            .with_target(true)
            .with_level(true)
            .with_env_filter(env_filter)
            .init();
    }
}

mod http_data {

    pub const INDEX_DATA: &str = r#"
<!doctype html>
<html lang="en">
  <title>WTransport-Example</title>
  <meta charset="utf-8">
  <script src="client.js"></script>
  <link rel="stylesheet" href="style.css">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <body>

    <h1>WTransport Example</h1>

    <div>
      <h2>Establish WebTransport connection</h2>
      <div class="input-line">
        <label for="url">URL:</label>
        <input type="text" name="url" id="url" value="https://localhost:${WEBTRANSPORT_PORT}/">
        <input type="button" id="connect" value="Connect" onclick="connect()">
      </div>
    </div>

    <div>
      <h2>Send data over WebTransport</h2>
      <form name="sending">
        <textarea name="data" id="data"></textarea>
        <div>
          <input type="radio" name="sendtype" value="datagram" id="datagram" checked>
          <label for="datagram">Send a datagram</label>
        </div>
        <div>
          <input type="radio" name="sendtype" value="unidi" id="unidi-stream">
          <label for="unidi-stream">Open a unidirectional stream</label>
        </div>
        <div>
          <input type="radio" name="sendtype" value="bidi" id="bidi-stream">
          <label for="bidi-stream">Open a bidirectional stream</label>
        </div>
        <input type="button" id="send" name="send" value="Send data" disabled onclick="sendData()">
      </form>
    </div>

    <div>
      <h2>Event log</h2>
      <ul id="event-log">
      </ul>
    </div>

  </body>
</html>
"#;

    pub const STYLE_DATA: &str = r#"
body {
  font-family: sans-serif;
}

h1 {
  margin: 0 auto;
  width: fit-content;
}

h2 {
  border-bottom: 1px dotted #333;
  font-size: 120%;
  font-weight: normal;
  padding-bottom: 0.2em;
  padding-top: 0.5em;
}

code {
  background-color: #eee;
}

input[type=text], textarea {
  font-family: monospace;
}

#top {
  display: flex;
  flex-direction: row-reverse;
  flex-wrap: wrap;
  justify-content: center;
}

#explanation {
  border: 1px dotted black;
  font-size: 90%;
  height: fit-content;
  margin-bottom: 1em;
  padding: 1em;
  width: 13em;
}

#tool {
  flex-grow: 1;
  margin: 0 auto;
  max-width: 26em;
  padding: 0 1em;
  width: 26em;
}

.input-line {
  display: flex;
}

.input-line input[type=text] {
  flex-grow: 1;
  margin: 0 0.5em;
}

textarea {
  height: 3em;
  width: 100%;
}

#send {
  margin-top: 0.5em;
  width: 15em;
}

#event-log {
  border: 1px dotted black;
  font-family: monospace;
  height: 12em;
  overflow: scroll;
  padding-bottom: 1em;
  padding-top: 1em;
}

.log-error {
  color: darkred;
}

#explanation ul {
  padding-left: 1em;
}
"#;

    pub const CLIENT_DATA: &str = r#"
// Adds an entry to the event log on the page, optionally applying a specified
// CSS class.

const HASH = new Uint8Array(${CERT_DIGEST});

let currentTransport, streamNumber, currentTransportDatagramWriter;

// "Connect" button handler.
async function connect() {
  const url = document.getElementById('url').value;
  try {
    var transport = new WebTransport(url, { serverCertificateHashes: [ { algorithm: "sha-256", value: HASH.buffer } ] } );
    addToEventLog('Initiating connection...');
  } catch (e) {
    addToEventLog('Failed to create connection object. ' + e, 'error');
    return;
  }

  try {
    await transport.ready;
    addToEventLog('Connection ready.');
  } catch (e) {
    addToEventLog('Connection failed. ' + e, 'error');
    return;
  }

  transport.closed
      .then(() => {
        addToEventLog('Connection closed normally.');
      })
      .catch(() => {
        addToEventLog('Connection closed abruptly.', 'error');
      });

  currentTransport = transport;
  streamNumber = 1;
  try {
    currentTransportDatagramWriter = transport.datagrams.writable.getWriter();
    addToEventLog('Datagram writer ready.');
  } catch (e) {
    addToEventLog('Sending datagrams not supported: ' + e, 'error');
    return;
  }
  readDatagrams(transport);
  acceptUnidirectionalStreams(transport);
  document.forms.sending.elements.send.disabled = false;
  document.getElementById('connect').disabled = true;
}

// "Send data" button handler.
async function sendData() {
  let form = document.forms.sending.elements;
  let encoder = new TextEncoder('utf-8');
  let rawData = sending.data.value;
  let data = encoder.encode(rawData);
  let transport = currentTransport;
  try {
    switch (form.sendtype.value) {
      case 'datagram':
        await currentTransportDatagramWriter.write(data);
        addToEventLog('Sent datagram: ' + rawData);
        break;
      case 'unidi': {
        let stream = await transport.createUnidirectionalStream();
        let writer = stream.getWriter();
        await writer.write(data);
        await writer.close();
        addToEventLog('Sent a unidirectional stream with data: ' + rawData);
        break;
      }
      case 'bidi': {
        let stream = await transport.createBidirectionalStream();
        let number = streamNumber++;
        readFromIncomingStream(stream.readable, number);

        let writer = stream.writable.getWriter();
        await writer.write(data);
        await writer.close();
        addToEventLog(
            'Opened bidirectional stream #' + number +
            ' with data: ' + rawData);
        break;
      }
    }
  } catch (e) {
    addToEventLog('Error while sending data: ' + e, 'error');
  }
}

// Reads datagrams from |transport| into the event log until EOF is reached.
async function readDatagrams(transport) {
  try {
    var reader = transport.datagrams.readable.getReader();
    addToEventLog('Datagram reader ready.');
  } catch (e) {
    addToEventLog('Receiving datagrams not supported: ' + e, 'error');
    return;
  }
  let decoder = new TextDecoder('utf-8');
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        addToEventLog('Done reading datagrams!');
        return;
      }
      let data = decoder.decode(value);
      addToEventLog('Datagram received: ' + data);
    }
  } catch (e) {
    addToEventLog('Error while reading datagrams: ' + e, 'error');
  }
}

async function acceptUnidirectionalStreams(transport) {
  let reader = transport.incomingUnidirectionalStreams.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        addToEventLog('Done accepting unidirectional streams!');
        return;
      }
      let stream = value;
      let number = streamNumber++;
      addToEventLog('New incoming unidirectional stream #' + number);
      readFromIncomingStream(stream, number);
    }
  } catch (e) {
    addToEventLog('Error while accepting streams: ' + e, 'error');
  }
}

async function readFromIncomingStream(stream, number) {
  let decoder = new TextDecoderStream('utf-8');
  let reader = stream.pipeThrough(decoder).getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        addToEventLog('Stream #' + number + ' closed');
        return;
      }
      let data = value;
      addToEventLog('Received data on stream #' + number + ': ' + data);
    }
  } catch (e) {
    addToEventLog(
        'Error while reading from stream #' + number + ': ' + e, 'error');
    addToEventLog('    ' + e.message);
  }
}

function addToEventLog(text, severity = 'info') {
  let log = document.getElementById('event-log');
  let mostRecentEntry = log.lastElementChild;
  let entry = document.createElement('li');
  entry.innerText = text;
  entry.className = 'log-' + severity;
  log.appendChild(entry);

  // If the most recent entry in the log was visible, scroll the log to the
  // newly added element.
  if (mostRecentEntry != null &&
      mostRecentEntry.getBoundingClientRect().top <
          log.getBoundingClientRect().bottom) {
    entry.scrollIntoView();
  }
}
"#;
}
