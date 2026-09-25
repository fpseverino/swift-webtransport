# Contributing

### Testing

The project tests expect the WebTransport Go server in the `/go-server` directory and the Rust server in the `/rust-server` directory to be running.
You can start them by running the commands:

```sh
cd go-server && go run .
```

```sh
cd rust-server && cargo run
```

### Formatting

We use `swift-format` for formatting code.

To format the code, run the following command:

```sh
swift format --parallel --in-place --recursive .
```

To check if the code is properly formatted, you can run the following command:

```sh
swift format lint --parallel --strict --recursive .
```
