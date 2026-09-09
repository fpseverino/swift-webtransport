# Contributing

### Testing

The project tests expect the WebTransport Go server in the `/server` directory to be running.
You can start it by running the command:

```sh
cd server && go run .
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
