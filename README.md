## KafkaJS Utils

Collection of simple utils to help working with [KafkaJS](https://kafka.js.org/).

### Exactly Once Utils

`KafkaOneToNExaftlyOnceManager` - Provides a consumer set up to adhere to Exactly Once Semantics (EOS) and generates transactions that adhere to 1:N EOS operations for a given source topic and partition.

`KafakaOneToNExactlyOnceExecutor` - Provides an executor that will execute 1:N EOS operations. Just provide a processor that takes an event and returns N events.

See class documentation and test cases for more information.

### Testing

To locally run tests:

`npm run test:start-env` # Start Docker Compose env.

`npm run test` # Run tests.

`npm run test:stop-env` # Stop Docker Compose env.
