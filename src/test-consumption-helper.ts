import { Consumer, EachMessageHandler } from "kafkajs";

/** Simple helper function to process messages for a consumer.
 *
 * This handles forwarding errors to jest if the message handler fails (i.e. expect fails),
 * which doesn't happen normally since the eachMessage handler is running "outside" of jest.
 *
 * In your message handler, call the given resolve() from the wrapper to indicate
 * the test case is complete.  If resolve isn't called, and you await the resturned promise,
 * the test will timeout.
 *
 * @param consumer Consumer to consume from.
 * @param eachMessageWrapper Given a resolve function and should return a message handler.
 * @return Promise that will be fulfilled when the given message handler calls resolve().
 */
export const testConsumptionHelper = <T = unknown>(
  consumer: Consumer,
  eachMessageWrapper: (resolve: (value: T) => void) => EachMessageHandler
) => {
  return new Promise<T>(
    (resolve, reject) =>
      void consumer.run({
        autoCommit: false,
        eachMessage: async (...args) => {
          const eachMessageFn = eachMessageWrapper(resolve);

          // This callback won't have errors forwarded up to jest so we have
          // to pass any errors manually.
          try {
            await eachMessageFn(...args);
          } catch (e) {
            reject(e);
          }
        },
      })
  );
};
