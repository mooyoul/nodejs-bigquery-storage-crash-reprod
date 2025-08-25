import { managedwriter, protos, adapt } from "@google-cloud/bigquery-storage";
import { randomUUID } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import { GoogleAuth } from "google-auth-library";
import debug from "debug";

const log = debug("nodejs-bigquery-storage-crash-reprod");
log.enabled = true; // always print debug logs

managedwriter.setLogFunction((() => {
  const instance = debug("@google-cloud/bigquery-storage");
  instance.enabled = true;

  return instance;
})());

const originalExit = process.exit;
process.exit = (code) => {
  log("process.exit called with code:", code);
  log("stack:", new Error().stack);
  return originalExit.call(process, code);
}

(async function main() {
  // The ID of the Project that contains given BigQuery dataset.
  const PROJECT_ID = mustReadEnv("PROJECT_ID");

  // The ID of the BigQuery stream to write to.
  // This should be the full stream ID, including the project and dataset.
  // For example: `projects/${PROJECT_ID}/datasets/${DATASET_ID}/tables/${TABLE_ID}/streams/_default`
  const STREAM_ID = mustReadEnv("STREAM_ID");

  // Create auth client instance
  // This example uses Application Default Credentials (ADC) to authenticate.
  const authClient = await getADCClient();

  // Create WriteClient
  const writerClient = new managedwriter.WriterClient({
    projectId: PROJECT_ID,
    authClient,
    "grpc.keepalive_permit_without_calls": 1,
    "grpc.max_connection_age_ms": 60 * 5 * 1000, // 5 minutes
    "grpc.max_connection_idle_ms": 60 * 5 * 1000, // 5 minutes
  });

  // Activate write retries
  writerClient.enableWriteRetries(true);

  // Get the write stream for the specified stream ID
  const writeStream = await writerClient.getWriteStream({
    streamId: STREAM_ID,
    view: protos.google.cloud.bigquery.storage.v1.WriteStreamView.FULL,
  });

  // Read the table schema from the write stream
  const { tableSchema } = writeStream;
  if (!tableSchema) {
    throw new Error("Unable to retrieve table schema for the destination table.");
  }

  const protoDescriptor = adapt.convertStorageSchemaToProto2Descriptor(writeStream.tableSchema, "root");

  // Create a connection to the stream
  const connection = await writerClient.createStreamConnection({
    streamId: STREAM_ID,
  });
  connection.on("error", (err) => {
    log("!!! [error] Connection error:", err);

    {
      log("writing some data to the stream right after receiving error event...");
      testWrite({ type: "write_after_error" }).finally(() => {
        log("attempted to write data to the stream after error event");
      });


      (async () => {
        await setTimeout(5000);
        log("writing some data to the stream after 5 seconds...");
        await testWrite({ type: "write_5sec_after_error" });
      })().finally(() => {
        log("attempted to write data to the stream after 5 seconds");
      })
    }

    async function testWrite(additionalProps = {}) {
      log("testWrite - process.getActiveResourcesInfo: ", process.getActiveResourcesInfo());

      try {
        log("attempting to write data to the stream...");
        const res = await jsonWriter.appendRows([ {
          event_id: randomUUID(),
          event_timestamp: new Date(),
          payload: JSON.stringify({
            foo: "bar",
            bar: true,
            baz: 123,
            qux: [ 1, 2, 3 ],
            now: Date.now(),
            ...additionalProps,
          }),
        } ]).getResult();

        log("appended rows to the stream successfully: ", res);
      } catch (e) {
        log("Error while writing to the stream:", e);
      }
    }
  });
  log("attached error event listener to connection");

  ["reconnect", "close", "pause", "resume", "schemaUpdated", "end"].forEach((eventName) => {
    connection.on(eventName, (...args) => {
      log("stream_connection triggered [%s]", eventName, ...args);
    });

    log("attached %s event listener to connection", eventName);
  });

  // Create a JSON writer instance
  const jsonWriter = new managedwriter.JSONWriter({
    connection,
    protoDescriptor,
  });

  // Listen for process signals for graceful shutdown
  // and do nothing. let it running until the signal is received.
  // normally there would be a loop to write data to the stream,
  // but for this example, we will just wait for the shutdown signal.
  log("created connection and waiting for shutdown signal...", new Date().toISOString());
  await waitForShutdownSignal(["SIGINT", "SIGTERM"]);

  // Close the connection and writer client if shutdown signal is received
  writerClient.close();

  log("closed client. process is about to exit!");
  log("process active resources (event loop): ", process.getActiveResourcesInfo());

  log("delaying process exit 5 seconds to allow for any pending operations to complete...");
  await setTimeout(5000);
  log("exiting now.");
})().catch((e) => {
  log("Error in main function:", e.stack);
  process.exit(1);
});

// Read environment variable and throw an error if not set
function mustReadEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Environment variable ${name} is not set.`);
  }
  return value;
}

async function getADCClient() {
  const auth = new GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/cloud-platform"],
  });

  return await auth.getClient();
}

// Wait for shutdown signal (SIGINT or SIGTERM)
async function waitForShutdownSignal(signals) {
  const { signal, cleanup } = await new Promise((resolve) => {
    const signalHandler = (signal) => {
      log("received signal %s", signal);
      log("stack: ", new Error(signal).stack);

      resolve({
        signal,
        cleanup,
      });
    };

    const cleanup = () => {
      signals.forEach((signal) => {
        process.removeListener(signal, signalHandler);
      });
    };

    signals.forEach((signal) => {
      process.on(signal, signalHandler);
    });
  });

  cleanup();

  return signal;
}
