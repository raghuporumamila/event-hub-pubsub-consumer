package com.eventhub.pubsub.consumer;

import com.eventhub.model.Event;
import com.google.cloud.NoCredentials;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.sql.Timestamp;
import java.time.Instant;

public class PubsubPipeline {
    public interface MyOptions extends PubsubOptions {
    }
    public static void main(String[] args) throws Exception {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        //options.setRunner((Class<? extends DirectRunner<?>>) DirectRunner.class);
        // 1. Configure EVERYTHING first
        System.setProperty("com.google.auth.creds.none", "true");
        options.setPubsubRootUrl("http://127.0.0.1:8085");
        options.setProject("local-project");

        // Force NoCredentialsFactory to stop the OAuth flow
        options.setCredentialFactoryClass(NoopCredentialFactory.class);
        // Also null out the credential in the GcpOptions layer
        options.as(GcpOptions.class).setGcpCredential(null);
        // 2. Now create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // 3. Register Coders
        pipeline.getCoderRegistry().registerCoderForClass(
                Event.class,
                SerializableCoder.of(Event.class)
        );



        // 4. Build the pipeline
        String insertSQL = "INSERT INTO \"event\".\"event\" (\"name\", payload, \"timestamp\", " +
                "event_definition_id, organization_id, source_id, workspace_id) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?)";

        pipeline

        // Use .fromSubscription instead of .fromTopic
                .apply("ReadFromPubSub",
                PubsubIO.readStrings()
                        .fromSubscription("projects/local-project/subscriptions/event-hub-sub"))
                /*.apply("ReadFromPubSub",
                        PubsubIO.readStrings().fromTopic("projects/local-project/topics/event-hub-topic"))*/
                /*.apply("ParseJson", MapElements.into(TypeDescriptor.of(Event.class))
                        .via(json -> {
                                System.out.println(json);
                                return new Gson().fromJson(json, Event.class);
                        }))*/
                .apply("ParseJson", MapElements.into(TypeDescriptor.of(Event.class))
                        .via(json -> {
                            System.out.println("Input JSON: " + json);

                            Gson gson = new GsonBuilder()
                                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                                    .create();

                            return gson.fromJson(json, Event.class);
                        }))
                .apply("WriteToAlloyDB", JdbcIO.<Event>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "org.postgresql.Driver",
                                        "jdbc:postgresql://localhost:5432/eventhub")
                                .withUsername("postgres")
                                .withPassword("password"))
                        .withStatement(insertSQL)
                        .withPreparedStatementSetter((element, statement) -> {
                            try {

                                statement.setString(1, element.getName());
                                statement.setString(2, element.getPayload());
                                statement.setTimestamp(3, Timestamp.from(Instant.now()));
                                statement.setLong(4, element.getEventDefinition().getId());
                                statement.setLong(5, element.getOrganization().getId());
                                statement.setLong(6, element.getSource().getId());
                                statement.setLong(7, element.getWorkspace().getId());
                            } catch(Exception error) {
                                error.printStackTrace();
                                System.out.println(element.toString());
                            }
                        })
                );

        pipeline.run().waitUntilFinish(); // Use waitUntilFinish for local DirectRunner
    }
}
