/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ResponseTimeoutException;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.ClosedSessionException;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import io.netty.util.AsciiString;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.http.LogThrottlingRejectHandler;
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.HttpBasicArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBufferConfig;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodecConfig;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HTTPSourceTest {
    /**
     * TODO: according to the new coding guideline, consider refactoring the following test cases into HTTPSourceIT.
     * - testHTTPJsonResponse200()
     * - testHTTPJsonResponse400()
     * - testHTTPJsonResponse413()
     * - testHTTPJsonResponse415()
     * - testHTTPJsonResponse429()
     * - testHTTPSJsonResponse()
     */
    private final String PLUGIN_NAME = "http";
    private final String TEST_PIPELINE_NAME = "test_pipeline";
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();
    private static final String CLOUDWATCH_LOGS_SAMPLE = "/cloudwatch-logs-sample.json";

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private CompletableFuture<Void> completableFuture;

    @Mock
    private CertificateProviderFactory certificateProviderFactory;

    @Mock
    private CertificateProvider certificateProvider;

    @Mock
    private Certificate certificate;

    @Mock
    private PipelineDescription pipelineDescription;

    private BlockingBuffer<Record<Log>> testBuffer;
    private HTTPSource HTTPSourceUnderTest;
    private List<Measurement> requestsReceivedMeasurements;
    private List<Measurement> successRequestsMeasurements;
    private List<Measurement> requestTimeoutsMeasurements;
    private List<Measurement> badRequestsMeasurements;
    private List<Measurement> requestsTooLargeMeasurements;
    private List<Measurement> rejectedRequestsMeasurements;
    private List<Measurement> requestProcessDurationMeasurements;
    private List<Measurement> payloadSizeSummaryMeasurements;
    private List<Measurement> serverConnectionsMeasurements;
    private HTTPSourceConfig sourceConfig;
    private PluginMetrics pluginMetrics;
    private PluginFactory pluginFactory;

    private BlockingBuffer<Record<Log>> getBuffer(final int bufferSize, final int batchSize) throws JsonProcessingException {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", bufferSize);
        integerHashMap.put("batch_size", batchSize);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(integerHashMap);
        BlockingBufferConfig blockingBufferConfig = objectMapper.readValue(json, BlockingBufferConfig.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(blockingBufferConfig, pipelineDescription);
    }

    /**
     * This method should be invoked after {@link HTTPSource::start(Buffer<T> buffer)} to scrape metrics
     */
    private void refreshMeasurements() {
        final String metricNamePrefix = new StringJoiner(MetricNames.DELIMITER)
                .add(TEST_PIPELINE_NAME).add(PLUGIN_NAME).toString();
        requestsReceivedMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogHTTPService.REQUESTS_RECEIVED).toString());
        successRequestsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogHTTPService.SUCCESS_REQUESTS).toString());
        requestTimeoutsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(HttpRequestExceptionHandler.REQUEST_TIMEOUTS).toString());
        badRequestsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(HttpRequestExceptionHandler.BAD_REQUESTS).toString());
        requestsTooLargeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(HttpRequestExceptionHandler.REQUESTS_TOO_LARGE).toString());
        rejectedRequestsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogThrottlingRejectHandler.REQUESTS_REJECTED).toString());
        requestProcessDurationMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogHTTPService.REQUEST_PROCESS_DURATION).toString());
        payloadSizeSummaryMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogHTTPService.PAYLOAD_SIZE).toString());
        serverConnectionsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(HTTPSource.SERVER_CONNECTIONS).toString());
    }

    private byte[] createGZipCompressedPayload(final String payload) throws IOException {
        // Create a GZip compressed request body
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (final GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            gzipStream.write(payload.getBytes(StandardCharsets.UTF_8));
        }
        return byteStream.toByteArray();
    }

    @BeforeEach
    public void setUp() throws JsonProcessingException {
        lenient().when(serverBuilder.annotatedService(any())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        sourceConfig = mock(HTTPSourceConfig.class);
        lenient().when(sourceConfig.getPort()).thenReturn(2021);
        lenient().when(sourceConfig.getPath()).thenReturn(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI);
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(10_000);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(200);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        lenient().when(sourceConfig.hasHealthCheckService()).thenReturn(true);
        lenient().when(sourceConfig.getCompression()).thenReturn(CompressionOption.NONE);

        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        pluginFactory = mock(PluginFactory.class);
        final ArmeriaHttpAuthenticationProvider authenticationProvider = new HttpBasicArmeriaHttpAuthenticationProvider(new HttpBasicAuthenticationConfig("test", "test"));
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);

        testBuffer = getBuffer(1, 1);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    @AfterEach
    public void cleanUp() {
        if (HTTPSourceUnderTest != null) {
            HTTPSourceUnderTest.stop();
        }
    }

    private void assertSecureResponseWithStatusCode(final AggregatedHttpResponse response, final HttpStatus expectedStatus) {
        assertThat("Http Status", response.status(), equalTo(expectedStatus));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(contains("server")));
    }

    @Test
    public void testHTTPJsonResponse200() {
        // Prepare
        final String testData = "[{\"log\": \"somelog\"}]";
        final int testPayloadSize = testData.getBytes().length;
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Then
        assertFalse(testBuffer.isEmpty());

        final Map.Entry<Collection<Record<Log>>, CheckpointState> result = testBuffer.read(100);
        List<Record<Log>> records = new ArrayList<>(result.getKey());
        assertEquals(1, records.size());
        final Record<Log> record = records.get(0);
        assertEquals("somelog", record.getData().get("log", String.class));
        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        assertEquals(1.0, successRequestsCount.getValue());
        final Measurement requestProcessDurationCount = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestProcessDurationCount.getValue());
        final Measurement requestProcessDurationMax = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.MAX);
        assertTrue(requestProcessDurationMax.getValue() > 0);
        final Measurement payloadSizeMax = MetricsTestUtil.getMeasurementFromList(
                payloadSizeSummaryMeasurements, Statistic.MAX);
        assertEquals(testPayloadSize, payloadSizeMax.getValue());
    }

    @Test
    public void testHttpCompressionResponse200() throws IOException {
        // Prepare
        final String testData = "[{\"log\": \"somelog\"}]";
        when(sourceConfig.getCompression()).thenReturn(CompressionOption.GZIP);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.POST)
                                .path("/log/ingest")
                                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                                .build(),
                        createGZipCompressedPayload(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Then
        assertFalse(testBuffer.isEmpty());

        final Map.Entry<Collection<Record<Log>>, CheckpointState> result = testBuffer.read(100);
        List<Record<Log>> records = new ArrayList<>(result.getKey());
        assertEquals(1, records.size());
        final Record<Log> record = records.get(0);
        assertEquals("somelog", record.getData().get("log", String.class));
        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        assertEquals(1.0, successRequestsCount.getValue());
        final Measurement requestProcessDurationCount = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestProcessDurationCount.getValue());
        final Measurement requestProcessDurationMax = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.MAX);
        assertTrue(requestProcessDurationMax.getValue() > 0);
    }

    @Test
    public void testHealthCheck() {
        // Prepare
        HTTPSourceUnderTest.start(testBuffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.GET)
                                .path("/health")
                                .build())
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
    }

    @Test
    public void testHealthCheckUnauthenticatedDisabled() {
        // Prepare
        when(sourceConfig.isUnauthenticatedHealthCheck()).thenReturn(false);
        when(sourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                    "username", "test",
                    "password", "test"
                )));
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        HTTPSourceUnderTest.start(testBuffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.UNAUTHORIZED)).join();
    }

    @Test
    public void testHTTPJsonResponse400() {
        // Prepare
        final String testBadData = "}";
        final int testPayloadSize = testBadData.getBytes().length;
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8(testBadData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.BAD_REQUEST)).join();

        // Then
        assertTrue(testBuffer.isEmpty());
        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement badRequestsCount = MetricsTestUtil.getMeasurementFromList(
                badRequestsMeasurements, Statistic.COUNT);
        assertEquals(1.0, badRequestsCount.getValue());
    }

    @Test
    public void testHTTPJsonResponse413() throws InterruptedException {
        // Prepare
        final String testData = "[{\"log\": \"test log 1\"}, {\"log\": \"test log 2\"}]";
        final int testPayloadSize = testData.getBytes().length;
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.REQUEST_ENTITY_TOO_LARGE)).join();

        // Then
        assertTrue(testBuffer.isEmpty());
        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        assertEquals(0.0, successRequestsCount.getValue());
        final Measurement requestsTooLargeCount = MetricsTestUtil.getMeasurementFromList(
                requestsTooLargeMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestsTooLargeCount.getValue());
        final Measurement requestProcessDurationCount = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestProcessDurationCount.getValue());
        final Measurement requestProcessDurationMax = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.MAX);
        assertTrue(requestProcessDurationMax.getValue() > 0);
        final Measurement payloadSizeMax = MetricsTestUtil.getMeasurementFromList(
                payloadSizeSummaryMeasurements, Statistic.MAX);
        assertEquals(testPayloadSize, payloadSizeMax.getValue());
    }

    @Test
    public void testHTTPJsonResponse408() {
        // Prepare
        final int testMaxPendingRequests = 1;
        final int testThreadCount = 1;
        final int serverTimeoutInMillis = 500;
        final int bufferTimeoutInMillis = 400;
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(serverTimeoutInMillis);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(bufferTimeoutInMillis);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(testMaxPendingRequests);
        when(sourceConfig.getThreadCount()).thenReturn(testThreadCount);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        // Start the source
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();
        final RequestHeaders testRequestHeaders = RequestHeaders.builder().scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:2021")
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.ofUtf8("[{\"log\": \"somelog\"}]");

        // Fill in the buffer
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Disable client timeout
        WebClient testWebClient = WebClient.builder().responseTimeoutMillis(0).build();

        // When/Then
        testWebClient.execute(testRequestHeaders, testHttpData)
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.REQUEST_TIMEOUT)).join();
        // verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        assertEquals(2.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        assertEquals(1.0, successRequestsCount.getValue());
        final Measurement requestTimeoutsCount = MetricsTestUtil.getMeasurementFromList(
                requestTimeoutsMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestTimeoutsCount.getValue());
        final Measurement requestProcessDurationMax = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.MAX);
        final double maxDurationInMillis = 1000 * requestProcessDurationMax.getValue();
        assertTrue(maxDurationInMillis > bufferTimeoutInMillis);
    }

    @Test
    public void testHTTPJsonResponse429() throws InterruptedException {
        // Prepare
        final int testMaxPendingRequests = 1;
        final int testThreadCount = 1;
        final int clientTimeoutInMillis = 100;
        final int serverTimeoutInMillis = (testMaxPendingRequests + testThreadCount + 1) * clientTimeoutInMillis;
        final Random rand = new Random();
        final double randomFactor = rand.nextDouble() + 1.5;
        final int requestTimeoutInMillis = (int)(serverTimeoutInMillis * randomFactor);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(requestTimeoutInMillis);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(serverTimeoutInMillis);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(testMaxPendingRequests);
        when(sourceConfig.getThreadCount()).thenReturn(testThreadCount);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        // Start the source
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();
        final RequestHeaders testRequestHeaders = RequestHeaders.builder().scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:2021")
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.ofUtf8("[{\"log\": \"somelog\"}]");

        // Fill in the buffer
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Send requests to throttle the server when buffer is full
        // Set the client timeout to be less than source serverTimeoutInMillis / (testMaxPendingRequests + testThreadCount)
        WebClient testWebClient = WebClient.builder().responseTimeoutMillis(clientTimeoutInMillis).build();
        for (int i = 0; i < testMaxPendingRequests + testThreadCount; i++) {
            CompletionException actualException = assertThrows(
                    CompletionException.class, () -> testWebClient.execute(testRequestHeaders, testHttpData).aggregate().join());
            assertThat(actualException.getCause(), instanceOf(ResponseTimeoutException.class));
        }

        // When/Then
        testWebClient.execute(testRequestHeaders, testHttpData).aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.TOO_MANY_REQUESTS)).join();

        // Wait until source server timeout a request processing thread
        Thread.sleep(serverTimeoutInMillis);
        // New request should timeout instead of being rejected
        CompletionException actualException = assertThrows(
                CompletionException.class, () -> testWebClient.execute(testRequestHeaders, testHttpData).aggregate().join());
        assertThat(actualException.getCause(), instanceOf(ResponseTimeoutException.class));
        // verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        assertEquals(testMaxPendingRequests + testThreadCount + 2, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        assertEquals(1.0, successRequestsCount.getValue());
        final Measurement rejectedRequestsCount = MetricsTestUtil.getMeasurementFromList(
                rejectedRequestsMeasurements, Statistic.COUNT);
        assertEquals(1.0, rejectedRequestsCount.getValue());
    }

    @Test
    public void testServerConnectionsMetric() throws InterruptedException {
        // Prepare
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // Verify connections metric value is 0
        Measurement serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(0, serverConnectionsMeasurement.getValue());

        final RequestHeaders testRequestHeaders = RequestHeaders.builder().scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:2021")
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.ofUtf8("[{\"log\": \"somelog\"}]");

        // Send request
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Verify connections metric value is 1
        serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(1.0, serverConnectionsMeasurement.getValue());
    }

    @Test
    public void testServerStartCertFileSuccess() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(server.stop()).thenReturn(completableFuture);

            final Path certFilePath = new File(TEST_SSL_CERTIFICATE_FILE).toPath();
            final Path keyFilePath = new File(TEST_SSL_KEY_FILE).toPath();
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);

            when(sourceConfig.isSsl()).thenReturn(true);
            when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
            when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
            HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
            HTTPSourceUnderTest.start(testBuffer);
            HTTPSourceUnderTest.stop();

            final ArgumentCaptor<InputStream> certificateIs = ArgumentCaptor.forClass(InputStream.class);
            final ArgumentCaptor<InputStream> privateKeyIs = ArgumentCaptor.forClass(InputStream.class);
            verify(serverBuilder).tls(certificateIs.capture(), privateKeyIs.capture());
            final String actualCertificate = IOUtils.toString(certificateIs.getValue(), StandardCharsets.UTF_8.name());
            final String actualPrivateKey = IOUtils.toString(privateKeyIs.getValue(), StandardCharsets.UTF_8.name());
            assertThat(actualCertificate, is(certAsString));
            assertThat(actualPrivateKey, is(keyAsString));
        }
    }

    @Test
    public void testServerStartCertFileMissing() {
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(null);
        when(sourceConfig.getSslKeyFile()).thenReturn(null);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            assertThrows(NullPointerException.class, () -> HTTPSourceUnderTest.start(testBuffer));
        }
    }

    @Test
    void testServerStartACMCertSuccess() throws IOException, NoSuchFieldException, IllegalAccessException {
        final Path certFilePath = new File(TEST_SSL_CERTIFICATE_FILE).toPath();
        final Path keyFilePath = new File(TEST_SSL_KEY_FILE).toPath();
        final String certAsString = Files.readString(certFilePath);
        final String keyAsString = Files.readString(keyFilePath);

        when(certificate.getCertificate()).thenReturn(certAsString);
        when(certificate.getPrivateKey()).thenReturn(keyAsString);
        when(certificateProvider.getCertificate()).thenReturn(certificate);
        when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
        when(sourceConfig.isSsl()).thenReturn(true);
        when(server.stop()).thenReturn(completableFuture);

        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        Field field = HTTPSourceUnderTest.getClass().getDeclaredField("certificateProviderFactory");
        field.setAccessible(true);
        field.set(HTTPSourceUnderTest, certificateProviderFactory);

        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            HTTPSourceUnderTest.start(testBuffer);
        }
        HTTPSourceUnderTest.stop();

        final ArgumentCaptor<InputStream> certificateIs = ArgumentCaptor.forClass(InputStream.class);
        final ArgumentCaptor<InputStream> privateKeyIs = ArgumentCaptor.forClass(InputStream.class);
        verify(serverBuilder).tls(certificateIs.capture(), privateKeyIs.capture());
        final String actualCertificate = IOUtils.toString(certificateIs.getValue(), StandardCharsets.UTF_8.name());
        final String actualPrivateKey = IOUtils.toString(privateKeyIs.getValue(), StandardCharsets.UTF_8.name());
        assertThat(actualCertificate, is(certAsString));
        assertThat(actualPrivateKey, is(keyAsString));
    }

    @Test
    void testServerStartACMCertNull() throws NoSuchFieldException, IllegalAccessException {
        when(certificate.getCertificate()).thenReturn(null);
        when(certificateProvider.getCertificate()).thenReturn(certificate);
        when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
        when(sourceConfig.isSsl()).thenReturn(true);

        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        Field field = HTTPSourceUnderTest.getClass().getDeclaredField("certificateProviderFactory");
        field.setAccessible(true);
        field.set(HTTPSourceUnderTest, certificateProviderFactory);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            assertThrows(NullPointerException.class, () -> HTTPSourceUnderTest.start(testBuffer));
        }
    }



    @Test
    void testHTTPSJsonResponse() throws JsonProcessingException {
        reset(sourceConfig);
        when(sourceConfig.getPort()).thenReturn(2021);
        when(sourceConfig.getPath()).thenReturn(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI);
        when(sourceConfig.getThreadCount()).thenReturn(200);
        when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(200);
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        testBuffer = getBuffer(1, 1);
        HTTPSourceUnderTest.start(testBuffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8("[{\"log\": \"somelog\"}]"))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
    }


    @Test
    void testHTTPRequestWhenSSLRequiredNoResponse() throws JsonProcessingException {
        reset(sourceConfig);
        when(sourceConfig.getPort()).thenReturn(2021);
        when(sourceConfig.getPath()).thenReturn(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI);
        when(sourceConfig.getThreadCount()).thenReturn(200);
        when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(200);
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        testBuffer = getBuffer(1, 1);
        HTTPSourceUnderTest.start(testBuffer);

        CompletableFuture<AggregatedHttpResponse> future = WebClient.builder()
                .factory(ClientFactory.insecure())
                .build()
                .execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.POST)
                                .path("/log/ingest")
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8("[{\"log\": \"somelog\"}]"))
                .aggregate();

        ExecutionException exception = assertThrows(ExecutionException.class,
                () -> future.get(2, TimeUnit.SECONDS)
        );
        assertInstanceOf(ClosedSessionException.class, exception.getCause());
    }

    @Test
    void testHTTPSJsonResponse_with_custom_path_along_with_placeholder() throws JsonProcessingException {
        reset(sourceConfig);
        when(sourceConfig.getPort()).thenReturn(2021);
        when(sourceConfig.getPath()).thenReturn("/${pipelineName}/test");
        when(sourceConfig.getThreadCount()).thenReturn(200);
        when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(200);
        when(sourceConfig.isSsl()).thenReturn(true);

        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        testBuffer = getBuffer(1, 1);
        HTTPSourceUnderTest.start(testBuffer);

        final String path = "/" + TEST_PIPELINE_NAME + "/test";

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTPS)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.POST)
                                .path(path)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8("[{\"log\": \"somelog\"}]"))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
    }

    @Test
    public void testDoubleStart() {
        // starting server
        HTTPSourceUnderTest.start(testBuffer);
        // double start server
        assertThrows(IllegalStateException.class, () -> HTTPSourceUnderTest.start(testBuffer));
    }

    @Test
    public void testStartWithEmptyBuffer() {
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(testBuffer));
        }
    }

    @Test
    public void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, () -> source.start(testBuffer));
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(testBuffer));
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);

            // When/Then
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));
            assertThrows(RuntimeException.class, source::stop);
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, source::stop);
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, source::stop);
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        // starting server
        HTTPSourceUnderTest.start(testBuffer);

        final HTTPSource secondSource = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        assertThrows(RuntimeException.class, () -> secondSource.start(testBuffer));
    }

    @Test
    public void request_that_exceeds_maxRequestLength_returns_413() {
        lenient().when(sourceConfig.getMaxRequestLength()).thenReturn(ByteCount.ofBytes(4));
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        // Prepare
        final String testData = "[{\"log\": \"somelog\"}]";

        assertThat((long) testData.getBytes().length, greaterThan(sourceConfig.getMaxRequestLength().getBytes()));
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.POST)
                                .path("/log/ingest")
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.REQUEST_ENTITY_TOO_LARGE)).join();

        // Then
        assertTrue(testBuffer.isEmpty());
    }

    @Test
    public void testHTTPJsonCodec() throws IOException {
        // Prepare
        final String testData;
        try (InputStream inputStream = this.getClass().getResourceAsStream(CLOUDWATCH_LOGS_SAMPLE)) {
            testData = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        final int testPayloadSize = testData.getBytes().length;

        when(sourceConfig.getCodec()).thenReturn(mock(PluginModel.class));
        when(sourceConfig.getMaxRequestLength()).thenReturn(ByteCount.ofBytes(2048000));

        ObjectMapper mapper = new ObjectMapper();
        final String configString = "{\"key_name\": \"logEvents\",\"include_keys\":[\"owner\",\"logGroup\",\"logStream\"]}";
        final JsonInputCodecConfig codecConfig = mapper.readValue(configString, JsonInputCodecConfig.class);

        final InputCodec codec = new JsonInputCodec(codecConfig);

        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class))).thenReturn(codec);

        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        testBuffer = getBuffer(5, 5);
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.POST)
                                .path("/log/ingest")
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Then
        assertFalse(testBuffer.isEmpty());

        final Map.Entry<Collection<Record<Log>>, CheckpointState> result = testBuffer.read(100);
        List<Record<Log>> records = new ArrayList<>(result.getKey());
        assertEquals(3, records.size());

        // Verify content
        final Record<Log> record = records.get(0);
        assertCommonFields(record);
        assertEquals("31953106606966983378809025079804211143289615424298221568", record.getData().get("id", String.class));
        assertEquals(1432826855000L, record.getData().get("timestamp", Long.class));
        assertEquals("{\"eventVersion\":\"1.01\",\"userIdentity\":{\"type\":\"Root\"}", record.getData().get("message", String.class));

        final Record<Log> record2 = records.get(1);
        assertCommonFields(record2);
        assertEquals("31953106606966983378809025079804211143289615424298221569", record2.getData().get("id", String.class));
        assertEquals(1432826855001L, record2.getData().get("timestamp", Long.class));
        assertEquals("{\"eventVersion\":\"1.02\",\"userIdentity\":{\"type\":\"Root\"}", record2.getData().get("message", String.class));

        final Record<Log> record3 = records.get(2);
        assertCommonFields(record3);
        assertEquals("31953106606966983378809025079804211143289615424298221570", record3.getData().get("id", String.class));
        assertEquals(1432826855002L, record3.getData().get("timestamp", Long.class));
        assertEquals("{\"eventVersion\":\"1.03\",\"userIdentity\":{\"type\":\"Root\"}", record3.getData().get("message", String.class));

        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        assertEquals(1.0, successRequestsCount.getValue());
        final Measurement requestProcessDurationCount = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestProcessDurationCount.getValue());
        final Measurement requestProcessDurationMax = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.MAX);
        assertTrue(requestProcessDurationMax.getValue() > 0);
        final Measurement payloadSizeMax = MetricsTestUtil.getMeasurementFromList(
                payloadSizeSummaryMeasurements, Statistic.MAX);
        assertEquals(testPayloadSize, payloadSizeMax.getValue());
    }

    private void assertCommonFields(Record<Log> record) {
        assertEquals("111111111111", record.getData().get("owner", String.class));
        assertEquals("CloudTrail/logs", record.getData().get("logGroup", String.class));
        assertEquals("111111111111_CloudTrail/logs_us-east-1", record.getData().get("logStream", String.class));
    }
}
