/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.s3.iam;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Helper class that contains the common behavior of the
 * CredentialsProviders that loads the credentials from a
 * local endpoint on a Compute instance
 */

public class IAMCredentialsFetcher
{
    private static final Logger log = Logger.get(IAMCredentialsFetcher.class);

    /**
     * The threshold after the last attempt to load credentials (in
     * milliseconds) at which credentials are attempted to be refreshed.
     */
    private static final int REFRESH_THRESHOLD = 1000 * 60 * 60;

    /**
     * The threshold before credentials expire (in milliseconds) at which
     * this class will attempt to load new credentials.
     */
    private static final int EXPIRATION_THRESHOLD = 1000 * 60 * 15;

    /**
     * The attempts count before failing fetchCredentials method
     */
    private static final int MAX_RETRIES = 8;

    /**
     * Maximum backoff of delay between multiple attempts for fetchCredentials
     */
    private static final int MAX_BACKOFF_DELAY = 64;

    /**
     * Jitter, random delay for spread attempts between many instances
     */
    private static final int JITTER = 10;

    /**
     * Maximum tcp sessions to metadata service
     */
    private static final int MAX_SESSIONS = 10;

    /** The host of metadata service. */
    private static final HttpHost HOST = getMetadataServiceHost();

    private static HttpHost getMetadataServiceHost()
    {
        String customMetadataAddr = System.getenv("YC_METADATA_ADDR");
        if (customMetadataAddr == null || customMetadataAddr.isEmpty()) {
            return new HttpHost("169.254.169.254");
        }
        return new HttpHost(customMetadataAddr);
    }

    /** The URI of IAM token.*/
    private static final String TOKEN_URI = "/computeMetadata/v1/instance/service-accounts/default/token";

    /** The current instance profile credentials */
    private volatile IAMCredentials credentials;

    /** The expiration for the current instance profile credentials */
    private volatile Instant credentialsExpiration;

    /** The time of the last attempt to check for new credentials */
    protected volatile Instant lastInstanceProfileCheck;

    /** Pooled http client **/
    private final CloseableHttpClient client;

    /** Custom retryHandler **/
    private final ExponentialRetryPolicy retryHandler;

    private static final IAMCredentialsFetcher INSTANCE = new IAMCredentialsFetcher();

    public static IAMCredentialsFetcher getInstance()
    {
        return INSTANCE;
    }

    private IAMCredentialsFetcher()
    {
        this.retryHandler = new ExponentialRetryPolicy(MAX_BACKOFF_DELAY, JITTER, MAX_RETRIES);
        this.client = HttpClients
                .custom()
                .disableAutomaticRetries() // We will be use retry policy on higher level
                .evictExpiredConnections()
                .evictIdleConnections(5, TimeUnit.MINUTES)
                .setMaxConnTotal(MAX_SESSIONS)
                .build();
    }

    public AwsCredentials getCredentials()
            throws IAMCredentialsException
    {
        if (needsToLoadCredentials()) {
            fetchCredentials();
        }
        if (expired()) {
            log.error("Received expired credentials from metadata");
            throw new IAMCredentialsException(
                    "The credentials received have been expired");
        }
        return credentials;
    }

    /**
     * Returns true if credentials are null, credentials are within expiration or
     * if the last attempt to refresh credentials is beyond the refresh threshold.
     */
    boolean needsToLoadCredentials()
    {
        return (credentials == null) ||
                isWithinExpirationThreshold() ||
                isPastRefreshThreshold();
    }

    void updateCredentialsFromStream(InputStream responseStream)
            throws IOException, IAMCredentialsException
    {
        /* Example of response
         * https://cloud.yandex.ru/docs/compute/operations/vm-connect/auth-inside-vm#auth-inside-vm
         * {"access_token":"...","expires_in":42653,"token_type":"Bearer"}
         */

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(responseStream);
        JsonNode token = node.get("access_token");
        if (token == null) {
            throw new IAMCredentialsException("response does not contain token");
        }
        credentials = new IAMCredentials(token.asText());
        JsonNode expiresIn = node.get("expires_in");
        if (expiresIn != null) {
            credentialsExpiration = Instant.now().plusSeconds(expiresIn.asLong());
        }
    }

    /**
     * Fetches the credentials from the endpoint.
     */
    private synchronized void fetchCredentials()
            throws IAMCredentialsException
    {
        if (!needsToLoadCredentials()) {
            return;
        }
        lastInstanceProfileCheck = Instant.now();

        for (int attempts = 0;; attempts++) {
            try {
                HttpUriRequest request = RequestBuilder.get()
                        .addHeader("Metadata-Flavor", "Google")
                        .setUri(TOKEN_URI)
                        .build();
                try (CloseableHttpResponse response = this.client.execute(HOST, request)) {
                    updateCredentialsFromStream(response.getEntity().getContent());
                }
                return;
            }
            catch (Exception exception) {
                log.warn("Unable to load credentials from metadata server, attempt#" + attempts, exception);
                if (!this.retryHandler.retryRequest(attempts)) {
                    log.error("Failed to get IAM-token with retries");
                    throw new IAMCredentialsException("Failed to get IAM-token with retries", exception);
                }
            }
        }
    }

    public void refresh()
    {
        credentials = null;
    }

    /**
     * Returns true if the current credentials are within the expiration
     * threshold, and therefore, should be refreshed.
     */
    private boolean isWithinExpirationThreshold()
    {
        return credentialsExpiration != null &&
                credentialsExpiration.minusMillis(EXPIRATION_THRESHOLD).isBefore(Instant.now());
    }

    /**
     * Returns true if the last attempt to refresh credentials is beyond the
     * refresh threshold, and therefore the credentials should attempt to be
     * refreshed.
     */
    private boolean isPastRefreshThreshold()
    {
        return lastInstanceProfileCheck != null &&
                Instant.now().minusMillis(REFRESH_THRESHOLD).isAfter(lastInstanceProfileCheck);
    }

    private boolean expired()
    {
        return credentialsExpiration != null && credentialsExpiration.isBefore(Instant.now());
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
