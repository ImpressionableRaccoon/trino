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

import io.airlift.log.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

//public class IAMMetadataCredentialsProvider implements AwsCredentialsProvider {
//    private final IAMCredentials credentials;
//
//    private final IAMCredentialsFetcher credentialsFetcher;
//
//    public IAMMetadataCredentialsProvider(String iamToken) {
//        this.credentials = new IAMCredentials(iamToken);
//        this.credentialsFetcher = IAMCredentialsFetcher.getInstance();
//    }
//
//    @Override
//    public AwsCredentials resolveCredentials() {
//        try {
//            credentials credentialsFetcher.getCredentials()
//        }
//
//        return ;
//    }
//}

/**
 * Credentials provider implementation that loads credentials from the Yandex Cloud
 * Compute Instance Metadata Service within GCP-like API.
 */
public class IAMMetadataCredentialsProvider
        implements AwsCredentialsProvider
{
    private static final Logger log = Logger.get(IAMCredentialsFetcher.class);

    /**
     * The wait time, after which the background thread initiates a refresh to
     * load the latest credentials if needed.
     */
    private static final int ASYNC_REFRESH_INTERVAL_TIME_MINUTES = 1;

    private final IAMCredentialsFetcher credentialsFetcher;

    /**
     * The executor service used for refreshing the credentials in the
     * background.
     */
    private volatile ScheduledExecutorService executor;

    private volatile boolean shouldRefresh;

    public IAMMetadataCredentialsProvider()
    {
        this(false);
    }

    /**
     * Spins up a new thread to refresh the credentials asynchronously if
     * refreshCredentialsAsync is set to true, otherwise the credentials will be
     * refreshed from the instance metadata service synchronously,
     *
     * @param refreshCredentialsAsync
     *            true if credentials needs to be refreshed asynchronously else
     *            false.
     */
    public IAMMetadataCredentialsProvider(boolean refreshCredentialsAsync)
    {
        this(refreshCredentialsAsync, true);
    }

    /**
     * Spins up a new thread to refresh the credentials asynchronously.
     *
     * @param eagerlyRefreshCredentialsAsync
     *            when set to false will not attempt to refresh credentials asynchronously
     *            until after a call has been made to {@link #resolveCredentials()} - ensures that
     *            {@link IAMMetadataCredentialsProvider#resolveCredentials()} is only hit when this CredentialProvider is actually required
     */
    public static IAMMetadataCredentialsProvider createAsyncRefreshingProvider(final boolean eagerlyRefreshCredentialsAsync)
    {
        return new IAMMetadataCredentialsProvider(true, eagerlyRefreshCredentialsAsync);
    }

    private IAMMetadataCredentialsProvider(boolean refreshCredentialsAsync, final boolean eagerlyRefreshCredentialsAsync)
    {
        credentialsFetcher = IAMCredentialsFetcher.getInstance();
        shouldRefresh = eagerlyRefreshCredentialsAsync;
        if (refreshCredentialsAsync) {
//            executor = Executors.newScheduledThreadPool(1);
            ThreadFactory threadFactory = new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r)
                {
                    Thread thread = new Thread(r);
                    thread.setName("iam-metadata-credentials-thread-" + counter.getAndIncrement());
                    thread.setDaemon(true); // Опционально, если нужны демоны
                    return thread;
                }
            };

            executor = (ScheduledExecutorService) Executors.newFixedThreadPool(10, threadFactory);
            executor.scheduleWithFixedDelay(() -> {
                try {
                    if (shouldRefresh) {
                        credentialsFetcher.getCredentials();
                    }
                }
                catch (IAMCredentialsException | RuntimeException | Error ace) {
                    IAMMetadataCredentialsProvider.this.handleError(ace);
                }
            }, 0, ASYNC_REFRESH_INTERVAL_TIME_MINUTES, TimeUnit.MINUTES);
        }
    }

    private void handleError(Throwable t)
    {
        refresh();
        log.error(t.getMessage(), t);
    }

    private void refresh()
    {
        credentialsFetcher.refresh();
    }

    @Override
    public AwsCredentials resolveCredentials()
    {
        IAMCredentials creds = null;
        try {
            creds = (IAMCredentials) credentialsFetcher.getCredentials();
        }
        catch (IAMCredentialsException e) {
            e.fillInStackTrace();
        }
        shouldRefresh = true;
        return creds;
    }
}
