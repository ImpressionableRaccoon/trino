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

import java.util.concurrent.ThreadLocalRandom;

/**
 * Helper class that implements Exponential Backoff, because sometimes
 * service could not return response due network issues, or something else.
 * We should retry insteadof fail entire user job.
 */

public class ExponentialRetryPolicy
{
    protected volatile int jitter;
    protected volatile int maximumBackoff;
    protected volatile int maximumRetries;

    public ExponentialRetryPolicy(int maximumBackoff, int jitter, int maximumRetries)
    {
        this.maximumBackoff = maximumBackoff;
        this.jitter = jitter;
        this.maximumRetries = maximumRetries;
    }

    public boolean retryRequest(int attempt)
    {
        if (attempt >= this.maximumRetries) {
            return false;
        }

        try {
            int jitterOffset = ThreadLocalRandom.current().nextInt(this.jitter);
            long sleepTime = Math.min(2 ^ attempt + jitterOffset, this.maximumBackoff);
            Thread.sleep(sleepTime * 1000);
        }
        catch (InterruptedException ie) {
            // Do nothing
        }

        return true;
    }
}
