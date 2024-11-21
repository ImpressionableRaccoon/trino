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

import software.amazon.awssdk.auth.credentials.AwsCredentials;

public record IAMCredentials(String iamToken) implements AwsCredentials
{
    private static final String accessKeyId = "YANDEX_CLOUD_FAKE_ACCESS_KEY";
    private static final String secretAccessKey = "YANDEX_CLOUD_FAKE_SECRET_KEY";

    @Override
    public String accessKeyId()
    {
        return accessKeyId;
    }

    @Override
    public String secretAccessKey()
    {
        return secretAccessKey;
    }
}
