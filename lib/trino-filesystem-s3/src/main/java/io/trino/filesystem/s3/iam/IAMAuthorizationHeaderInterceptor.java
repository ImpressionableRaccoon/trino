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

import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

public record IAMAuthorizationHeaderInterceptor() implements ExecutionInterceptor
{
    private static final IAMMetadataCredentialsProvider CREDENTIALS_PROVIDER = new IAMMetadataCredentialsProvider();

    @Override
    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes)
    {
        IAMCredentials credentials = (IAMCredentials) CREDENTIALS_PROVIDER.resolveCredentials();

        if (credentials == null) {
            return context.httpRequest();
        }

        return context.httpRequest().toBuilder()
                .putHeader("X-YaCloud-SubjectToken", credentials.iamToken())
                .build();
    }
}
