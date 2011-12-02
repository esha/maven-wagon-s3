/*
 * Copyright (C) 2011 Christopher Elkins
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cyclopsgroup.cym2.awss3;

import java.net.URL;
import java.util.Date;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.observers.AbstractTransferListener;
import org.apache.maven.wagon.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.joda.time.DateTime.now;

/**
 * Generates presigned URLs for successful transfers.
 *
 * @author Christopher Elkins
 */
public class S3PresignedUrlTransferListener extends AbstractTransferListener {

    public static final int MILLISECONDS_PER_HOUR = 1000 * 60 * 60;

    private final Logger log =
        LoggerFactory.getLogger(S3PresignedUrlTransferListener.class);

    private final S3Wagon wagon;
    private final int hoursToExpire;

    /**
     * Creates a new instance.
     *
     * @param wagon the wagon
     * @param hoursToExpire the number of hours until expiration
     */
    public S3PresignedUrlTransferListener(
        final S3Wagon wagon, final int hoursToExpire) {

        this.wagon = checkNotNull(wagon);
        this.hoursToExpire = Math.max(hoursToExpire, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void debug(final String message) {
        this.log.debug(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void transferCompleted(final TransferEvent transferEvent) {
        if (this.hoursToExpire > 0) {
            final Date expiration = getExpiration();
            final URL presignedUrl =
                generatePresignedUrl(transferEvent.getResource(), expiration);

            final String message =
                String.format("Presigned URL (expires %tc): %s", expiration, presignedUrl);
            if (this.log.isInfoEnabled()) {
                this.log.info(message);
            } else {
                debug(message);
            }
        } else {
            debug("No presigned URL generated for " + transferEvent.getResource().getName());
        }
    }

    /**
     * Returns the expiration date.
     *
     * @return the expiration date
     */
    private Date getExpiration() {
        return now().plus(MILLISECONDS_PER_HOUR * this.hoursToExpire).toDate();
    }

    /**
     * Generates a presigned URL for the specified resource.
     *
     * @param resource the resource
     * @param expiration the expiration date
     * @return a presigned URL
     */
    private URL generatePresignedUrl(
        final Resource resource, final Date expiration) {

        assert (resource != null);
        assert (expiration != null);

        final String bucketName = this.wagon.getBucketName();
        final String key = this.wagon.getKeyPrefix() + resource.getName();
        return this.wagon.getS3().generatePresignedUrl(
            new GeneratePresignedUrlRequest(bucketName, key)
                .withExpiration(expiration));
    }

}
