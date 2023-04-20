/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.acknowledgements;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;

/**
 * AcknowledgmentSet keeps track of set of events that
 * belong to the batch of events that a source creates.
 * Each event gets an event handle assigned when it is first
 * added to the acknowledgement set, which can later be used
 * to acquire more references to the event or release reference
 * to the event when the event is completed (pushed to sink,
 * dropped, etc)
 */
public interface AcknowledgementSet {

    /**
     * Adds an event to the acknowledgement set. Assigns initial reference
     * count of 1.
     *
     * @param event event to be added
     * @since 2.2
     */
    public void add(Event event);

    /**
     * Aquires a reference to the event by incrementing the reference
     * count. 
     *
     * @param eventHandle event handle
     * @since 2.2
     */
    public void acquire(final EventHandle eventHandle);

    /**
     * Releases a reference to the event by decrementing the reference
     * count. This is used to indicate the completion of a reference of
     * an event after successful or unsuccessful use of an event. Negative
     * acknowledgement of an event reference can be done by using `false`
     *  as the result
     *
     * @param eventHandle event handle
     * @param result flag indicating if the event has successfully completed or not
     * @return indicates if the release is the last release for the event
     * @since 2.2
     */
    public boolean release(final EventHandle eventHandle, final boolean result);
}