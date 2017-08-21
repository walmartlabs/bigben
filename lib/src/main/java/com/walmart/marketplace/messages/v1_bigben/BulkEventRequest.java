
package com.walmart.marketplace.messages.v1_bigben;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Generated;
import javax.validation.Valid;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.walmart.marketplace.messages.core.MessageImpl;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import com.walmart.marketplace.messages.v1_bigben.EventRequest;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
        "events"
})
public class BulkEventRequest
        extends MessageImpl
        implements Serializable
{

    /**
     * List of bigben event Request objects
     * <p>
     *
     *
     */
    @JsonProperty("events")
    @Valid
    private List<EventRequest> events = new ArrayList<EventRequest>();

    @JsonIgnore
    @JsonProperty("id")
    private String id;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();
    protected final static Object NOT_FOUND_VALUE = new Object();
    private final static long serialVersionUID = 8430761030713048724L;

    /**
     * No args constructor for use in serialization
     *
     */
    public BulkEventRequest() {
    }

    /**
     *
     * @param events
     */
    public BulkEventRequest(List<EventRequest> events, String id) {
        this.events = events;
        this.id = id;
    }

    /**
     * List of bigben event Request objects
     * <p>
     *
     *
     * @return
     *     The events
     */
    @JsonProperty("events")
    public List<EventRequest> getEvents() {
        return events;
    }

    /**
     * List of bigben event Request objects
     * <p>
     *
     *
     * @param events
     *     The events
     */
    @JsonProperty("events")
    public void setEvents(List<EventRequest> events) {
        this.events = events;
    }

    public BulkEventRequest withEvents(List<EventRequest> events) {
        this.events = events;
        return this;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public BulkEventRequest withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    protected boolean declaredProperty(String name, Object value) {
        if ("events".equals(name)) {
            if (value instanceof List) {
                setEvents(((List<EventRequest> ) value));
            } else {
                throw new IllegalArgumentException(("property \"events\" is of type \"java.util.List<com.walmart.marketplace.messages.v1_bigben.Event>\", but got "+ value.getClass().toString()));
            }
            return true;
        } else {
            return false;
        }
    }

    protected Object declaredPropertyOrNotFound(String name, Object notFoundValue) {
        if ("events".equals(name)) {
            return getEvents();
        } else {
            return notFoundValue;
        }
    }

    @SuppressWarnings({
            "unchecked"
    })
    public<T >T get(String name) {
        Object value = declaredPropertyOrNotFound(name, BulkEventRequest.NOT_FOUND_VALUE);
        if (BulkEventRequest.NOT_FOUND_VALUE!= value) {
            return ((T) value);
        } else {
            return ((T) getAdditionalProperties().get(name));
        }
    }

    public void set(String name, Object value) {
        if (!declaredProperty(name, value)) {
            getAdditionalProperties().put(name, ((Object) value));
        }
    }

    public BulkEventRequest with(String name, Object value) {
        if (!declaredProperty(name, value)) {
            getAdditionalProperties().put(name, ((Object) value));
        }
        return this;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().appendSuper(super.hashCode()).append(events).append(additionalProperties).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof BulkEventRequest) == false) {
            return false;
        }
        BulkEventRequest rhs = ((BulkEventRequest) other);
        return new EqualsBuilder().appendSuper(super.equals(other)).append(events, rhs.events).append(additionalProperties, rhs.additionalProperties).isEquals();
    }

}
