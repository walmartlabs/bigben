import com.walmartlabs.opensource.bigben.entities.Event
import com.walmartlabs.opensource.bigben.entities.EventRequest
import com.walmartlabs.opensource.bigben.entities.EventResponse
import com.walmartlabs.opensource.bigben.extns.domainProvider
import com.walmartlabs.opensource.bigben.extns.nowUTC

fun EventRequest.toResponse() = EventResponse(tenant = tenant, eventTime = eventTime, id = id, mode = mode, payload = payload)
fun Event.toResponse() = eventResponse?.let { it } ?: EventResponse(id = xrefId, eventId = id, triggeredAt = processedAt?.toString(),
        tenant = tenant, eventTime = eventTime?.toString(), payload = payload, eventStatus = status)

fun EventResponse.event() = domainProvider<Event>().let { it.raw(it.selector(Event::class.java)) }.
        also { it.tenant = tenant; it.xrefId = id; triggeredAt = nowUTC().toString(); it.payload = payload; it.eventResponse = this }