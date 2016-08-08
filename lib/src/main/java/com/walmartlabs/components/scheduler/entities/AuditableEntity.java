package com.walmartlabs.components.scheduler.entities;

import java.time.ZonedDateTime;

/**
 * Created by smalik3 on 8/7/16
 */
public interface AuditableEntity {

    ZonedDateTime getCreatedAt();

    void setCreatedAt(ZonedDateTime createdAt);

    ZonedDateTime getModifiedAt();

    void setModifiedAt(ZonedDateTime modifiedAt);

}
