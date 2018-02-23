package com.walmartlabs.opensource.bigben.extns

import org.slf4j.Logger

/**
 * Created by smalik3 on 2/22/18
 */
inline fun debug(l: Logger, t: (Logger) -> Unit) {
    if (l.isDebugEnabled)
        t(l)
}

inline fun info(l: Logger, t: (Logger) -> Unit) {
    if (l.isInfoEnabled)
        t(l)
}