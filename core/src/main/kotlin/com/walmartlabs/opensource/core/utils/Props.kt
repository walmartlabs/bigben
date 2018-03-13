package com.walmartlabs.opensource.core.utils

/**
 * Created by smalik3 on 2/21/18
 */
object Props {

    fun int(name: String, defaultValue: Int? = null): Int {
        return Integer.getInteger(name, defaultValue)
    }

    fun long(name: String, defaultValue: Long? = null): Long {
        return System.getProperty(name)?.toLong() ?: defaultValue ?: throw IllegalArgumentException("no such 'long' property $name")
    }

    fun string(name: String, defaultValue: String? = null): String {
        return System.getProperty(name, defaultValue)
    }

    fun boolean(name: String, defaultValue: Boolean? = false): Boolean {
        return System.getProperty(name)?.toBoolean() ?: defaultValue ?: throw IllegalArgumentException("no such 'boolean' property $name")
    }

}