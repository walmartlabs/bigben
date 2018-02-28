package com.walmartlabs.opensource.bigben.hz

/**
 * Created by smalik3 on 2/23/18
 */
interface Service {
    val name: String
    fun init()
    fun execute()
    fun destroy()
}