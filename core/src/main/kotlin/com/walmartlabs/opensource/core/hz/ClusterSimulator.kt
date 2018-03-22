package com.walmartlabs.opensource.core.hz

import sun.misc.URLClassPath
import java.lang.Thread.currentThread
import java.net.URLClassLoader


/**
 * Created by smalik3 on 3/14/18
 */
object ClusterSimulator {

    fun nodes(nodes: Int, args: Array<String>, entryClass: String, entryMethod: String = "init") {
        require(nodes > 0) { "invalid node size: $nodes" }
        val urls = (currentThread().contextClassLoader.javaClass.getDeclaredField("ucp").apply { isAccessible = true }.get(currentThread().contextClassLoader) as URLClassPath).urLs

        (1..nodes).forEach {
            Thread({
                currentThread().contextClassLoader = URLClassLoader(urls, null)
                Class.forName(entryClass).getDeclaredMethod(entryMethod, Array<String>::class.java).invoke(null, args)
            }, "main-$it").start()
        }
    }
}