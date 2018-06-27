/*-
 * #%L
 * BigBen:commons
 * =======================================
 * Copyright (C) 2016 - 2018 Walmart Inc.
 * =======================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.walmartlabs.bigben.utils.hz

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
