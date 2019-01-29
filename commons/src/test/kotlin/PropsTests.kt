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
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.commons.Props.boolean
import com.walmartlabs.bigben.utils.commons.Props.exists
import com.walmartlabs.bigben.utils.commons.Props.int
import com.walmartlabs.bigben.utils.commons.Props.list
import com.walmartlabs.bigben.utils.commons.Props.long
import com.walmartlabs.bigben.utils.commons.Props.map
import com.walmartlabs.bigben.utils.commons.Props.string
import com.walmartlabs.bigben.utils.commons.PropsLoader
import org.testng.annotations.Test
import java.util.function.Supplier
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Created by smalik3 on 7/6/18
 */
class PropsTests {

    @Test(priority = 1)
    fun `props test - file`() {
        Props.load("file://props.yaml")
        asserts()
    }

    @Test(priority = 2)
    fun `prop test - supplier`() {
        Props.load(Supplier { "file://props.yaml" })
        asserts()
    }

    @Test(enabled = false)
    fun `test overrides`() {
        val props = PropsLoader().load("file://overrides.yaml", "file://props.yaml")
        /*assertTrue(props.exists("a"))
        assertTrue(props.exists("a.b"))
        assertTrue(props.exists("a.c.d"))*/
        assertEquals(props.string("a.c.d"), "y1") // override
        assertEquals(props.string("a.b"), "x")
        assertEquals(props.int("a.e"), 12)
        assertEquals(props.boolean("a.f"), true)
        assertEquals(props.list("a.g"), listOf(1, 3)) // override
        assertEquals(props.long("a.i", 10), 10)
        assertEquals(
            props.map("a"), mapOf(
                "b" to "x", "c.d" to "y", "e" to 12, "f" to true,
                "g" to listOf(1, 2), "h" to mapOf("h1" to "H11", "h2" to "H22") // override
            )
        )
        assertEquals(
            PropsLoader.flatten(map("a")), mapOf(
                "b" to "x", "c.d" to "y", "e" to 12, "f" to true,
                "g" to listOf(1, 2), "h.h1" to "H11", "h.h2" to "H22" // override
            )
        )
    }

    private fun asserts() {
        assertTrue(exists("a"))
        assertTrue(exists("a.b"))
        assertTrue(exists("a.c.d"))
        assertEquals(string("a.c.d"), "y")
        assertEquals(string("a.b"), "x")
        assertEquals(int("a.e"), 12)
        assertEquals(boolean("a.f"), true)
        assertEquals(list("a.g"), listOf(1, 2))
        assertEquals(long("a.i", 10), 10)
        /*assertEquals(
            map("a"), mapOf(
                "b" to "x", "c.d" to "y", "e" to 12, "f" to true,
                "g" to listOf(1, 2), "h" to mapOf("h1" to "H1", "h2" to "H2")
            )
        )*/
        assertEquals(
            PropsLoader.flatten(map("a")), mapOf(
                "b" to "x", "c.d" to "y", "e" to 12, "f" to true,
                "g" to listOf(1, 2), "h.h1" to "H1", "h.h2" to "H2"
            )
        )
    }

    @Test(enabled = false)
    fun `test hocon`() {
        //val b = PropsLoader().load("file://props.yaml").map("a")
        //val o = PropsLoader().load("file://overrides.yaml").map("a")
        System.setProperty("a.j", "10")
        System.setProperty("a.h.h2", "H2222")
        val props = PropsLoader().load("file://overrides.yaml", "file://props.yaml")
        val m = PropsLoader.merge(mutableMapOf(), mutableMapOf("a" to "b"))
        println(m)
    }
}
