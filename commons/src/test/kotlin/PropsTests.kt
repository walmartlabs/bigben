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
import com.walmartlabs.bigben.utils.Json
import com.walmartlabs.bigben.utils.commons.Props
import com.walmartlabs.bigben.utils.commons.Props.boolean
import com.walmartlabs.bigben.utils.commons.Props.exists
import com.walmartlabs.bigben.utils.commons.Props.flattenedMap
import com.walmartlabs.bigben.utils.commons.Props.int
import com.walmartlabs.bigben.utils.commons.Props.list
import com.walmartlabs.bigben.utils.commons.Props.long
import com.walmartlabs.bigben.utils.commons.Props.map
import com.walmartlabs.bigben.utils.commons.Props.string
import com.walmartlabs.bigben.utils.json
import com.walmartlabs.bigben.utils.typeRefYaml
import org.testng.annotations.Test
import java.util.*
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
    fun `props test - base64`() {
        val base64 = Base64.getEncoder().encodeToString(PropsTests::class.java.getResource("props.yaml").readText().toByteArray())
        Props.load("base64://$base64")
        asserts()
    }

    @Test(priority = 3)
    fun `props test - yaml`() {
        val yaml = PropsTests::class.java.getResource("props.yaml").readText()
        Props.load("yaml://$yaml")
        asserts()
    }

    @Test(priority = 4)
    fun `props test - json`() {
        val yaml = PropsTests::class.java.getResource("props.yaml").readText()
        val json = typeRefYaml<Json>(yaml).json()
        Props.load("json://$json")
        asserts()
    }

    @Test(priority = 5)
    fun `prop test - supplier`() {
        Props.load(Supplier { "file://props.yaml" })
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
        assertEquals(map("a"), mapOf("b" to "x", "c.d" to "y", "e" to 12, "f" to true,
                "g" to listOf(1, 2), "h" to mapOf("h1" to "H1", "h2" to "H2")))
        assertEquals(flattenedMap(map("a")), mapOf("b" to "x", "c.d" to "y", "e" to 12, "f" to true,
                "g" to listOf(1, 2), "h.h1" to "H1", "h.h2" to "H2"))
    }
}
