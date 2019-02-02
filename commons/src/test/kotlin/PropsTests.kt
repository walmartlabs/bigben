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
import com.walmartlabs.bigben.utils.commons.ResourceLoader
import com.walmartlabs.bigben.utils.typeRefYaml
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

    @Test
    fun `test overrides`() {
        val props = PropsLoader().load("file://overrides.yaml", "file://props.yaml")
        assertTrue(props.exists("a"))
        assertTrue(props.exists("a.b"))
        assertTrue(props.exists("a.c.d"))
        assertEquals(props.string("a.c.d"), "y1") // override
        assertEquals(props.string("a.b"), "x")
        assertEquals(props.int("a.e"), 12)
        assertEquals(props.boolean("a.f"), true)
        assertEquals(props.list("a.g"), listOf(1, 2, 3)) // override => list append
        assertEquals(props.long("a.i", 10), 10)
        val actual = props.map("a")
        val expected = mapOf(
            "b" to "x", "c.d" to "y1", "e" to 12, "f" to true,
            "g" to listOf(1, 2, 3), "h" to mapOf("h1" to "abc", "h2" to "H2", "h3" to System.getProperty("user.home")),
            "j" to 1
        )
        println(expected)
        println(actual)
        assertEquals(expected, actual)
        val actualFlattened = PropsLoader.flatten(props.map("a"))
        val expectedFlattened = mapOf(
            "b" to "x", "c.d" to "y1", "e" to 12, "f" to true,
            "g" to listOf(1, 2, 3), "j" to 1, "h.h1" to "abc", "h.h2" to "H2", "h.h3" to System.getProperty("user.home")
        )
        println(actualFlattened)
        println(expectedFlattened)
        assertEquals(expectedFlattened, actualFlattened)
    }

    /*@Test
    fun `test flatten and unflatten`() {
        //val merged = Props.load("file://sub1-overrides.yaml", "file://sub1.yaml").root()
        val expected = mapOf(
            "a" to "b", "c" to
                    listOf(
                        "4", "5", mapOf("i1" to "I1"), mapOf("d1" to "D1"), mapOf("G" to "H1"),
                        mapOf("g" to "h"), mapOf(
                            "d" to mapOf(
                                "d11" to System.getProperty("java.home1", "acc"),
                                "d22" to "D22", "e" to "E22", "l" to
                                        listOf(
                                            mapOf("a" to System.getProperty("java.io.tmpdir1", "Aaa")),
                                            mapOf("a1" to "b1"), mapOf("c" to "d"), mapOf("e" to mapOf("f" to "F1"))
                                        )
                            )
                        ),
                        mapOf("i" to mapOf("j" to "k11", "l" to "m", "j1" to "J1"))
                    )
        )
        val flattened = PropsLoader.flatten(expected)
        val unflattened = PropsLoader.unflatten(flattened)
        assertEquals(expected, unflattened)
    }

    @Test
    fun `test list substitutions`() {
        val comparator = Comparator<Any> { o1, o2 -> o1.toString().compareTo(o2.toString()) }
        val merged = Props.load("file://sub1-overrides.yaml", "file://sub1.yaml").root()
        val expected = mapOf(
            "a" to "b", "c" to
                    sortedSetOf(
                        comparator,
                        "4", "5", mapOf("i1" to "I1"), mapOf("d1" to "D1"), mapOf("G" to "H1"),
                        mapOf("g" to "h"), mapOf(
                            "d" to mapOf(
                                "d11" to System.getProperty("java.home1", "acc"),
                                "d22" to "D22", "e" to "E22", "l" to
                                        sortedSetOf(
                                            comparator,
                                            mapOf("a" to System.getProperty("java.io.tmpdir1", "Aaa")),
                                            mapOf("a1" to "b1"), mapOf("c" to "d"), mapOf("e" to mapOf("f" to "F1"))
                                        )
                            )
                        ),
                        mapOf("i" to mapOf("j" to "k11", "l" to "m", "j1" to "J1"))
                    )
        )
        val flattened = PropsLoader.flatten(merged) as Json
        val unflattened = PropsLoader.unflatten(flattened)
        println("merged: $merged")
        println("flatte: $flattened")
        println("unflat: $unflattened")
        println("expect: $expected")
        TODO("complete the asserts")
    }*/

    @Test
    fun `test substitutions in list`() {
        val s = ResourceLoader.load("file://a.yaml")
        val yaml = typeRefYaml<Map<String, Any>>(s)


        val merged = Props.load("file://b.yaml", "file://a.yaml").root()
        println(merged)
        //val unflatten = PropsLoader.unflatten(merged.root())
        //println(unflatten.yaml())
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
        assertEquals(
            PropsLoader.flatten(map("a")), mapOf(
                "b" to "x", "c.d" to "y", "e" to 12, "f" to true,
                "g" to listOf(1, 2), "h.h1" to "H1", "h.h2" to "H2"
            )
        )
    }
}
