package com.cruxcoach.android

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.Description
import org.junit.runner.JUnitCore
import org.junit.runner.Request
import org.junit.runner.manipulation.Filter
import java.io.File

/**
 * LOCAL PROBE, NEVER COMMITTED (fix/0.2.5-flaky-tests-2). Runs whole test classes again and again in one test JVM and
 * reports every failure with its trace, and the longest run of green rounds per class. Only does something when
 * FLAKY_LOOP_CLASSES is set.
 */
class FlakyLoopProbe {
    @Test
    fun loop() {
        val classes = System.getenv("FLAKY_LOOP_CLASSES")?.split(',')?.map { it.trim() }?.filter { it.isNotEmpty() } ?: return
        val rounds = System.getenv("FLAKY_LOOP_ROUNDS")?.toIntOrNull() ?: 20
        val method = System.getenv("FLAKY_LOOP_METHOD")?.takeIf { it.isNotBlank() }
        val out = System.getenv("FLAKY_LOOP_OUT")?.let(::File)
        val summary = mutableListOf<String>()
        fun say(line: String) { println(line); out?.appendText(line + "\n") }
        var failures = 0
        for (entry in classes) {
            // "a.b.SomeTest#part of a name|other part*5": only those methods, 5 times instead of FLAKY_LOOP_ROUNDS.
            val name = entry.substringBefore('*').substringBefore('#')
            val rounds = entry.substringAfter('*', "").toIntOrNull() ?: rounds
            val method = entry.substringBefore('*').substringAfter('#', "").takeIf { it.isNotBlank() } ?: method
            val type = Class.forName(name)
            var classFailures = 0
            var tests = 0
            var streak = 0
            var bestStreak = 0
            val started = System.currentTimeMillis()
            repeat(rounds) { round ->
                var request = Request.aClass(type)
                if (method != null) request = request.filterWith(object : Filter() {
                    override fun shouldRun(description: Description) =
                        description.isSuite || method.split('|').any { description.methodName?.contains(it) == true }
                    override fun describe() = "method contains $method"
                })
                val t0 = System.currentTimeMillis()
                val result = JUnitCore().run(request)
                tests += result.runCount
                if (result.failureCount == 0 && result.runCount > 0) {
                    streak++
                    bestStreak = maxOf(bestStreak, streak)
                } else {
                    streak = 0
                    classFailures += result.failureCount
                    result.failures.forEach { failure ->
                        say("FAIL ${type.simpleName} round ${round + 1}: ${failure.testHeader}: ${failure.exception}")
                        say(failure.trace.lineSequence().take(40).joinToString("\n") { "    $it" })
                    }
                }
                say("ROUND ${type.simpleName} ${round + 1}/$rounds: ${result.runCount} run, ${result.failureCount} failed, ${result.ignoreCount} ignored, ${System.currentTimeMillis() - t0} ms")
            }
            val line = "LOOP ${type.name}: $rounds rounds, $tests tests, $classFailures failures, best green streak $bestStreak, " +
                "${(System.currentTimeMillis() - started) / 1000} s"
            say(line)
            summary += line
            failures += classFailures
        }
        assertEquals(summary.joinToString("\n"), 0, failures)
    }
}
