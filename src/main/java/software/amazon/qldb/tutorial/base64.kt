package software.amazon.qldb.tutorial

import java.util.Base64

fun ByteArray.base64encode(): String = Base64.getEncoder().encodeToString(this)

fun String.base64decode(): ByteArray = Base64.getDecoder().decode(this)
