package com.github.pyracantha.coccinea.replication

import com.github.pyracantha.coccinea.journal.ChangeId
import com.github.pyracantha.coccinea.journal.changeId

fun transferLogEntry(
    value: ChangeId = changeId()
) = TransferLogEntry(value)