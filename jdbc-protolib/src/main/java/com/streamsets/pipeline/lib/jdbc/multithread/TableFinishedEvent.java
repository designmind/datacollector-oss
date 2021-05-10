/*
 * Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.jdbc.multithread;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.EventDef;
import com.streamsets.pipeline.api.EventFieldDef;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.lib.event.EventCreator;

@EventDef(
    type = TableFinishedEvent.TABLE_FINISHED_TAG,
    description = "Generated when the origin completes processing all data within a table.",
    version = TableFinishedEvent.VERSION
)
public class TableFinishedEvent {
    public static final String TABLE_FINISHED_TAG = "table-finished";
    public static final int VERSION = 1;

    @EventFieldDef(
        name = TableFinishedEvent.SCHEMA_FIELD,
        description = "The schema associated with the table."
    )
    public static final String SCHEMA_FIELD = "schema";

    @EventFieldDef(
        name = TableFinishedEvent.TABLE_FIELD,
        description = "The table that has been processed."
    )
    public static final String TABLE_FIELD = "table";

    @EventFieldDef(
        name = TableFinishedEvent.ROW_COUNT,
        description = "Number of processed rows.",
        optional = true
    )
    public static final String ROW_COUNT = "rowcount";

    @EventFieldDef(
        name = TableFinishedEvent.IS_ANY_OFFSETS_RECORDED,
        description = "isAnyOffsetsRecorded.",
        optional = true
    )
    public static final String IS_ANY_OFFSETS_RECORDED = "isanyoffsetsrecorded";

    @EventFieldDef(
        name = TableFinishedEvent.IS_USING_NONINCREMENTAL_LOAD,
        description = "isUsingNonIncrementalLoad.",
        optional = true
    )
    public static final String IS_USING_NONINCREMENTAL_LOAD = "isusingnonincrementalload";

    @EventFieldDef(
        name = TableFinishedEvent.INITIAL_OFFSETS,
        description = "InitialStoredOffsets (empty after origin reset).",
        optional = true
    )
    public static final String INITIAL_OFFSETS = "initialoffsets";
        
    public static final EventCreator EVENT_CREATOR = new EventCreator.Builder(TABLE_FINISHED_TAG, VERSION)
        .withRequiredField(SCHEMA_FIELD)
        .withRequiredField(TABLE_FIELD)
        .withOptionalField(ROW_COUNT)
        .withOptionalField(IS_ANY_OFFSETS_RECORDED)
        .withOptionalField(IS_USING_NONINCREMENTAL_LOAD)
        .withOptionalField(INITIAL_OFFSETS)
        .build();

    public static void createTableFinishedEvent(
        PushSource.Context context,
        BatchContext batchContext,
        TableRuntimeContext tableRuntimeContext,
        Long recordCount
    ) {
        EVENT_CREATOR.create(context, batchContext)
            .with(SCHEMA_FIELD,  tableRuntimeContext.getSourceTableContext().getSchema())
            .with(TABLE_FIELD, tableRuntimeContext.getSourceTableContext().getTableName())
            .with(ROW_COUNT, recordCount)
            .with(IS_ANY_OFFSETS_RECORDED, Boolean.toString(tableRuntimeContext.isAnyOffsetsRecorded()))
            .with(IS_USING_NONINCREMENTAL_LOAD, Boolean.toString(tableRuntimeContext.isUsingNonIncrementalLoad()))
            .with(INITIAL_OFFSETS, ((null == tableRuntimeContext.getInitialStoredOffsets() || 
                tableRuntimeContext.getInitialStoredOffsets().isEmpty()) ? "" : 
                    tableRuntimeContext.getInitialStoredOffsets().toString()))
            .createAndSend();
    }
}



