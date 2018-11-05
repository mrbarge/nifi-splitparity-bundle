/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author Matt Bargenquast
 */
package matt.processors.splitparity;

import com.backblaze.erasure.ReedSolomon;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"split"})
@CapabilityDescription("Split a file into data fragments and error correction parity files.")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename", description = "The filename of the parent FlowFile")})
public class SplitParity extends AbstractProcessor {

    public static final PropertyDescriptor SHARD_SIZE = new PropertyDescriptor
            .Builder().name("SHARD_SIZE")
            .displayName("Shard Size")
            .description("Number of Data Shards to split the file into.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("10000000")
            .build();

    public static final PropertyDescriptor DATA_SHARDS = new PropertyDescriptor
            .Builder().name("DATA_SHARDS")
            .displayName("Data Shards")
            .description("Number of Data Shards to split the file into.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PARITY_SHARDS = new PropertyDescriptor
            .Builder().name("PARITY_SHARDS")
            .displayName("Parity Shards")
            .description("Number of Parity Shards to generate for the file.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final Relationship REL_SPLITS = new Relationship.Builder()
            .name("splits")
            .description("The individual 'segments' of the original FlowFile will be routed to this relationship.")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successfully splitting an input FlowFile, the original FlowFile will be sent to this relationship.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed, the unchanged FlowFile will be routed to this relationship.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SHARD_SIZE);
        descriptors.add(DATA_SHARDS);
        descriptors.add(PARITY_SHARDS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SPLITS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile originalFlowFile = session.get();
        if ( originalFlowFile == null ) {
            return;
        }

        final ComponentLog logger = getLogger();
        final Integer dataShards = context.getProperty(DATA_SHARDS).asInteger();
        final Integer parityShards = context.getProperty(PARITY_SHARDS).asInteger();

        // Get the size of the input file.
        final int fileSize = (int) originalFlowFile.getSize();
        final int storedSize = (int) originalFlowFile.getSize() + 4;
        final int shardSize = (storedSize + dataShards -1) / dataShards;
        final int bufferSize = shardSize * dataShards;

        // Determine data shards and parity shards based on shard size
        Integer totalShards = dataShards + parityShards;
        byte [] [] shards = new byte [totalShards] [shardSize];

        // get flow file content
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        session.exportTo(originalFlowFile, byteStream);
        byte [] allBytes = new byte[bufferSize];
        ByteBuffer.wrap(allBytes).putInt(fileSize);
        byte[] tmpBytes = byteStream.toByteArray();
        if (allBytes.length > tmpBytes.length) {
            System.arraycopy(tmpBytes,0, allBytes, 4, tmpBytes.length);
        }

        // fill in the data shards
        for (int i = 0; i < dataShards; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }

        // Calculate parity
        ReedSolomon reedSolomon = ReedSolomon.create(dataShards, parityShards);
        reedSolomon.encodeParity(shards, 0, shardSize);

        // Send the resulting parts as flow files
        final List<FlowFile> parts = new ArrayList<>();
        for (Integer i = 0; i < totalShards; i++) {
            FlowFile part = session.create(originalFlowFile);
            final int idx = i;
            part = session.write(part, out -> out.write(shards[idx]));
            part = session.putAttribute(part,"filename", originalFlowFile.getAttribute("filename") + "." + i.toString());
            part = session.putAttribute(part, "fragment.index", i.toString());
            part = session.putAttribute(part, "segment.original.filename", originalFlowFile.getAttribute("filename"));
            part = session.putAttribute(part, "fragment.count", Integer.toString(dataShards + parityShards));
            parts.add(part);
        }

        parts.forEach((part) -> {
            session.transfer(part, REL_SPLITS);
        });

        // now transfer the original flow file
        FlowFile flowFile = originalFlowFile;
        logger.info("Routing {} to {}", new Object[] {flowFile, REL_ORIGINAL});
        session.getProvenanceReporter().route(originalFlowFile, REL_ORIGINAL);
        session.transfer(flowFile, REL_ORIGINAL);
    }
}
